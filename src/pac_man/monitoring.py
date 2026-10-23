"""Local monitoring storage, shared snapshots, and bounded dashboard reads."""

from __future__ import annotations

from contextlib import closing
import json
import logging
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import tempfile
import time
from typing import Any

import pandas as pd

LOG = logging.getLogger(__name__)


class MonitoringBusyError(RuntimeError):
    """The monitoring database remained busy after bounded retries."""


def _write_location(path: Path, location: dict[str, str]) -> None:
    """Atomically publish location metadata to dashboard readers."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        json.dump(location, stream)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def configure_local_monitoring(hub: Any, local_dir: str | None) -> None:
    """Configure an isolated live SQLite database under explicitly local scratch.

    Args:
        hub: Parsl monitoring hub to configure before loading the DFK.
        local_dir: Scratch root; falls back to PACMAN_MONITORING_LOCAL_DIR.
    """
    root = local_dir if local_dir is not None else os.environ.get('PACMAN_MONITORING_LOCAL_DIR')
    if not root:
        return
    expanded = os.path.expandvars(root)
    if '$' in expanded:
        raise ValueError('monitoring_local_dir contains an unset environment variable')
    root_path = Path(expanded).expanduser().resolve()
    root_path.mkdir(parents=True, exist_ok=True)
    live = Path(tempfile.mkdtemp(prefix='pacman-monitoring-', dir=root_path)) / 'monitoring.db'
    with closing(sqlite3.connect(live, timeout=30)) as connection:
        connection.execute('PRAGMA journal_mode=WAL')
    hub.logging_endpoint = f'sqlite:///{live}'
    hub.pacman_location = {
        'live': str(live),
        'hostname': socket.gethostname(),
        'state': 'active',
        'shared': str(Path('runinfo/monitoring.db').resolve()),
    }


def publish_monitoring_location(hub: Any, run_dir: str) -> None:
    """Advertise a loaded run's live database from its shared working directory."""
    location = getattr(hub, 'pacman_location', None)
    if not isinstance(location, dict):
        # Replace stale local-run metadata when returning to ordinary storage.
        endpoint = getattr(hub, 'logging_endpoint', None)
        if isinstance(endpoint, str) and endpoint.startswith('sqlite:///'):
            _write_location(Path('runinfo/monitoring.json'), {
                'state': 'shared', 'live': str(Path(endpoint.removeprefix('sqlite:///')).resolve()),
            })
        return
    location['snapshot'] = str(Path(run_dir).resolve() / 'monitoring.db')
    _write_location(Path(location['shared']).with_suffix('.json'), location)
    LOG.info('Live monitoring database: %s (host %s)', location['live'], location['hostname'])


def snapshot_monitoring(hub: Any) -> None:
    """Save a consistent snapshot after Parsl cleanup, then switch dashboard readers.

    The local database is retained for recovery if snapshot publication fails.
    """
    location = getattr(hub, 'pacman_location', None)
    if not isinstance(location, dict) or 'snapshot' not in location:
        return
    live = Path(location['live'])
    archive = Path(location['snapshot'])
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=archive.parent, delete=False) as stream:
        temporary = Path(stream.name)
    deadline = time.monotonic() + 30

    def check_timeout(status: int, remaining: int, total: int) -> None:
        if time.monotonic() > deadline:
            raise MonitoringBusyError('Timed out saving monitoring snapshot; local database retained')

    try:
        with closing(sqlite3.connect(live.as_uri() + '?mode=ro', uri=True, timeout=5)) as source:
            with closing(sqlite3.connect(temporary)) as target:
                source.backup(target, pages=256, progress=check_timeout, sleep=0.1)
                target.execute('PRAGMA journal_mode=DELETE')
        temporary.replace(archive)
    finally:
        temporary.unlink(missing_ok=True)
    # Copy the CLOSED snapshot, never the live WAL database, to the usual path.
    shared = Path(location['shared'])
    with tempfile.NamedTemporaryFile(dir=shared.parent, delete=False) as stream:
        temporary = Path(stream.name)
    try:
        shutil.copyfile(archive, temporary)
        temporary.replace(shared)
    finally:
        temporary.unlink(missing_ok=True)
    location['state'] = 'finished'
    _write_location(shared.with_suffix('.json'), location)
    LOG.info('Saved monitoring snapshot: %s', archive)


def resolve_monitoring_db(db_path: str | Path) -> Path:
    """Resolve a shared dashboard path to a live local DB or completed snapshot."""
    path = Path(db_path).expanduser().absolute()
    marker = path.with_suffix('.json')
    if marker.is_file():
        location = json.loads(marker.read_text())
        if location['state'] == 'shared':
            path = Path(location['live'])
        elif location['state'] == 'finished':
            path = Path(location['snapshot'])
        elif location['hostname'] != socket.gethostname():
            raise FileNotFoundError(
                f"Live monitoring is on node {location['hostname']}. Run the dashboard on that node; "
                'a shared snapshot will be available after the pipeline exits.'
            )
        else:
            path = Path(location['live'])
    if not path.is_file():
        raise FileNotFoundError(f'Monitoring database is not available yet: {path}')
    return path


def read_monitoring_query(db_path: str | Path, query: str, params: tuple | list | None = None) -> pd.DataFrame:
    """Read telemetry with fresh connections and bounded retries for lock contention."""
    for attempt in range(3):
        path = resolve_monitoring_db(db_path)
        try:
            with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)) as connection:
                cursor = connection.execute(query, params or ())
                return pd.DataFrame.from_records(cursor.fetchall(), columns=[column[0] for column in cursor.description])
        except sqlite3.OperationalError as error:
            if 'locked' not in str(error).lower() and 'busy' not in str(error).lower():
                raise
            if attempt == 2:
                raise MonitoringBusyError('Monitoring database is busy; retrying on the next dashboard refresh') from error
            time.sleep(0.2 * (attempt + 1))
    raise AssertionError('Unreachable')
