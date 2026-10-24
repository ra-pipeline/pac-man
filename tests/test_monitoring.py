"""Monitoring tests using local synthetic SQLite databases and mocked schedulers."""

from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pac_man import config, monitoring


def test_live_wal_database_and_final_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Live rows are visible, WAL contents survive backup, and remote readers switch to the archive."""
    monkeypatch.chdir(tmp_path)
    hub = SimpleNamespace(logging_endpoint=None)
    monitoring.configure_local_monitoring(hub, str(tmp_path / 'local'))
    live = Path(hub.pacman_location['live'])
    with closing(sqlite3.connect(live)) as writer:
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('CREATE TABLE progress (stage INTEGER)')
        writer.execute('INSERT INTO progress VALUES (1)')
        writer.commit()
        monitoring.publish_monitoring_location(hub, str(tmp_path / 'runinfo/001'))
        shared = tmp_path / 'runinfo/monitoring.db'
        assert not shared.exists()
        assert monitoring.resolve_monitoring_db(shared) == live
        assert _read_progress(shared) == [1]
        writer.execute('INSERT INTO progress VALUES (2)')
        writer.commit()
        assert live.with_name('monitoring.db-wal').stat().st_size > 0
        monitoring.snapshot_monitoring(hub)
        archive = tmp_path / 'runinfo/001/monitoring.db'
        with patch.object(monitoring.socket, 'gethostname', return_value='other-node.example.org'):
            assert monitoring.resolve_monitoring_db(shared) == archive
            assert _read_progress(shared) == [1, 2]
        assert shared.is_file() and not shared.is_symlink()
        with closing(sqlite3.connect(shared)) as saved:
            assert saved.execute('PRAGMA integrity_check').fetchone() == ('ok',)
            assert saved.execute('SELECT COUNT(*) FROM progress').fetchone() == (2,)
            assert saved.execute('PRAGMA journal_mode').fetchone() == ('delete',)


def test_wrong_node_never_reads_old_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An old snapshot cannot masquerade as live progress on another node."""
    monkeypatch.chdir(tmp_path)
    hub = SimpleNamespace(logging_endpoint=None)
    monitoring.configure_local_monitoring(hub, str(tmp_path / 'local'))
    monitoring.publish_monitoring_location(hub, str(tmp_path / 'runinfo/001'))
    shared = tmp_path / 'runinfo/monitoring.db'
    shared.touch()
    with patch.object(monitoring.socket, 'gethostname', return_value='other-node.example.org'):
        with pytest.raises(FileNotFoundError, match='Open the database on that node'):
            monitoring.resolve_monitoring_db(shared)


def test_new_runs_are_isolated(tmp_path: Path) -> None:
    """Every run gets a separate scratch directory under the selected root."""
    first, second = SimpleNamespace(), SimpleNamespace()
    monitoring.configure_local_monitoring(first, str(tmp_path))
    monitoring.configure_local_monitoring(second, str(tmp_path))
    assert first.logging_endpoint != second.logging_endpoint


def test_default_storage_replaces_stale_local_pointer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Returning to shared storage updates discovery instead of retaining an old run."""
    monkeypatch.chdir(tmp_path)
    hub = SimpleNamespace()
    monitoring.configure_local_monitoring(hub, str(tmp_path / 'local'))
    monitoring.publish_monitoring_location(hub, str(tmp_path / 'runinfo/001'))
    shared = tmp_path / 'runinfo/monitoring.db'
    shared.touch()
    monitoring.publish_monitoring_location(SimpleNamespace(logging_endpoint=f'sqlite:///{shared}'), 'unused')
    assert monitoring.resolve_monitoring_db(shared) == shared


def test_environment_scratch_and_unset_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The environment override expands job-local scratch; missing variables fail early."""
    monkeypatch.setenv('PACMAN_MONITORING_LOCAL_DIR', str(tmp_path))
    hub = SimpleNamespace()
    monitoring.configure_local_monitoring(hub, None)
    assert Path(hub.pacman_location['live']).parent.parent == tmp_path
    monkeypatch.delenv('DEMO_MISSING_SCRATCH', raising=False)
    with pytest.raises(ValueError, match='unset environment'):
        monitoring.configure_local_monitoring(SimpleNamespace(), '$DEMO_MISSING_SCRATCH')


def test_failed_backup_retains_live_pointer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Snapshot failures keep the recoverable local database advertised."""
    monkeypatch.chdir(tmp_path)
    hub = SimpleNamespace()
    monitoring.configure_local_monitoring(hub, str(tmp_path / 'local'))
    monitoring.publish_monitoring_location(hub, str(tmp_path / 'runinfo/001'))
    marker = tmp_path / 'runinfo/monitoring.json'
    with patch.object(monitoring.shutil, 'copyfile', side_effect=OSError('synthetic disk full')):
        with pytest.raises(OSError, match='disk full'):
            monitoring.snapshot_monitoring(hub)
    assert json.loads(marker.read_text())['state'] == 'active'
    assert monitoring.resolve_monitoring_db(tmp_path / 'runinfo/monitoring.db').is_file()


def test_initialize_configures_both_writers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The configured endpoint is present before Parsl starts and location publication follows loading."""
    monkeypatch.chdir(tmp_path)
    hub = SimpleNamespace(logging_endpoint=None)
    cfg = SimpleNamespace(monitoring=hub, run_dir="runinfo")
    with (
        patch.object(config, 'get_local_threads_config', return_value=cfg),
        patch('pac_man.events.install_event_tracking'),
        patch.object(config.parsl, 'load', return_value=SimpleNamespace(run_dir=str(tmp_path / 'runinfo/001'))) as load,
    ):
        config.initialize_parsl(backend='threads', parsl_monitoring=True, monitoring_local_dir=str(tmp_path / 'local'))
    assert load.call_args.args[0].monitoring.logging_endpoint.startswith('sqlite:///')
    assert (tmp_path / 'runinfo/monitoring.json').exists()


def _read_progress(path):
    with closing(sqlite3.connect(monitoring.resolve_monitoring_db(path))) as connection:
        return [row[0] for row in connection.execute('SELECT stage FROM progress')]


def test_optional_diagnostics_use_timeout_without_preparing_schema(tmp_path):
    from sqlalchemy import create_engine

    hub = SimpleNamespace(logging_endpoint=None)
    monitoring.configure_sqlite_monitoring(hub, str(tmp_path / 'runinfo'))
    path = monitoring.sqlite_monitoring_path(hub.logging_endpoint)
    assert not path.exists()
    engine = create_engine(hub.logging_endpoint)
    try:
        with engine.connect() as connection:
            assert connection.exec_driver_sql('PRAGMA busy_timeout').scalar() == 30000
            assert connection.exec_driver_sql('PRAGMA journal_mode').scalar() == 'delete'
            assert connection.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == []
    finally:
        engine.dispose()


def test_optional_parsl_monitoring_writes_database_and_snapshot(tmp_path, monkeypatch):
    from parsl.app.app import python_app
    from parsl.dataflow.dflow import DataFlowKernel
    from pac_man.events import install_event_tracking

    monkeypatch.chdir(tmp_path)
    cfg = config.get_local_threads_config(parsl_monitoring=True)
    cfg.run_dir = str(tmp_path / 'runinfo')
    monitoring.configure_local_monitoring(cfg.monitoring, str(tmp_path / 'scratch'))
    monitoring.configure_sqlite_monitoring(cfg.monitoring, cfg.run_dir)
    dfk = DataFlowKernel(cfg)
    install_event_tracking(dfk, 'optional-diagnostics')
    dfk.pacman_sqlite_available = True
    monitoring.publish_monitoring_location(cfg.monitoring, dfk.run_dir)
    try:
        assert python_app(abs, data_flow_kernel=dfk)(-2).result(timeout=20) == 2
    finally:
        dfk.cleanup()
    monitoring.snapshot_monitoring(cfg.monitoring)
    path = monitoring.resolve_monitoring_db(tmp_path / 'runinfo/monitoring.db')
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('SELECT run_id FROM workflow').fetchone() == (dfk.run_id,)
        assert connection.execute('SELECT COUNT(*) FROM task').fetchone() == (1,)
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='stage_execution'").fetchall() == []
