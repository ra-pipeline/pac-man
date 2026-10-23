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
        assert monitoring.read_monitoring_query(shared, 'SELECT stage FROM progress').stage.tolist() == [1]
        writer.execute('INSERT INTO progress VALUES (2)')
        writer.commit()
        assert live.with_name('monitoring.db-wal').stat().st_size > 0
        monitoring.snapshot_monitoring(hub)
        archive = tmp_path / 'runinfo/001/monitoring.db'
        with patch.object(monitoring.socket, 'gethostname', return_value='other-node.example.org'):
            assert monitoring.resolve_monitoring_db(shared) == archive
            assert monitoring.read_monitoring_query(shared, 'SELECT stage FROM progress').stage.tolist() == [1, 2]
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
        with pytest.raises(FileNotFoundError, match='Run the dashboard on that node'):
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


def test_lock_retry_is_bounded_and_connections_close(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Real exclusive-lock contention is reported without leaking failed reader connections."""
    db = tmp_path / 'monitoring.db'
    real_connect = sqlite3.connect
    readers = []

    def fast_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs['timeout'] = 0.001
        connection = real_connect(*args, **kwargs)
        readers.append(connection)
        return connection

    with closing(real_connect(db)) as writer:
        writer.execute('CREATE TABLE progress (stage INTEGER)')
        writer.commit()
        writer.execute('BEGIN EXCLUSIVE')
        monkeypatch.setattr(monitoring.sqlite3, 'connect', fast_connect)
        monkeypatch.setattr(monitoring.time, 'sleep', lambda _: None)
        with pytest.raises(monitoring.MonitoringBusyError):
            monitoring.read_monitoring_query(db, 'SELECT * FROM progress')
        assert len(readers) == 3
        for reader in readers:
            with pytest.raises(sqlite3.ProgrammingError, match='closed'):
                reader.execute('SELECT 1')
        writer.rollback()
        assert monitoring.read_monitoring_query(db, 'SELECT * FROM progress').empty
    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        monitoring.read_monitoring_query(db, 'SELECT * FROM missing_table')


def test_initialize_configures_both_writers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The configured endpoint is present before Parsl starts and location publication follows loading."""
    monkeypatch.chdir(tmp_path)
    hub = SimpleNamespace(logging_endpoint=None)
    cfg = SimpleNamespace(monitoring=hub)
    with (
        patch.object(config, 'get_local_threads_config', return_value=cfg),
        patch.object(config.parsl, 'load', return_value=SimpleNamespace(run_dir=str(tmp_path / 'runinfo/001'))) as load,
    ):
        config.initialize_parsl(backend='threads', monitoring_local_dir=str(tmp_path / 'local'))
    assert load.call_args.args[0].monitoring.logging_endpoint.startswith('sqlite:///')
    assert (tmp_path / 'runinfo/monitoring.json').exists()


def test_dashboard_reads_live_then_archived_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Streamlit renders without errors while live storage transitions to a shared snapshot."""
    from streamlit.testing.v1 import AppTest

    dashboard = Path(__file__).resolve().parents[1] / 'scripts/dashboard.py'
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('sys.argv', [str(dashboard)])
    hub = SimpleNamespace()
    monitoring.configure_local_monitoring(hub, str(tmp_path / 'local'))
    monitoring.publish_monitoring_location(hub, str(tmp_path / 'runinfo/001'))
    monkeypatch.setenv('PACMAN_MONITORING_DB', str(tmp_path / 'runinfo/monitoring.db'))
    with closing(sqlite3.connect(hub.pacman_location['live'])) as writer:
        writer.executescript('''
            CREATE TABLE workflow (run_id TEXT, workflow_name TEXT, time_began TEXT, time_completed TEXT);
            INSERT INTO workflow VALUES ('demo', 'demo', '2026-01-01', NULL);
            CREATE TABLE task (run_id TEXT, task_id INTEGER, task_func_name TEXT,
                               task_time_invoked TEXT, task_time_returned TEXT);
            CREATE TABLE status (run_id TEXT, task_id INTEGER, task_status_name TEXT, timestamp TEXT);
            CREATE TABLE try (run_id TEXT, task_id INTEGER, task_try_time_launched TEXT,
                              task_try_time_returned TEXT, task_executor TEXT, hostname TEXT);
        ''')
    app = AppTest.from_file(str(dashboard)).run(timeout=20)
    assert not app.exception
    assert app.metric[0].value == '0'
    monitoring.snapshot_monitoring(hub)
    with patch.object(monitoring.socket, 'gethostname', return_value='other-node.example.org'):
        app.run(timeout=20)
    assert not app.exception
    assert app.metric[0].value == '0'
