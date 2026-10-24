"""JSONL dashboard discovery, recovery and workflow selection."""
from datetime import datetime, timezone
from pathlib import Path
import socket
import sqlite3
from unittest.mock import patch

import pytest

streamlit = pytest.importorskip('streamlit')
from streamlit.testing.v1 import AppTest
from pac_man import events

DASHBOARD = Path(__file__).resolve().parents[1] / 'scripts/dashboard.py'


@pytest.mark.parametrize('initial', ['missing', 'empty', 'partial'])
def test_initial_telemetry_failure_keeps_refresh_registered(initial, tmp_path, monkeypatch):
    directory = tmp_path / 'events'
    if initial != 'missing':
        (directory / 'run').mkdir(parents=True)
    if initial == 'partial':
        (directory / 'run/partial.jsonl').write_text('{"incomplete":')
    monkeypatch.setattr('sys.argv', ['dashboard.py'])
    monkeypatch.setenv('PACMAN_EVENTS_DIR', str(directory))
    with patch.object(streamlit, 'fragment', wraps=streamlit.fragment) as fragment:
        app = AppTest.from_file(str(DASHBOARD), default_timeout=20).run()
        assert not app.exception
        assert app.info
        assert app.toggle[0].value is False
        fragment.assert_called_with(run_every=None)
        app.toggle[0].set_value(True).run()
        fragment.assert_called_with(run_every=30)
        events.emit_event(directory, 'run-1', 'workflow', workflow_name='first', state='running')
        events.emit_event(directory, 'run-2', 'workflow', workflow_name='second', state='running')
        app.run()
        assert not app.exception
        assert app.selectbox[0].value == 'run-2'
        app.selectbox[0].select('run-1').run()
        events.emit_event(directory, 'run-3', 'workflow', workflow_name='third', state='running')
        app.run()
        assert not app.exception
        assert app.selectbox[0].value == 'run-1'
        app.toggle[0].set_value(False).run()
        fragment.assert_called_with(run_every=None)


@pytest.mark.parametrize('source', ['environment', 'legacy', 'argument', 'discovery'])
def test_jsonl_dashboard_ignores_locked_diagnostic_database(tmp_path, monkeypatch, source):
    db = tmp_path / 'runinfo/monitoring.db'
    directory = db.parent / 'events'
    events.emit_event(directory, 'run', 'workflow', workflow_name='test', state='running')
    events.emit_event(directory, 'run', 'stage', stage_number=1, stage_label='Stage 1',
                      start_time=datetime.now(timezone.utc), status='running', hostname=socket.gethostname())
    events.emit_event(directory, 'run', 'task', task_id=0, try_id=0, task_func_name='render',
                      task_executor='weblog_executor', state='launched',
                      task_time_invoked=datetime.now(timezone.utc),
                      task_try_time_launched=datetime.now(timezone.utc))
    events.emit_event(directory, 'run', 'diagnostic', message='database writer exited')
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('sys.argv', ['dashboard.py'])
    monkeypatch.delenv('PACMAN_EVENTS_DIR', raising=False)
    monkeypatch.delenv('PACMAN_MONITORING_DB', raising=False)
    if source == 'environment':
        monkeypatch.setenv('PACMAN_EVENTS_DIR', str(directory))
        monkeypatch.setenv('PACMAN_MONITORING_DB', '/unused/monitoring.db')
    elif source == 'legacy':
        monkeypatch.setenv('PACMAN_MONITORING_DB', str(db))
    elif source == 'argument':
        monkeypatch.setattr('sys.argv', ['dashboard.py', str(directory)])
        monkeypatch.setenv('PACMAN_EVENTS_DIR', '/unused/events')
    # Even an exclusive diagnostic lock must not affect rendering.
    connection = sqlite3.connect(db)
    try:
        connection.execute('BEGIN EXCLUSIVE')
        app = AppTest.from_file(str(DASHBOARD), default_timeout=20).run()
        assert not app.exception
        assert app.selectbox[0].value == 'run'
        assert app.metric[0].value == '1'
        assert app.warning
        assert len(app.dataframe[-1].value) == 2
    finally:
        connection.close()


def test_timeline_keeps_compute_above_tier0_and_weblog_across_refreshes(tmp_path, monkeypatch):
    import plotly.express as px

    directory = tmp_path / 'events'
    monkeypatch.setattr('sys.argv', ['dashboard.py'])
    monkeypatch.setenv('PACMAN_EVENTS_DIR', str(directory))
    events.emit_event(directory, 'run', 'workflow', workflow_name='test', state='running')
    # Emit worker rows first and main stages out of order to exercise explicit ordering.
    for task_id, executor, name in [(0, 'weblog_executor', 'render'), (1, 'tier0_executor', 'clean')]:
        events.emit_event(directory, 'run', 'task', task_id=task_id, try_id=0,
                          task_func_name=name, task_executor=executor, state='launched',
                          task_time_invoked='2026-01-01T00:00:00+00:00',
                          task_try_time_launched='2026-01-01T00:00:00+00:00')
    for stage in (2, 1):
        events.emit_event(directory, 'run', 'stage', stage_number=stage,
                          stage_label=f'Stage_{stage:03d} [compute]', status='running',
                          start_time=f'2026-01-01T00:00:0{stage}+00:00', hostname='main')
    figures = []
    original = px.timeline

    def capture(*args, **kwargs):
        figure = original(*args, **kwargs)
        figures.append(figure)
        return figure

    expected = ['Stage_001 [compute]', 'Stage_002 [compute]', 'clean [tier0 subtask]', 'render [weblog]']
    with patch.object(px, 'timeline', side_effect=capture):
        app = AppTest.from_file(str(DASHBOARD), default_timeout=20).run()
        assert not app.exception
        assert list(figures[-1].layout.yaxis.categoryarray) == expected
        assert figures[-1].layout.yaxis.autorange == 'reversed'
        assert [trace.name for trace in figures[-1].data] == ['main_process', 'tier0_executor', 'weblog_executor']
        events.emit_event(directory, 'run', 'stage', stage_number=3, stage_label='Stage_003 [compute]',
                          status='running', start_time='2026-01-01T00:00:03+00:00', hostname='main')
        app.run()
        assert not app.exception
        assert list(figures[-1].layout.yaxis.categoryarray) == expected[:2] + ['Stage_003 [compute]'] + expected[2:]
