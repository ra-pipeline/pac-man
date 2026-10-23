"""Independent progress telemetry with real Parsl transitions and no SQLite reads."""
from concurrent.futures import ThreadPoolExecutor as FuturesPool
from datetime import datetime, timezone
import json
import socket
from unittest.mock import patch
from types import SimpleNamespace

import pytest
from parsl.app.app import python_app
from parsl.config import Config
from parsl.dataflow.dflow import DataFlowKernel
from parsl.executors import ThreadPoolExecutor

from pac_man import config, events


def test_events_are_complete_per_writer_and_ignore_partial_lines(tmp_path):
    with FuturesPool(max_workers=4) as pool:
        list(pool.map(lambda i: events.emit_event(tmp_path, 'run', 'sample', number=i), range(100)))
    files = list((tmp_path / 'run').glob('*.jsonl'))
    assert len(files) == 1
    with files[0].open('a') as stream:
        stream.write('invalid record\n{"incomplete":')
    read = events.read_events(tmp_path)
    assert len(read) == 100
    assert {row['number'] for row in read} == set(range(100))
    duplicate = tmp_path / 'run/duplicate.jsonl'
    duplicate.write_text(json.dumps(read[0]) + '\n')
    assert len(events.read_events(tmp_path)) == 100


def test_real_parsl_events_include_retries_dependencies_and_worker_hosts(tmp_path):
    dfk = DataFlowKernel(Config(executors=[ThreadPoolExecutor(label='events', max_threads=2)],
                               run_dir=str(tmp_path / 'runinfo'), monitoring=None, retries=1))
    events.install_event_tracking(dfk, 'test-workflow')
    attempts = []

    def retry_once():
        attempts.append(1)
        if len(attempts) == 1:
            raise ValueError('transient failure')
        return 12

    def fail():
        raise RuntimeError('worker failure')

    def dependent(value):
        return value + 1

    try:
        cached = python_app(abs, data_flow_kernel=dfk, cache=True)
        assert cached(-4).result(timeout=20) == 4
        assert cached(-4).result(timeout=20) == 4
        retried = python_app(retry_once, data_flow_kernel=dfk)()
        assert python_app(dependent, data_flow_kernel=dfk)(retried).result(timeout=20) == 13
        failed = python_app(fail, data_flow_kernel=dfk)()
        blocked = python_app(dependent, data_flow_kernel=dfk)(failed)
        with pytest.raises(Exception):
            blocked.result(timeout=20)
        events.record_stage(dfk, stage_number=1, stage_label='Stage 1', task_name='task',
                            start_time=datetime.now(timezone.utc), end_time=None,
                            status='running', hostname=socket.gethostname())
    finally:
        dfk.cleanup()
    records = events.read_events(dfk.pacman_events_dir)
    assert events.workflow_frame(records).iloc[0]['time_completed'] is not None
    assert {'fail_retryable', 'failed', 'dep_fail', 'exec_done', 'memo_done'} <= {
        row['state'] for row in records if row['kind'] == 'task'
    }
    status, timeline, details = events.progress_frames(records, dfk.run_id)
    assert status['count'].sum() == 6
    assert len(details[details.task_id == retried.tid]) >= 2
    worker_rows = timeline[(timeline.task_executor == 'events') & (timeline.task_status != 'memo_done')]
    assert worker_rows.hostname.notna().all()
    assert 'main_process' in timeline.task_executor.tolist()


def test_sqlite_failure_does_not_stop_jsonl_startup(tmp_path):
    hub = SimpleNamespace(logging_endpoint='sqlite:///runinfo/monitoring.db')
    cfg = SimpleNamespace(monitoring=hub, run_dir=str(tmp_path / 'runinfo'))
    dfk = SimpleNamespace(config=cfg, run_id='run', run_dir=str(tmp_path / 'runinfo/000'))

    def install(kernel, name):
        kernel.pacman_events_dir = str(tmp_path / 'runinfo/events')
        events.emit_event(kernel.pacman_events_dir, kernel.run_id, 'workflow', workflow_name=name, state='running')

    with (
        patch.object(config, 'get_local_threads_config', return_value=cfg),
        patch.object(config, 'configure_local_monitoring'),
        patch.object(config, 'configure_sqlite_monitoring', side_effect=OSError('disk unavailable')),
        patch.object(config.parsl, 'load', return_value=dfk),
        patch.object(events, 'install_event_tracking', side_effect=install),
    ):
        config.initialize_parsl(backend='threads', parsl_monitoring=True)
    assert dfk.pacman_sqlite_available is False
    assert cfg.monitoring is None
    records = events.read_events(dfk.pacman_events_dir)
    assert not events.workflow_frame(records).empty
    assert any(row['kind'] == 'diagnostic' for row in records)


def test_jsonl_worker_events_survive_remote_function_serialization():
    """The same wrapper used on Slurm executes in a separate local HTEX worker."""
    import os
    import parsl

    dfk = parsl.dfk()
    future = python_app(os.getpid, data_flow_kernel=dfk, executors=['tier0_executor'])()
    assert future.result(timeout=45) != os.getpid()
    records = events.read_events(dfk.pacman_events_dir)
    assert any(row['kind'] == 'worker' and row['task_id'] == future.tid
               and row['run_id'] == dfk.run_id for row in records)
