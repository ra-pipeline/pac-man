"""Independent JSONL progress events and dashboard projections.

Each process owns a uniquely named file. Threads in that process serialize
appends; readers ignore incomplete lines and deduplicate event IDs.
"""
from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
import json
import logging
import os
from pathlib import Path
import socket
import threading
import uuid

import pandas as pd

LOG = logging.getLogger(__name__)
_LOCK = threading.Lock()
_WRITER = uuid.uuid4().hex


def _time(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f'Unsupported event value: {type(value).__name__}')


def emit_event(directory, run_id, kind, **fields):
    """Append one event without allowing telemetry errors to fail computation."""
    event = dict(version=1, event_id=uuid.uuid4().hex, run_id=run_id, kind=kind,
                 timestamp=datetime.now(timezone.utc).isoformat(), **fields)
    try:
        directory = Path(directory) / run_id
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f'{socket.gethostname()}-{os.getpid()}-{_WRITER}.jsonl'
        data = json.dumps(event, default=_time) + '\n'
        with _LOCK, path.open('a', encoding='utf-8') as stream:
            stream.write(data)
    except (OSError, TypeError, ValueError):
        LOG.warning('Could not write JSONL progress event', exc_info=True)


def _worker_call(function, directory, run_id, task_id, try_id, *args, **kwargs):
    emit_event(directory, run_id, 'worker', task_id=task_id, try_id=try_id,
               hostname=socket.gethostname(), state='running')
    return function(*args, **kwargs)


def install_event_tracking(dfk, workflow_name):
    """Observe DFK state transitions and wrap worker calls without querying SQLite.

    The instance adapters use Parsl's _send_task_info and launch_task hooks.
    Worker wrapping happens at executor submission, after memoization lookup.
    Integration tests cover state changes, dependencies, retries and exceptions.
    """
    if getattr(dfk, 'pacman_events_dir', None) is not None:
        return
    directory = str((Path(dfk.run_dir).resolve().parent / 'events'))
    dfk.pacman_events_dir = directory
    emit_event(directory, dfk.run_id, 'workflow', workflow_name=workflow_name, state='running')
    send_info, launch, cleanup = dfk._send_task_info, dfk.launch_task, dfk.cleanup

    def task_info(record):
        emit_event(directory, dfk.run_id, 'task', task_id=record['id'],
                   try_id=record['try_id'], task_func_name=record['func_name'],
                   task_executor=record['executor'], state=record['status'].name,
                   task_time_invoked=record.get('time_invoked'),
                   task_time_returned=record.get('time_returned'),
                   task_try_time_launched=record.get('try_time_launched'),
                   task_try_time_returned=record.get('try_time_returned'))
        try:
            if getattr(dfk, 'pacman_sqlite_available', False):
                hub = dfk.config.monitoring
                if not hub.dbm_proc.is_alive():
                    dfk.pacman_sqlite_available = False
                    hub.resource_monitoring_enabled = False
                    emit_event(directory, dfk.run_id, 'diagnostic', message='SQLite monitoring writer exited')
                    LOG.warning('SQLite monitoring writer exited; JSONL progress remains active')
                    return
            if dfk.monitoring is not None and not getattr(dfk, 'pacman_sqlite_available', True):
                return
            send_info(record)
        except Exception:
            LOG.warning('Parsl diagnostic monitoring send failed; JSONL progress remains active', exc_info=True)

    active_task = threading.local()

    def wrap_submit(submit):
        def tracked_submit(function, resource_specification, *args, **kwargs):
            task = getattr(active_task, 'identity', None)
            if task is not None:
                function = partial(_worker_call, function, directory, dfk.run_id, *task)
            return submit(function, resource_specification, *args, **kwargs)
        return tracked_submit

    for executor in dfk.executors.values():
        executor.submit = wrap_submit(executor.submit)

    def launch_task(record):
        previous = getattr(active_task, 'identity', None)
        active_task.identity = (record['id'], record['try_id'])
        try:
            return launch(record)
        finally:
            active_task.identity = previous

    def finish():
        try:
            return cleanup()
        finally:
            if not getattr(dfk, 'pacman_events_finished', False):
                emit_event(directory, dfk.run_id, 'workflow', state='finished')
                dfk.pacman_events_finished = True

    dfk._send_task_info = task_info
    dfk.launch_task = launch_task
    dfk.cleanup = finish


def record_stage(dfk, **fields):
    emit_event(dfk.pacman_events_dir, dfk.run_id, 'stage', **fields)


def read_events(directory):
    """Read complete valid records; a writer may currently be appending a line."""
    events = {}
    for path in Path(directory).glob('*/*.jsonl'):
        try:
            with path.open(encoding='utf-8') as stream:
                for line in stream:
                    if not line.endswith('\n'):
                        continue
                    try:
                        event = json.loads(line)
                        if (isinstance(event, dict) and event.get('version') == 1
                                and all(isinstance(event.get(key), str) for key in ('event_id', 'run_id', 'timestamp', 'kind'))):
                            events[event['event_id']] = event
                    except (ValueError, TypeError):
                        continue
        except OSError:
            LOG.warning('Could not read progress events: %s', path)
    return sorted(events.values(), key=lambda event: (event['timestamp'], event['event_id']))


def workflow_frame(events):
    runs = {}
    for event in events:
        if event['kind'] != 'workflow':
            continue
        row = runs.setdefault(event['run_id'], dict(run_id=event['run_id'], workflow_name='',
                                                    time_began=None, time_completed=None))
        if event.get('workflow_name'):
            row['workflow_name'] = event['workflow_name']
            row['time_began'] = event['timestamp']
        if event.get('state') == 'finished':
            row['time_completed'] = event['timestamp']
    frame = pd.DataFrame(runs.values(), columns=['run_id', 'workflow_name', 'time_began', 'time_completed'])
    return frame.sort_values('time_began', ascending=False) if not frame.empty else frame


TIMELINE_COLUMNS = ['task_id', 'task_func_name', 'task_try_time_launched', 'task_try_time_returned',
                    'task_status', 'task_executor', 'hostname', 'try_id']
DETAIL_COLUMNS = ['task_id', 'task_func_name', 'task_time_invoked', 'task_time_returned',
                  'hostname', 'task_executor', 'try_id', 'task_status']


def progress_frames(events, run_id):
    """Project run events into the current dashboard's status, timeline and details."""
    attempts, stages, latest = {}, {}, {}
    for event in events:
        if event['run_id'] != run_id:
            continue
        kind = event['kind']
        if kind == 'stage':
            stages[event['stage_number']] = event
        elif kind in ('task', 'worker'):
            key = (event['task_id'], event.get('try_id', 0))
            row = attempts.setdefault(key, dict(task_id=key[0], try_id=key[1], hostname=None))
            if kind == 'task':
                for field in ('task_func_name', 'task_executor', 'task_time_invoked', 'task_time_returned',
                              'task_try_time_launched', 'task_try_time_returned'):
                    if event.get(field) is not None:
                        row[field] = event[field]
                row['task_status'] = event['state']
                latest[key[0]] = row
            else:
                row['hostname'] = event['hostname']
                # Worker timestamps refine execution start, but never overwrite terminal state.
                row['worker_started'] = event['timestamp']
    timeline, details = [], []
    for row in attempts.values():
        if 'task_func_name' not in row:
            continue
        if row.get('worker_started'):
            row['task_try_time_launched'] = row['worker_started']
            if row.get('task_status') == 'launched':
                row['task_status'] = 'running'
        details.append(row)
        if row.get('task_try_time_launched'):
            timeline.append(row)
    for stage in stages.values():
        row = dict(task_id=stage['stage_number'], task_func_name=stage['stage_label'],
                   task_try_time_launched=stage['start_time'], task_try_time_returned=stage.get('end_time'),
                   task_time_invoked=stage['start_time'], task_time_returned=stage.get('end_time'),
                   task_status=stage['status'], task_executor='main_process', hostname=stage['hostname'], try_id=0)
        timeline.append(row)
        details.append(row)
    counts = {}
    for row in latest.values():
        state = row['task_status']
        counts[state] = counts.get(state, 0) + 1
    status = pd.DataFrame(list(counts.items()), columns=['task_status_name', 'count'])
    return status, pd.DataFrame(timeline, columns=TIMELINE_COLUMNS), pd.DataFrame(details, columns=DETAIL_COLUMNS)
