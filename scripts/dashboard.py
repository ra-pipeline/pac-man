from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

from pac_man.events import read_events, workflow_frame, progress_frames
from pac_man.telemetry import TASK_COLOR_MAP, format_task_label

st.set_page_config(layout='wide', page_title='PAC-MAN Dashboard')

roots = [Path(os.environ.get('INIT_CWD', '.')), Path.cwd()]
if os.environ.get('PIXI_PROJECT_ROOT'):
    roots.append(Path(os.environ['PIXI_PROJECT_ROOT']))
candidates = list(dict.fromkeys(
    (root / relative / 'runinfo/events').resolve()
    for root in roots for relative in (Path('.'), Path('working'), Path('proc/working'))
))
discovered = [path for path in candidates if path.is_dir()]

if len(sys.argv) > 1:
    event_dir = Path(sys.argv[1]).expanduser().resolve()
elif os.environ.get('PACMAN_EVENTS_DIR'):
    event_dir = Path(os.environ['PACMAN_EVENTS_DIR']).expanduser().resolve()
elif os.environ.get('PACMAN_MONITORING_DB'):
    # Preserve old launch scripts without opening their database.
    legacy_path = Path(os.environ['PACMAN_MONITORING_DB']).expanduser().resolve()
    event_dir = legacy_path.parent / 'events'
    st.sidebar.caption('PACMAN_MONITORING_DB is deprecated; use PACMAN_EVENTS_DIR.')
elif len(discovered) > 1:
    event_dir = st.sidebar.selectbox('Progress Directory', discovered)
else:
    event_dir = discovered[0] if discovered else candidates[0]

st.title('PAC-MAN Execution Dashboard')
st.caption(f'Progress directory: `{event_dir}`')
auto_refresh = st.sidebar.toggle('Auto-refresh', value=False, help='Refresh progress every 30 seconds.')
st.button('Refresh now')


def normalize_to_local(series: pd.Series) -> pd.Series:
    local_tz = datetime.now().astimezone().tzinfo
    return (pd.to_datetime(series, format='mixed', utc=True)
            .dt.tz_convert(local_tz).dt.tz_localize(None).astype('datetime64[ns]'))


def tier0_strips(timeline, details, expanded):
    """Give every attempt its own strip while keeping function groups compact."""
    timeline = timeline.copy()
    timeline['lane'] = timeline['task_func_name']
    timeline['strip_offset'] = -0.4
    timeline['strip_width'] = 0.8
    labels = {}
    tier0 = timeline[timeline['task_executor'] == 'tier0_executor']
    for name, group in tier0.groupby('task_func_name', sort=False):
        group = group.sort_values(['task_id', 'try_id'], kind='stable')
        tasks = details[(details['task_executor'] == 'tier0_executor')
                        & (details['task_func_name'] == name)]
        latest = tasks.sort_values('try_id').drop_duplicates('task_id', keep='last')
        states = latest['task_status'].replace({
            'exec_done': 'completed', 'memo_done': 'cached', 'launched': 'starting',
        }).value_counts()
        summary = ' · '.join(f'{count} {state}' for state, count in states.items())
        labels[name] = f'{name} — {len(latest)} tasks · {summary}'
        for position, index in enumerate(group.index):
            if name in expanded:
                row = group.loc[index]
                lane = f"{name} / task {row['task_id']} / attempt {row['try_id']}"
                timeline.at[index, 'lane'] = lane
                labels[lane] = f"  Task {row['task_id']} · attempt {row['try_id']}"
            else:
                timeline.at[index, 'strip_offset'] = -0.4 + position * 0.8 / len(group)
                timeline.at[index, 'strip_width'] = 0.8 / len(group)
        if name in expanded:
            # Put the group summary on the first attempt instead of adding an empty bar.
            first_lane = timeline.at[group.index[0], 'lane']
            labels[first_lane] = f'{labels[name]} / {labels[first_lane].strip()}'
    return timeline, labels


@st.fragment(run_every=30 if auto_refresh else None)
def render_progress() -> None:
    """Retry progress discovery on every refresh, even before the first event."""
    events = read_events(event_dir)
    workflows = workflow_frame(events)
    if workflows.empty:
        st.info('No recorded workflow runs found in the progress files. Waiting for a refresh.')
        st.stop()

    run_ids = workflows['run_id'].tolist()
    if (st.session_state.get('workflow_directory') != str(event_dir)
            or st.session_state.get('workflow_run') not in run_ids):
        st.session_state['workflow_run'] = run_ids[0]
    st.session_state['workflow_directory'] = str(event_dir)
    selected_run = st.selectbox('Workflow Run', run_ids, key='workflow_run')

    status_df, timeline, details = progress_frames(events, selected_run)
    diagnostics = [event for event in events if event['run_id'] == selected_run and event['kind'] == 'diagnostic']
    if diagnostics:
        st.warning(f"Optional SQLite diagnostics unavailable: {diagnostics[-1]['message']}. Progress remains available.")

    cols = st.columns(len(status_df) + 1)
    cols[0].metric('Total Tasks', status_df['count'].sum())
    for i, row in status_df.iterrows():
        cols[i + 1].metric(row['task_status_name'], row['count'])
    if not status_df.empty:
        st.bar_chart(status_df.set_index('task_status_name'))

    for frame in (timeline, details):
        if not frame.empty:
            frame['task_func_name'] = frame.apply(
                lambda row: format_task_label(row['task_func_name'], str(row['task_executor'])), axis=1
            )

    if not timeline.empty:
        timeline['Start'] = normalize_to_local(timeline['task_try_time_launched'])
        timeline['Finish'] = normalize_to_local(timeline['task_try_time_returned']).fillna(pd.Timestamp.now())
        timeline['duration_s'] = (timeline['Finish'] - timeline['Start']).dt.total_seconds().round(1)
        groups = timeline.loc[timeline['task_executor'] == 'tier0_executor', 'task_func_name'].drop_duplicates().tolist()
        expanded = st.multiselect(
            'Expand Tier0 task groups', groups,
            help='Each compact group contains one strip per task attempt. Expand a group for full-height task rows.',
        ) if groups else []
        timeline, lane_labels = tier0_strips(timeline, details, expanded)
        executor_order = list(TASK_COLOR_MAP)
        row_order = (
            timeline.assign(_executor_order=timeline['task_executor'].map(
                {executor: index for index, executor in enumerate(executor_order)}
            ).fillna(len(executor_order)))
            .assign(_group_start=lambda frame: frame.groupby(['task_executor', 'task_func_name'])['Start'].transform('min'))
            .sort_values(['_executor_order', '_group_start', 'task_func_name', 'task_id', 'try_id'], kind='stable')
            ['lane'].drop_duplicates().tolist()
        )
        fig = px.timeline(
            timeline, x_start='Start', x_end='Finish', y='lane', color='task_executor',
            color_discrete_map=TASK_COLOR_MAP,
            category_orders={'task_executor': executor_order},
            hover_name='task_func_name',
            hover_data=['task_id', 'task_status', 'duration_s', 'hostname', 'try_id'],
        )
        for trace in fig.data:
            rows = timeline[timeline['task_executor'] == trace.name]
            trace.offset = rows['strip_offset'].tolist()
            trace.width = rows['strip_width'].tolist()
            trace.marker.line.width = 0.5
            trace.marker.line.color = 'rgba(255,255,255,0.6)'
        fig.update_layout(barmode='overlay', height=max(500, len(row_order) * 30))
        fig.update_yaxes(title=None, autorange='reversed', dtick=1, categoryorder='array', categoryarray=row_order,
                         tickmode='array', tickvals=row_order,
                         ticktext=[lane_labels.get(lane, lane) for lane in row_order])
        st.plotly_chart(fig, width='stretch')
    else:
        st.info('No launched tasks found for this run.')

    st.subheader('Task Details')
    if not details.empty:
        details['task_time_invoked'] = normalize_to_local(details['task_time_invoked'])
        details = details.sort_values('task_time_invoked')
    st.dataframe(details, width='stretch')


render_progress()
