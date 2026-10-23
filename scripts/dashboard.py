from __future__ import annotations

import os
import sqlite3
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

from pac_man.monitoring import MonitoringBusyError, read_monitoring_query, resolve_monitoring_db

st.set_page_config(layout='wide', page_title='PAC-MAN Dashboard')

# Discover candidate databases across common execution locations
discovered_dbs = []
candidates = []
init_cwd = os.environ.get('INIT_CWD')
if init_cwd:
    candidates.extend([
        os.path.join(init_cwd, 'runinfo', 'monitoring.db'),
        os.path.join(init_cwd, 'working', 'runinfo', 'monitoring.db'),
        os.path.join(init_cwd, 'proc', 'working', 'runinfo', 'monitoring.db'),
    ])
candidates.extend([
    'runinfo/monitoring.db',
    os.path.join('working', 'runinfo', 'monitoring.db'),
    os.path.join('proc', 'working', 'runinfo', 'monitoring.db'),
])
pixi_root = os.environ.get('PIXI_PROJECT_ROOT')
if pixi_root:
    candidates.extend([
        os.path.join(pixi_root, 'runinfo', 'monitoring.db'),
        os.path.join(pixi_root, 'working', 'runinfo', 'monitoring.db'),
        os.path.join(pixi_root, 'proc', 'working', 'runinfo', 'monitoring.db'),
    ])

seen_paths = set()
for c in candidates:
    abs_c = os.path.abspath(c)
    if abs_c not in seen_paths and (os.path.exists(abs_c) or os.path.exists(os.path.splitext(abs_c)[0] + '.json')):
        seen_paths.add(abs_c)
        discovered_dbs.append(c)

if len(sys.argv) > 1:
    DB_PATH = sys.argv[1]
elif 'PACMAN_MONITORING_DB' in os.environ:
    DB_PATH = os.environ['PACMAN_MONITORING_DB']
elif discovered_dbs:
    if len(discovered_dbs) > 1:
        DB_PATH = st.sidebar.selectbox('Monitoring Database', discovered_dbs, index=0)
    else:
        DB_PATH = discovered_dbs[0]
elif init_cwd and os.path.exists(os.path.join(init_cwd, 'runinfo', 'monitoring.db')):
    DB_PATH = os.path.join(init_cwd, 'runinfo', 'monitoring.db')
else:
    DB_PATH = os.path.join('working', 'runinfo', 'monitoring.db')


def get_data(query: str, params: tuple | list | None = None) -> pd.DataFrame:
    try:
        return read_monitoring_query(DB_PATH, query, params)
    except MonitoringBusyError as error:
        st.warning(str(error))
        st.stop()
    except FileNotFoundError as error:
        st.info(str(error))
        st.stop()


st.title('PAC-MAN Execution Dashboard')
st.caption(f'Database: `{os.path.abspath(DB_PATH)}`')

st.button('Refresh now')

try:
    resolved_db = resolve_monitoring_db(DB_PATH)
    st.caption(f'Reading: `{resolved_db}` (live data requires the orchestrator node)')
    workflows = get_data('SELECT run_id, workflow_name, time_began, time_completed FROM workflow ORDER BY time_began DESC')
except (FileNotFoundError, sqlite3.OperationalError) as error:
    st.info(f'Telemetry is not ready: {error}. Use Refresh now after the workflow starts.')
    st.stop()

if workflows.empty:
    st.info('No recorded workflow runs found in the monitoring database.')
    st.stop()

selected_run = st.sidebar.selectbox('Workflow Run', workflows['run_id'])

@st.fragment(run_every=10)
def render_progress() -> None:
    """Refresh progress without resetting the selected workflow."""
    # 2. Task Status Summary (status table has task_status_name)
    status_df = get_data(
        """
        SELECT s.task_status_name, COUNT(DISTINCT s.task_id) as count
        FROM status s
        JOIN task t ON s.task_id = t.task_id AND s.run_id = t.run_id
        WHERE t.run_id = ?
          AND s.timestamp = (
              SELECT MAX(s2.timestamp) FROM status s2
              WHERE s2.task_id = s.task_id AND s2.run_id = s.run_id
          )
        GROUP BY s.task_status_name
    """,
        params=(selected_run,),
    )

    cols = st.columns(len(status_df) + 1)
    cols[0].metric('Total Tasks', status_df['count'].sum())
    for i, row in status_df.iterrows():
        cols[i + 1].metric(row['task_status_name'], row['count'])

    if not status_df.empty:
        st.bar_chart(status_df.set_index('task_status_name'))

    # 3. Gantt Timeline (both synchronous compute and asynchronous weblog rendering)
    compute_df = pd.DataFrame()
    try:
        compute_df = get_data(
            """
            SELECT stage_number as task_id,
                   stage_label as task_func_name,
                   start_time as task_try_time_launched,
                   end_time as task_try_time_returned,
                   status as task_status,
                   'main_process' as task_executor,
                   hostname
            FROM stage_execution
            WHERE run_id = ? AND start_time IS NOT NULL
        """,
            params=(selected_run,),
        )
    except Exception:
        pass

    parsl_tasks_df = pd.DataFrame()
    try:
        parsl_tasks_df = get_data(
            """
            SELECT t.task_id, t.task_func_name,
                   tr.task_try_time_launched, tr.task_try_time_returned,
                   (SELECT s.task_status_name FROM status s
                    WHERE s.task_id = t.task_id AND s.run_id = t.run_id
                    ORDER BY s.timestamp DESC LIMIT 1) as task_status,
                   tr.task_executor,
                   tr.hostname
            FROM task t
            JOIN try tr ON t.task_id = tr.task_id AND t.run_id = tr.run_id
            WHERE t.run_id = ? AND tr.task_try_time_launched IS NOT NULL
        """,
            params=(selected_run,),
        )
    except Exception:
        pass

    from pac_man.telemetry import TASK_COLOR_MAP, format_task_label

    if not parsl_tasks_df.empty:
        parsl_tasks_df['task_func_name'] = parsl_tasks_df.apply(
            lambda r: format_task_label(r['task_func_name'], str(r.get('task_executor', ''))), axis=1
        )

    from datetime import datetime

    local_tz = datetime.now().astimezone().tzinfo


    def normalize_to_local(series: pd.Series) -> pd.Series:
        def _convert(val):
            if pd.isna(val):
                return pd.NaT
            dt = pd.to_datetime(val, format='mixed')
            if dt.tz is not None:
                return dt.tz_convert(local_tz).tz_localize(None)
            return dt

        return series.apply(_convert)


    tasks_df = pd.concat([compute_df, parsl_tasks_df], ignore_index=True)

    if not tasks_df.empty:
        tasks_df['Start'] = normalize_to_local(tasks_df['task_try_time_launched'])
        tasks_df['Finish'] = normalize_to_local(tasks_df['task_try_time_returned']).fillna(pd.Timestamp.now())
        tasks_df['duration_s'] = (tasks_df['Finish'] - tasks_df['Start']).dt.total_seconds().round(1)
        fig = px.timeline(
            tasks_df,
            x_start='Start',
            x_end='Finish',
            y='task_func_name',
            color='task_executor',
            color_discrete_map=TASK_COLOR_MAP,
            hover_data=['task_id', 'task_status', 'duration_s', 'hostname'],
        )
        n_categories = tasks_df['task_func_name'].nunique()
        fig.update_layout(height=max(500, n_categories * 26))
        fig.update_yaxes(autorange='reversed', dtick=1)
        st.plotly_chart(fig, width='stretch')
    else:
        st.info('No launched tasks found for this run.')

    # 4. Raw task table
    st.subheader('Task Details')
    raw_parsl_df = pd.DataFrame()
    try:
        raw_parsl_df = get_data(
            """
            SELECT t.task_id, t.task_func_name, t.task_time_invoked, t.task_time_returned,
                   tr.hostname, tr.task_executor
            FROM task t
            LEFT JOIN try tr ON t.task_id = tr.task_id AND t.run_id = tr.run_id
            WHERE t.run_id = ?
            ORDER BY t.task_id
        """,
            params=(selected_run,),
        )
    except Exception:
        pass

    if not raw_parsl_df.empty:
        raw_parsl_df['task_func_name'] = raw_parsl_df.apply(
            lambda r: format_task_label(r['task_func_name'], str(r.get('task_executor', ''))), axis=1
        )

    raw_compute_df = pd.DataFrame()
    try:
        raw_compute_df = get_data(
            """
            SELECT stage_number as task_id,
                   stage_label as task_func_name,
                   start_time as task_time_invoked,
                   end_time as task_time_returned,
                   hostname,
                   'main_process' as task_executor
            FROM stage_execution
            WHERE run_id = ?
            ORDER BY stage_number
        """,
            params=(selected_run,),
        )
    except Exception:
        pass

    detail_df = pd.concat([raw_compute_df, raw_parsl_df], ignore_index=True)
    if not detail_df.empty:
        detail_df['task_time_invoked'] = normalize_to_local(detail_df['task_time_invoked'])
        detail_df = detail_df.sort_values(by=['task_time_invoked'])
    st.dataframe(detail_df, width='stretch')


render_progress()
