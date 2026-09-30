import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import sys
import os

st.set_page_config(layout="wide", page_title="PAC-MAN Dashboard")

# Accept DB path from CLI arg or default to working/runinfo/monitoring.db
DB_PATH = sys.argv[1] if len(sys.argv) > 1 else os.path.join("working", "runinfo", "monitoring.db")


def get_data(query: str, params: tuple | list | None = None) -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


st.title("PAC-MAN Execution Dashboard")
st.caption(f"Database: `{os.path.abspath(DB_PATH)}`")

# Pre-flight check: ensure monitoring DB exists before querying
if not os.path.exists(DB_PATH):
    st.info(f"Monitoring database not found at `{DB_PATH}`. Please run a pipeline workflow first to generate telemetry.")
    st.stop()

# 1. Workflow Selection
workflows = get_data("SELECT run_id, workflow_name, time_began, time_completed FROM workflow ORDER BY time_began DESC")
if workflows.empty:
    st.info("No recorded workflow runs found in the monitoring database.")
    st.stop()

selected_run = st.sidebar.selectbox("Workflow Run", workflows['run_id'])

# 2. Task Status Summary (status table has task_status_name)
status_df = get_data("""
    SELECT s.task_status_name, COUNT(DISTINCT s.task_id) as count
    FROM status s
    JOIN task t ON s.task_id = t.task_id AND s.run_id = t.run_id
    WHERE t.run_id = ?
      AND s.timestamp = (
          SELECT MAX(s2.timestamp) FROM status s2
          WHERE s2.task_id = s.task_id AND s2.run_id = s.run_id
      )
    GROUP BY s.task_status_name
""", params=(selected_run,))

cols = st.columns(len(status_df) + 1)
cols[0].metric("Total Tasks", status_df['count'].sum())
for i, row in status_df.iterrows():
    cols[i + 1].metric(row['task_status_name'], row['count'])

st.bar_chart(status_df.set_index("task_status_name"))

# 3. Gantt Timeline (both synchronous compute and asynchronous weblog rendering)
compute_df = pd.DataFrame()
try:
    compute_df = get_data("""
        SELECT stage_number as task_id,
               stage_label as task_func_name,
               start_time as task_try_time_launched,
               end_time as task_try_time_returned,
               status as task_status,
               'main_process' as task_executor,
               hostname
        FROM stage_execution
        WHERE run_id = ? AND start_time IS NOT NULL
    """, params=(selected_run,))
except Exception:
    pass

parsl_tasks_df = pd.DataFrame()
try:
    parsl_tasks_df = get_data("""
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
    """, params=(selected_run,))
except Exception:
    pass

if not parsl_tasks_df.empty:
    parsl_tasks_df['task_func_name'] = parsl_tasks_df['task_func_name'].apply(
        lambda n: n if ('[weblog]' in n or '[compute]' in n) else f"{n} [weblog]"
    )

tasks_df = pd.concat([compute_df, parsl_tasks_df], ignore_index=True)

if not tasks_df.empty:
    tasks_df['Start'] = pd.to_datetime(tasks_df['task_try_time_launched'])
    tasks_df['Finish'] = pd.to_datetime(tasks_df['task_try_time_returned']).fillna(pd.Timestamp.now())
    tasks_df['duration_s'] = (tasks_df['Finish'] - tasks_df['Start']).dt.total_seconds().round(1)
    fig = px.timeline(tasks_df, x_start="Start", x_end="Finish", y="task_func_name",
                      color="task_executor", hover_data=["task_id", "task_status", "duration_s", "hostname"])
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No launched tasks found for this run.")

# 4. Raw task table
st.subheader("Task Details")
raw_parsl_df = pd.DataFrame()
try:
    raw_parsl_df = get_data("""
        SELECT t.task_id, t.task_func_name, t.task_time_invoked, t.task_time_returned,
               tr.hostname, tr.task_executor
        FROM task t
        LEFT JOIN try tr ON t.task_id = tr.task_id AND t.run_id = tr.run_id
        WHERE t.run_id = ?
        ORDER BY t.task_id
    """, params=(selected_run,))
except Exception:
    pass

if not raw_parsl_df.empty:
    raw_parsl_df['task_func_name'] = raw_parsl_df['task_func_name'].apply(
        lambda n: n if ('[weblog]' in n or '[compute]' in n) else f"{n} [weblog]"
    )

raw_compute_df = pd.DataFrame()
try:
    raw_compute_df = get_data("""
        SELECT stage_number as task_id,
               stage_label as task_func_name,
               start_time as task_time_invoked,
               end_time as task_time_returned,
               hostname,
               'main_process' as task_executor
        FROM stage_execution
        WHERE run_id = ?
        ORDER BY stage_number
    """, params=(selected_run,))
except Exception:
    pass

detail_df = pd.concat([raw_compute_df, raw_parsl_df], ignore_index=True)
if not detail_df.empty:
    detail_df = detail_df.sort_values(by=['task_time_invoked'])
st.dataframe(detail_df, use_container_width=True)
