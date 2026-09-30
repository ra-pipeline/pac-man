#!/usr/bin/env python
"""Export PAC-MAN execution dashboard timeline as a standalone interactive HTML asset.

Reads telemetry from a monitoring database (or uses generic synthetic pipeline data)
and generates a sanitized interactive Plotly timeline for documentation.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import pandas as pd
import plotly.express as px

SYNTHETIC_TASKS = [
    {
        "task_id": 1,
        "task_func_name": "Stage_001_hifv_importdata [compute]",
        "Start": "2026-09-30 14:08:30",
        "Finish": "2026-09-30 14:08:40",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 10.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 2,
        "task_func_name": "Stage_002_hifv_hanning [compute]",
        "Start": "2026-09-30 14:08:40",
        "Finish": "2026-09-30 14:08:42",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 2.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 3,
        "task_func_name": "Stage_003_hifv_flagdata [compute]",
        "Start": "2026-09-30 14:08:42",
        "Finish": "2026-09-30 14:09:17",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 35.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 4,
        "task_func_name": "Stage_004_hifv_vlasetjy [compute]",
        "Start": "2026-09-30 14:09:17",
        "Finish": "2026-09-30 14:09:32",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 15.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 5,
        "task_func_name": "Stage_005_hifv_priorcals [compute]",
        "Start": "2026-09-30 14:09:32",
        "Finish": "2026-09-30 14:10:47",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 75.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 6,
        "task_func_name": "Stage_006_hifv_syspower [compute]",
        "Start": "2026-09-30 14:10:47",
        "Finish": "2026-09-30 14:10:52",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 5.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 7,
        "task_func_name": "Stage_007_hifv_testBPdcals [compute]",
        "Start": "2026-09-30 14:10:52",
        "Finish": "2026-09-30 14:10:56",
        "task_executor": "main_process",
        "task_status": "exec_done",
        "duration_s": 4.0,
        "hostname": "worker-01",
    },
    {
        "task_id": 8,
        "task_func_name": "Stage_001_hifv_importdata [weblog]",
        "Start": "2026-09-30 14:08:40",
        "Finish": "2026-09-30 14:09:40",
        "task_executor": "weblog_executor",
        "task_status": "exec_done",
        "duration_s": 60.0,
        "hostname": "worker-02",
    },
    {
        "task_id": 9,
        "task_func_name": "Stage_002_hifv_hanning [weblog]",
        "Start": "2026-09-30 14:09:40",
        "Finish": "2026-09-30 14:09:46",
        "task_executor": "weblog_executor",
        "task_status": "exec_done",
        "duration_s": 6.0,
        "hostname": "worker-02",
    },
    {
        "task_id": 10,
        "task_func_name": "Stage_003_hifv_flagdata [weblog]",
        "Start": "2026-09-30 14:09:46",
        "Finish": "2026-09-30 14:09:54",
        "task_executor": "weblog_executor",
        "task_status": "exec_done",
        "duration_s": 8.0,
        "hostname": "worker-02",
    },
    {
        "task_id": 11,
        "task_func_name": "Stage_004_hifv_vlasetjy [weblog]",
        "Start": "2026-09-30 14:09:54",
        "Finish": "2026-09-30 14:10:08",
        "task_executor": "weblog_executor",
        "task_status": "exec_done",
        "duration_s": 14.0,
        "hostname": "worker-02",
    },
    {
        "task_id": 12,
        "task_func_name": "Stage_005_hifv_priorcals [weblog]",
        "Start": "2026-09-30 14:10:48",
        "Finish": "2026-09-30 14:10:56",
        "task_executor": "weblog_executor",
        "task_status": "running",
        "duration_s": 8.0,
        "hostname": "worker-02",
    },
]


def load_telemetry_from_db(db_path: str) -> pd.DataFrame:
    """Load and sanitize tasks from monitoring database."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        compute_df = pd.read_sql_query(
            """
            SELECT stage_number as task_id,
                   stage_label as task_func_name,
                   start_time as task_try_time_launched,
                   end_time as task_try_time_returned,
                   status as task_status,
                   'main_process' as task_executor,
                   'worker-01' as hostname
            FROM stage_execution
            WHERE start_time IS NOT NULL
            """,
            conn,
        )
        parsl_df = pd.read_sql_query(
            """
            SELECT t.task_id, t.task_func_name,
                   tr.task_try_time_launched, tr.task_try_time_returned,
                   COALESCE((SELECT s.task_status_name FROM status s
                             WHERE s.task_id = t.task_id AND s.run_id = t.run_id
                             ORDER BY s.timestamp DESC LIMIT 1), 'running') as task_status,
                   tr.task_executor,
                   'worker-02' as hostname
            FROM task t
            JOIN try tr ON t.task_id = tr.task_id AND t.run_id = tr.run_id
            WHERE tr.task_try_time_launched IS NOT NULL
            """,
            conn,
        )
    finally:
        conn.close()

    if not parsl_df.empty:
        parsl_df["task_func_name"] = parsl_df["task_func_name"].apply(
            lambda n: n if ("[weblog]" in n or "[compute]" in n) else f"{n} [weblog]"
        )

    tasks_df = pd.concat([compute_df, parsl_df], ignore_index=True)
    if tasks_df.empty:
        return pd.DataFrame()

    tasks_df["Start"] = pd.to_datetime(tasks_df["task_try_time_launched"])
    tasks_df["Finish"] = pd.to_datetime(tasks_df["task_try_time_returned"]).fillna(pd.Timestamp.now())
    tasks_df["duration_s"] = (tasks_df["Finish"] - tasks_df["Start"]).dt.total_seconds().round(1)
    return tasks_df


def build_timeline_figure(tasks_df: pd.DataFrame) -> px.timeline:
    """Create a formatted Plotly timeline figure."""
    fig = px.timeline(
        tasks_df,
        x_start="Start",
        x_end="Finish",
        y="task_func_name",
        color="task_executor",
        hover_data=["task_id", "task_status", "duration_s", "hostname"],
        color_discrete_map={
            "main_process": "#0d6efd",
            "weblog_executor": "#6ea8fe",
        },
    )
    fig.update_yaxes(autorange="reversed", title_text="")
    fig.update_xaxes(title_text="Timeline")
    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
    )
    return fig


def main():
    parser = argparse.ArgumentParser(description="Export dashboard timeline to HTML")
    parser.add_argument(
        "--db",
        default="working/runinfo/monitoring.db",
        help="Path to monitoring SQLite database.",
    )
    parser.add_argument(
        "--output",
        default="docs/assets/dashboard_timeline.html",
        help="Target output HTML file path.",
    )
    args = parser.parse_args()

    tasks_df = pd.DataFrame()
    if os.path.exists(args.db):
        try:
            tasks_df = load_telemetry_from_db(args.db)
        except Exception:
            pass

    if tasks_df.empty:
        tasks_df = pd.DataFrame(SYNTHETIC_TASKS)
        tasks_df["Start"] = pd.to_datetime(tasks_df["Start"])
        tasks_df["Finish"] = pd.to_datetime(tasks_df["Finish"])

    fig = build_timeline_figure(tasks_df)
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    fig.write_html(args.output, include_plotlyjs="cdn")
    print(f"Exported dashboard timeline to {args.output} ({os.path.getsize(args.output)} bytes)")


if __name__ == "__main__":
    main()
