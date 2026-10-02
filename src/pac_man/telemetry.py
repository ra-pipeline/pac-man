"""Telemetry formatting and visualization utilities for PAC-MAN."""

from __future__ import annotations

TASK_COLOR_MAP: dict[str, str] = {
    "main_process": "#0d6efd",     # Main process stage compute (Blue)
    "tier0_executor": "#20c997",   # Tier0 distributed subtask compute (Emerald)
    "weblog_executor": "#6ea8fe",  # Background weblog rendering (Light Blue)
}


def format_task_label(name: str, executor: str) -> str:
    """Format task name with execution role tag based on executor and task identity.

    Args:
        name: Name of the executed task or function.
        executor: Executor label that processed the task.

    Returns:
        Role-tagged display label.
    """
    if any(tag in name for tag in ["[weblog]", "[compute]", "[tier0 subtask]", "[tier0]"]):
        return name
    if executor == "tier0_executor" or name.startswith("Tier0"):
        return f"{name} [tier0 subtask]"
    if executor == "weblog_executor":
        return f"{name} [weblog]"
    if executor == "main_process":
        return f"{name} [compute]"
    return name
