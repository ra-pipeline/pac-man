"""Unit tests for PAC-MAN resource-targeted concurrency barriers."""

from __future__ import annotations

from unittest.mock import MagicMock

from pac_man import runner


def test_is_ms_mutative_task():
    """Verify task classification for in-place MeasurementSet mutations."""
    # Mutative tasks
    assert runner.is_ms_mutative_task("hifv_hanning") is True
    assert runner.is_ms_mutative_task("hif_applycal") is True
    assert runner.is_ms_mutative_task("hifa_applycal") is True
    assert runner.is_ms_mutative_task("hifv_applycals") is True
    assert runner.is_ms_mutative_task("statwt") is True
    assert runner.is_ms_mutative_task("hifv_statwt") is True
    assert runner.is_ms_mutative_task("hifa_flagdata") is True
    assert runner.is_ms_mutative_task("hifv_checkflag") is True
    assert runner.is_ms_mutative_task("hifa_targetflag") is True

    # Non-mutative / additive tasks
    assert runner.is_ms_mutative_task("hif_mstransform") is False
    assert runner.is_ms_mutative_task("hifa_bandpass") is False
    assert runner.is_ms_mutative_task("hifa_antpos") is False
    assert runner.is_ms_mutative_task("hif_makeimages") is False
    assert runner.is_ms_mutative_task("hif_findcont") is False


def test_background_ms_reader_classification():
    """Verify classification of background tasks that open vis.ms."""
    assert "hifa_importdata" in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hifv_importdata" in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hifa_applycal" in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hifv_flagdata" in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hifv_checkflag" in runner.TASKS_READING_MS_IN_BACKGROUND

    # Tasks whose QA/weblog do not open vis.ms
    assert "hifa_bandpass" not in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hifa_antpos" not in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hifa_tsyscal" not in runner.TASKS_READING_MS_IN_BACKGROUND
    assert "hif_makeimages" not in runner.TASKS_READING_MS_IN_BACKGROUND


def test_inflight_stage_task_reads_ms():
    """Verify InFlightStageTask correctly infers reads_ms flag."""
    future_mock = MagicMock()
    future_mock.done.return_value = False

    task_import = runner.InFlightStageTask(future_mock, "hifa_importdata", 1)
    assert task_import.reads_ms is True
    assert task_import.is_done is False

    task_antpos = runner.InFlightStageTask(future_mock, "hifa_antpos", 2)
    assert task_antpos.reads_ms is False
    assert task_antpos.is_done is False
