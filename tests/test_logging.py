"""Tests for aligned console timestamps and independent Parsl verbosity."""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pytest

from pac_man.logging_utils import CONSOLE_DATE_FORMAT, CONSOLE_FORMAT, configure_console_logging


def test_timestamp_alignment() -> None:
    """Dropping milliseconds preserves the column occupied by CASA priorities."""
    record = logging.LogRecord('parsl.jobs.strategy', logging.DEBUG, '', 0, 'example', (), None)
    formatted = logging.Formatter(CONSOLE_FORMAT, CONSOLE_DATE_FORMAT).format(record)
    assert formatted.index('[DEBUG]') == len('2026-10-22 19:58:38     ')
    assert len(formatted.split('     ')[0]) == 19


@pytest.mark.parametrize('parsl_level, shows_debug', [('info', False), ('debug', True)])
def test_independent_console_levels(
    parsl_level: str, shows_debug: bool, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Parsl debug can stay in diagnostic files without appearing in the console."""
    stream = io.StringIO()
    console = logging.StreamHandler(stream)
    diagnostic = logging.FileHandler(tmp_path / 'diagnostic.log')
    diagnostic_formatter = diagnostic.formatter
    root = logging.getLogger()
    monkeypatch.setattr(root, 'handlers', [console, diagnostic])
    monkeypatch.setattr(root, 'level', logging.DEBUG)
    try:
        configure_console_logging(parsl_level)
        configure_console_logging(parsl_level)
        assert len(console.filters) == 1
        assert diagnostic.formatter is diagnostic_formatter
        for name, level, message in (
            ('parsl.jobs.strategy', logging.DEBUG, 'parsl debug'),
            ('parsl.jobs.strategy', logging.INFO, 'parsl info'),
            ('pipeline.task', logging.DEBUG, 'pipeline debug'),
        ):
            record = logging.LogRecord(name, level, '', 0, message, (), None)
            console.handle(record)
            diagnostic.handle(record)
        output = stream.getvalue()
        assert ('parsl debug' in output) is shows_debug
        assert 'parsl info' in output
        assert 'pipeline debug' in output
        assert 'parsl debug' in (tmp_path / 'diagnostic.log').read_text()
    finally:
        diagnostic.close()
        console.close()


def test_invalid_parsl_level() -> None:
    """Unknown profile log levels fail with a clear error."""
    with pytest.raises(ValueError, match='Unknown Parsl log level'):
        configure_console_logging('invalid')
