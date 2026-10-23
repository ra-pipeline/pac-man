"""Configure console formatting and independent Parsl verbosity."""

from __future__ import annotations

import logging

CONSOLE_FORMAT = '%(asctime)s     [%(levelname)s] %(name)s: %(message)s'
CONSOLE_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class ParslConsoleFilter(logging.Filter):
    """Limit Parsl console records without filtering pipeline or diagnostic files."""

    def __init__(self, level: int) -> None:
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        is_parsl = record.name == 'parsl' or record.name.startswith('parsl.')
        return not is_parsl or record.levelno >= self.level


def configure_console_logging(parsl_loglevel: str = 'info') -> None:
    """Align console timestamps with CASA and set Parsl's console threshold.

    Parsl can reset its own logger to DEBUG when creating diagnostic files.
    Filtering console handlers keeps that detail out of the terminal while
    retaining it in Parsl's files and leaving pipeline log levels independent.
    """
    level = logging.getLevelNamesMapping().get(parsl_loglevel.upper())
    if level is None:
        raise ValueError(f'Unknown Parsl log level: {parsl_loglevel}')

    logging.basicConfig(level=logging.INFO, format=CONSOLE_FORMAT, datefmt=CONSOLE_DATE_FORMAT)
    formatter = logging.Formatter(CONSOLE_FORMAT, datefmt=CONSOLE_DATE_FORMAT)
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handler.setFormatter(formatter)
            for existing in list(handler.filters):
                if isinstance(existing, ParslConsoleFilter):
                    handler.removeFilter(existing)
            handler.addFilter(ParslConsoleFilter(level))
