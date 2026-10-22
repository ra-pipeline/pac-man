"""
PAC-MAN: Pipeline Asynchronous Computing Manager
External orchestration library using Parsl for HTC/OSG integration.
"""

from pac_man.runner import (
    executeppr,
    executevlappr,
    pac_man_executeppr,
    pac_man_executevlappr,
    pac_man_reduce,
    reduce,
)

__version__ = '0.1.0'


def main(argv=None):
    """CLI entry point wrapper to allow lazy loading of CLI modules."""
    from pac_man.cli import main as _main

    return _main(argv)


__all__ = [
    'executeppr',
    'executevlappr',
    'main',
    'pac_man_executeppr',
    'pac_man_executevlappr',
    'pac_man_reduce',
    'reduce',
]
