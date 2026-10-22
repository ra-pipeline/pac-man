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

__all__ = [
    'executeppr',
    'executevlappr',
    'pac_man_executeppr',
    'pac_man_executevlappr',
    'pac_man_reduce',
    'reduce',
]
