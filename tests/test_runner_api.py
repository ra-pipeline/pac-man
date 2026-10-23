"""Regression tests for the runner's public calling conventions."""

from __future__ import annotations

import inspect

import pytest

from pac_man import runner


@pytest.mark.parametrize(
    'name, legacy_names',
    [
        (
            'pac_man_reduce',
            [
                'vis', 'infiles', 'procedure', 'context', 'name', 'loglevel', 'plotlevel', 'session',
                'exitstage', 'startstage', 'backend', 'max_tier0_workers', 'max_weblog_workers',
                'casalog', 'log2term',
            ],
        ),
        (
            'pac_man_executeppr',
            [
                'pprXmlFile', 'importonly', 'bp_name', 'bpaction', 'loglevel', 'plotlevel', 'backend',
                'max_tier0_workers', 'max_weblog_workers', 'casalog', 'log2term', 'startstage', 'exitstage',
            ],
        ),
        (
            'pac_man_executevlappr',
            [
                'pprXmlFile', 'importonly', 'loglevel', 'plotlevel', 'backend', 'max_tier0_workers',
                'max_weblog_workers', 'casalog', 'log2term', 'startstage', 'exitstage',
            ],
        ),
    ],
)
def test_legacy_positional_arguments(name: str, legacy_names: list[str]) -> None:
    """Old positional calls keep their meaning alongside new keyword-only options."""
    signature = inspect.signature(getattr(runner, name))
    values = [object() for _ in legacy_names]
    bound = signature.bind(*values, slurm_partition='compute', slurm_tier0_memory_gb=64)
    for argument, value in zip(legacy_names, values):
        assert bound.arguments[argument] is value
    assert bound.arguments['slurm_partition'] == 'compute'
    assert bound.arguments['slurm_tier0_memory_gb'] == 64
    for option in (
        'slurm_partition', 'slurm_account', 'slurm_qos', 'slurm_constraint',
        'condor_project', 'condor_requirements', 'condor_accounting_group',
        'slurm_tier0_memory_gb', 'slurm_weblog_memory_gb',
        'slurm_walltime', 'slurm_max_idletime',
        'parsl_loglevel', 'profile_name', 'monitoring_local_dir',
        'slurm_mail_user', 'slurm_mail_type',
        'condor_schedd', 'condor_pool',
    ):
        assert signature.parameters[option].kind is inspect.Parameter.KEYWORD_ONLY
