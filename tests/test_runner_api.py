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
        'parsl_loglevel', 'profile_name', 'monitoring_local_dir', 'parsl_monitoring',
        'slurm_mail_user', 'slurm_mail_type',
        'condor_schedd', 'condor_pool',
    ):
        assert signature.parameters[option].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize('name', ['pac_man_executeppr', 'pac_man_executevlappr'])
def test_ppr_rawdata_directory_is_keyword_only(name):
    signature = inspect.signature(getattr(runner, name))
    assert signature.parameters['rawdata_dir'].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters['rawdata_dir'].default is None


@pytest.mark.parametrize('name, task', [
    ('pac_man_executeppr', 'hifa_importdata'),
    ('pac_man_executevlappr', 'hifv_importdata'),
])
@pytest.mark.parametrize('has_raw_dataset', [True, False])
@pytest.mark.parametrize('scheduler_root', ['', '/unused/scheduler-root'])
def test_ppr_explicit_rawdata_ignores_symlinked_working_parent(
    tmp_path, monkeypatch, name, task, has_raw_dataset, scheduler_root,
):
    """Both PPR runners pass the explicit directory through to import task inputs."""
    physical_working = tmp_path / 'project/archive/member/execution'
    physical_working.mkdir(parents=True)
    logical_working = tmp_path / 'project/working'
    logical_working.symlink_to(physical_working, target_is_directory=True)
    real_raw = tmp_path / 'datasets/rawdata'
    real_raw.mkdir(parents=True)
    logical_raw = tmp_path / 'project/rawdata'
    logical_raw.symlink_to(real_raw, target_is_directory=True)
    if has_raw_dataset:
        (real_raw / 'synthetic.ms').mkdir()
    # A same-named local dataset must not override an explicit rawdata directory.
    (physical_working / 'synthetic.ms').mkdir()
    monkeypatch.chdir(logical_working)
    monkeypatch.setenv('SCIPIPE_ROOTDIR', scheduler_root)

    execute = _mock_ppr_execution(monkeypatch, task)
    getattr(runner, name)('synthetic.xml', rawdata_dir=str(logical_raw))
    import_args = execute.call_args.kwargs['task_sequence'][0][1]
    assert import_args['vis'] == [str(logical_raw / 'synthetic.ms')]
    assert execute.call_args.kwargs['working_dir'] == str(physical_working)


def test_rawdata_override_validates_and_expands_paths(tmp_path, monkeypatch):
    monkeypatch.setenv('PPR_TEST_DATA', str(tmp_path))
    assert runner._resolve_ppr_rawdata_dir('$PPR_TEST_DATA', 'unused') == str(tmp_path)
    assert runner._resolve_ppr_rawdata_dir(None, 'existing-default') == 'existing-default'
    with pytest.raises(ValueError, match='empty'):
        runner._resolve_ppr_rawdata_dir('', 'unused')
    with pytest.raises(FileNotFoundError, match='raw-data directory not found'):
        runner._resolve_ppr_rawdata_dir(str(tmp_path / 'missing'), 'unused')


def _mock_ppr_execution(monkeypatch, task):
    from types import ModuleType
    from unittest.mock import MagicMock

    parser = MagicMock()
    parser._getFirstRequest.return_value = (
        [], [], 'archive/member', {}, [('archive/member', 'synthetic.ms', 'uid://synthetic')],
        'synthetic-procedure', [(task, {})],
    )
    parser._getProcessingIntents.return_value = {}
    pipeline = ModuleType('pipeline')
    pipeline.cli = MagicMock()
    infrastructure = ModuleType('pipeline.infrastructure')
    infrastructure.executeppr = parser
    infrastructure.executevlappr = parser
    infrastructure.filenamer = MagicMock()
    infrastructure.project = MagicMock()
    pipeline.infrastructure = infrastructure
    monkeypatch.setitem(__import__('sys').modules, 'pipeline', pipeline)
    monkeypatch.setitem(__import__('sys').modules, 'pipeline.infrastructure', infrastructure)
    execute = MagicMock()
    monkeypatch.setattr(runner, '_run_task_sequence', execute)
    return execute


@pytest.mark.parametrize('name, task', [
    ('pac_man_executeppr', 'hifa_importdata'),
    ('pac_man_executevlappr', 'hifv_importdata'),
])
@pytest.mark.parametrize('root_kind', ['unset', 'empty', 'populated'])
def test_ppr_scheduler_root_routing_without_override(tmp_path, monkeypatch, name, task, root_kind):
    working = tmp_path / 'local/working'
    working.mkdir(parents=True)
    monkeypatch.chdir(working)
    if root_kind == 'unset':
        monkeypatch.delenv('SCIPIPE_ROOTDIR', raising=False)
    else:
        monkeypatch.setenv('SCIPIPE_ROOTDIR', str(tmp_path / 'scheduler') if root_kind == 'populated' else '')
    base = tmp_path / 'scheduler/archive/member' if root_kind == 'populated' else working.parent
    rawdata = base / 'rawdata'
    (rawdata / 'synthetic.ms').mkdir(parents=True)
    execute = _mock_ppr_execution(monkeypatch, task)
    getattr(runner, name)('synthetic.xml')
    assert execute.call_args.kwargs['task_sequence'][0][1]['vis'] == [str(rawdata / 'synthetic.ms')]
    assert execute.call_args.kwargs['working_dir'] == str(base / 'working')
