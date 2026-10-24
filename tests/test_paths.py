"""Project-root workdir semantics across command-line entry points."""
import os
from pathlib import Path
import runpy

import pytest

from pac_man import cli, config_loader
from pac_man.paths import project_directories


@pytest.mark.parametrize('legacy', [False, True])
def test_project_layout_preserves_working_symlink(tmp_path, monkeypatch, legacy):
    root = tmp_path / 'project'
    root.mkdir()
    physical = tmp_path / 'archive/member/execution'
    physical.mkdir(parents=True)
    (root / 'working').symlink_to(physical, target_is_directory=True)
    selected = root / 'working' if legacy else root
    assert project_directories(selected) == (root, root / 'working', root / 'rawdata')
    monkeypatch.chdir(tmp_path)
    profile = config_loader.ResolvedProfile(name='test', mode='recipe', workdir=str(selected))
    assert cli.apply_preflight_environment(profile) == physical
    assert profile.workdir == str(root)
    assert Path.cwd() == physical
    assert not (physical / 'working').exists()


def test_default_project_root_creates_execution_subdirectory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('INIT_CWD', raising=False)
    profile = config_loader.ResolvedProfile(name='test', mode='recipe')
    assert cli.apply_preflight_environment(profile) == tmp_path / 'working'
    assert profile.workdir == str(tmp_path)
    assert Path.cwd() == tmp_path / 'working'


@pytest.mark.parametrize('legacy', [False, True])
def test_pipeline_test_script_uses_same_project_layout(tmp_path, monkeypatch, legacy):
    script = Path(__file__).resolve().parents[1] / 'scripts/test_pipeline.py'
    main = runpy.run_path(str(script), run_name='pacman_test_pipeline')['main']
    root = tmp_path / 'project'
    selected = root / 'working' if legacy else root
    monkeypatch.chdir(tmp_path)
    monkeypatch.setitem(main.__globals__, 'run_pipeline', lambda **kwargs: os.getcwd())
    assert main(['--workdir', str(selected), '--dry-run']) == str(root / 'working')
    assert not (root / 'working/working').exists()


@pytest.mark.parametrize('custom_root', [False, True])
def test_pipeline_test_resolves_caller_relative_inputs_before_chdir(tmp_path, monkeypatch, custom_root):
    script = Path(__file__).resolve().parents[1] / 'scripts/test_pipeline.py'
    main = runpy.run_path(str(script), run_name='pacman_test_pipeline')['main']
    caller = tmp_path / 'caller'
    (caller / 'rawdata/synthetic.ms').mkdir(parents=True)
    (caller / 'procedure.xml').write_text('<procedure/>')
    monkeypatch.chdir(caller)
    monkeypatch.setenv('INIT_CWD', str(caller))
    arguments = ['--vis', 'rawdata/synthetic.ms', '--procedure', 'procedure.xml', '--dry-run']
    project = tmp_path / 'output' if custom_root else caller
    if custom_root:
        arguments += ['--workdir', str(project)]
    # Exercise the real dataset validator, not just a mocked pipeline dispatch.
    assert main(arguments) is None
    assert Path.cwd() == project / 'working'


def test_pipeline_test_preserves_unresolved_casa_aliases(tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[1] / 'scripts/test_pipeline.py'
    main = runpy.run_path(str(script), run_name='pacman_test_pipeline')['main']
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('INIT_CWD', str(tmp_path))
    monkeypatch.setitem(main.__globals__, 'run_pipeline', lambda **kwargs: kwargs)
    result = main(['--vis', 'casa-data-alias.ms', '--procedure', 'procedure_alias.xml', '--dry-run'])
    assert result['vis'] == ['casa-data-alias.ms']
    assert result['procedure'] == 'procedure_alias.xml'
