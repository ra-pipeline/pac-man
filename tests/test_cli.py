"""Unit tests for PAC-MAN configuration loader and CLI entry point."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pac_man import cli, config_loader


@pytest.fixture
def sample_config_yaml(tmp_path: Path) -> Path:
    """Create a temporary sample YAML configuration file."""
    yaml_content = """
default_profile: alma_test

profiles:
  base:
    backend: subprocess
    loglevel: warning
    parsl_loglevel: error
    monitoring_local_dir: /tmp/demo-scratch
    parsl_monitoring: true
    workdir: /tmp/test_workdir
    max_weblog_workers: 3
    slurm_partition: compute
    slurm_mail_user: alice@example.org
    slurm_mail_type: BEGIN,END,FAIL
    condor_project: demo
    condor_schedd: schedd.example.org
    condor_pool: collector.example.org
    slurm_tier0_memory_gb: 64
    slurm_weblog_memory_gb: 4
    slurm_walltime: "08:30:00"
    slurm_max_idletime: 600

  alma_test:
    inherits: base
    mode: ppr
    telescope: alma
    ppr: test_alma_ppr.xml
    bpaction: break

  vla_test:
    inherits: base
    mode: ppr
    telescope: vla
    ppr: test_vla_ppr.xml
    plotlevel: summary

  recipe_test:
    inherits: base
    mode: recipe
    procedure: procedure_hifv.xml
    vis:
      - dataset_a.ms
      - dataset_b.ms
"""
    cfg_file = tmp_path / "pacman.yaml"
    cfg_file.write_text(yaml_content, encoding="utf-8")
    return cfg_file


@pytest.fixture
def sample_config_toml(tmp_path: Path) -> Path:
    """Create a temporary sample TOML configuration file."""
    toml_content = """
default_profile = "vla_test"

[profiles.base]
backend = "subprocess"
loglevel = "info"
max_weblog_workers = 4

[profiles.vla_test]
inherits = "base"
mode = "ppr"
telescope = "vla"
ppr = "test_vla_ppr.xml"
"""
    cfg_file = tmp_path / "pacman.toml"
    cfg_file.write_text(toml_content, encoding="utf-8")
    return cfg_file


def test_load_yaml_and_toml_config(sample_config_yaml: Path, sample_config_toml: Path):
    """Test loading both YAML and TOML configuration files."""
    yaml_cfg = config_loader.load_raw_config(sample_config_yaml)
    assert "profiles" in yaml_cfg
    assert "alma_test" in yaml_cfg["profiles"]

    toml_cfg = config_loader.load_raw_config(sample_config_toml)
    assert "profiles" in toml_cfg
    assert "vla_test" in toml_cfg["profiles"]


def test_profile_inheritance_and_deep_merge(sample_config_yaml: Path):
    """Test that profile inheritance merges base settings and applies overrides."""
    profile = config_loader.load_profile(sample_config_yaml, "alma_test")

    assert profile.name == "alma_test"
    assert profile.mode == "ppr"
    assert profile.telescope == "alma"
    # Inherited from base
    assert profile.backend == "subprocess"
    assert profile.loglevel == "warning"
    assert profile.parsl_loglevel == "error"
    assert profile.monitoring_local_dir == "/tmp/demo-scratch"
    assert profile.parsl_monitoring is True
    assert profile.max_weblog_workers == 3
    assert profile.slurm_partition == "compute"
    assert profile.slurm_mail_user == "alice@example.org"
    assert profile.slurm_mail_type == "BEGIN,END,FAIL"
    assert profile.condor_project == "demo"
    assert profile.condor_schedd == "schedd.example.org"
    assert profile.condor_pool == "collector.example.org"
    assert profile.slurm_tier0_memory_gb == 64
    assert profile.slurm_weblog_memory_gb == 4
    # Overridden / set in alma_test
    assert profile.bpaction == "break"
    assert profile.ppr.endswith("test_alma_ppr.xml")


def test_profile_default_selection(sample_config_yaml: Path):
    """Test that omitting profile name selects default_profile."""
    profile = config_loader.load_profile(sample_config_yaml)
    assert profile.name == "alma_test"


def test_cyclic_inheritance_detection(tmp_path: Path):
    """Test that circular profile inheritance raises ValueError."""
    bad_yaml = """
profiles:
  a:
    inherits: b
  b:
    inherits: a
"""
    cfg_path = tmp_path / "cyclic.yaml"
    cfg_path.write_text(bad_yaml, encoding="utf-8")

    with pytest.raises(ValueError, match="Cyclic profile inheritance detected"):
        config_loader.load_profile(cfg_path, "a")


def test_find_default_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test discovering configuration file in current directory and user home."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PIXI_PROJECT_ROOT", raising=False)
    monkeypatch.delenv("PACMAN_CONFIG", raising=False)
    monkeypatch.setattr(config_loader, "USER_CONFIG_CANDIDATES", [])
    assert config_loader.find_default_config(tmp_path) is None

    cfg = tmp_path / "pacman.yaml"
    cfg.write_text("default_profile: foo\nprofiles:\n  foo:\n    mode: ppr\n", encoding="utf-8")
    assert config_loader.find_default_config(tmp_path) == cfg


def test_find_user_home_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test discovering user-level ~/.pacman/config.toml."""
    empty_workdir = tmp_path / "workdir"
    empty_workdir.mkdir()
    monkeypatch.chdir(empty_workdir)
    monkeypatch.delenv("PIXI_PROJECT_ROOT", raising=False)
    monkeypatch.delenv("PACMAN_CONFIG", raising=False)

    user_pacman_dir = tmp_path / ".pacman"
    user_pacman_dir.mkdir()
    user_config = user_pacman_dir / "config.toml"
    user_config.write_text("default_profile = 'user'\n[profiles.user]\nmode = 'ppr'\n", encoding="utf-8")

    monkeypatch.setattr(config_loader, "USER_CONFIG_CANDIDATES", [user_config])
    assert config_loader.find_default_config(empty_workdir) == user_config


def test_cli_list_profiles(sample_config_yaml: Path, capsys: pytest.CaptureFixture):
    """Test CLI --list-profiles output."""
    exit_code = cli.main(["--config", str(sample_config_yaml), "--list-profiles"])
    assert exit_code == 0
    captured = capsys.readouterr().out
    assert "alma_test" in captured
    assert "vla_test" in captured
    assert "recipe_test" in captured


def test_cli_dry_run_ppr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test CLI --dry-run validation for PPR mode."""
    monkeypatch.chdir(tmp_path)
    ppr_file = tmp_path / "test.ppr.xml"
    ppr_file.write_text("<SciPipeRequest></SciPipeRequest>", encoding="utf-8")

    cfg_file = tmp_path / "pacman.yaml"
    cfg_file.write_text(
        f"""
profiles:
  default:
    mode: ppr
    ppr: {ppr_file.name}
""",
        encoding="utf-8",
    )

    exit_code = cli.main(["--config", str(cfg_file), "--dry-run"])
    assert exit_code == 0


def test_cli_missing_ppr_file_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test that a nonexistent PPR file returns exit code 1."""
    monkeypatch.chdir(tmp_path)
    cfg_file = tmp_path / "pacman.yaml"
    cfg_file.write_text(
        """
profiles:
  default:
    mode: ppr
    ppr: nonexistent_ppr.xml
""",
        encoding="utf-8",
    )

    exit_code = cli.main(["--config", str(cfg_file)])
    assert exit_code == 1


@pytest.mark.parametrize(
    'logging_options, expected_parsl_level',
    [([], 'info'), (['--loglevel', 'debug'], 'info'),
     (['--loglevel', 'debug', '--parsl-loglevel', 'warning'], 'warning')],
)
def test_cli_dispatch_alma_ppr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, logging_options: list[str], expected_parsl_level: str
):
    """Test that ALMA PPR mode dispatches to pac_man_executeppr with resolved arguments."""
    monkeypatch.chdir(tmp_path)
    ppr_file = tmp_path / "PPR_uid___A001_X101_X101.xml"
    ppr_file.write_text("<SciPipeRequest></SciPipeRequest>", encoding="utf-8")

    cfg_file = tmp_path / "pacman.yaml"
    cfg_file.write_text(
        f"""
profiles:
  default:
    mode: ppr
    telescope: alma
    ppr: {ppr_file.name}
    backend: subprocess
    bpaction: break
    slurm_partition: compute
    condor_requirements: TARGET.HasFeature == true
    slurm_tier0_memory_gb: 64
    slurm_weblog_memory_gb: 4
    slurm_walltime: "08:30:00"
    slurm_max_idletime: 600
    monitoring_local_dir: /tmp/demo-scratch
    parsl_monitoring: true
    slurm_mail_user: alice@example.org
    slurm_mail_type: BEGIN,END,FAIL
    condor_schedd: schedd.example.org
    condor_pool: collector.example.org
""",
        encoding="utf-8",
    )

    with patch("pac_man.runner.pac_man_executeppr") as mock_exec:
        mock_exec.return_value = MagicMock()
        exit_code = cli.main(["--config", str(cfg_file), *logging_options])
        assert exit_code == 0
        mock_exec.assert_called_once()
        _, kwargs = mock_exec.call_args
        assert kwargs["pprXmlFile"] == str(ppr_file.resolve())
        assert kwargs["bpaction"] == "break"
        assert kwargs["backend"] == "subprocess"
        assert kwargs["slurm_partition"] == "compute"
        assert kwargs["condor_requirements"] == "TARGET.HasFeature == true"
        assert kwargs["slurm_tier0_memory_gb"] == 64
        assert kwargs["slurm_weblog_memory_gb"] == 4
        assert kwargs["slurm_walltime"] == "08:30:00"
        assert kwargs["slurm_max_idletime"] == 600
        assert kwargs["parsl_loglevel"] == expected_parsl_level
        assert kwargs["profile_name"] == "default"
        assert kwargs["monitoring_local_dir"] == "/tmp/demo-scratch"
        assert kwargs["parsl_monitoring"] is True
        assert kwargs["slurm_mail_user"] == "alice@example.org"
        assert kwargs["slurm_mail_type"] == "BEGIN,END,FAIL"
        assert kwargs["condor_schedd"] == "schedd.example.org"
        assert kwargs["condor_pool"] == "collector.example.org"
        assert kwargs["loglevel"] == ('debug' if logging_options else 'info')


def test_cli_dispatch_vla_ppr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test that VLA PPR mode dispatches to pac_man_executevlappr."""
    monkeypatch.chdir(tmp_path)
    ppr_file = tmp_path / "PPR_VLA.xml"
    ppr_file.write_text("<SciPipeRequest></SciPipeRequest>", encoding="utf-8")

    cfg_file = tmp_path / "pacman.yaml"
    cfg_file.write_text(
        f"""
profiles:
  default:
    mode: ppr
    telescope: vla
    ppr: {ppr_file.name}
    backend: threads
""",
        encoding="utf-8",
    )

    with patch("pac_man.runner.pac_man_executevlappr") as mock_exec:
        mock_exec.return_value = MagicMock()
        exit_code = cli.main(["--config", str(cfg_file)])
        assert exit_code == 0
        mock_exec.assert_called_once()
        _, kwargs = mock_exec.call_args
        assert kwargs["pprXmlFile"] == str(ppr_file.resolve())
        assert kwargs["backend"] == "threads"
        assert kwargs["profile_name"] == "default"


def test_cli_dispatch_recipe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test that recipe mode dispatches to pac_man_reduce with datasets."""
    monkeypatch.chdir(tmp_path)
    ds1 = tmp_path / "test1.ms"
    ds1.mkdir()
    ds2 = tmp_path / "test2.ms"
    ds2.mkdir()

    cfg_file = tmp_path / "pacman.yaml"
    cfg_file.write_text(
        f"""
profiles:
  default:
    mode: recipe
    procedure: procedure_hifv.xml
    vis:
      - {ds1.name}
      - {ds2.name}
""",
        encoding="utf-8",
    )

    with patch("pac_man.runner.pac_man_reduce") as mock_reduce:
        mock_reduce.return_value = MagicMock()
        exit_code = cli.main(["--config", str(cfg_file)])
        assert exit_code == 0
        mock_reduce.assert_called_once()
        _, kwargs = mock_reduce.call_args
        assert kwargs["procedure"] == "procedure_hifv.xml"
        assert kwargs["profile_name"] == "default"
        assert len(kwargs["vis"]) == 2


@pytest.mark.parametrize('setting, expected', [('', False), ('parsl_monitoring = false', False), ('parsl_monitoring = true', True)])
def test_parsl_diagnostics_profile_opt_in(tmp_path, setting, expected):
    path = tmp_path / 'config.toml'
    path.write_text('[profiles.default]\nmode = "recipe"\nprocedure = "demo.xml"\n' + setting + '\n')
    assert config_loader.load_profile(path).parsl_monitoring is expected


def test_parsl_diagnostics_rejects_string_boolean(tmp_path):
    path = tmp_path / 'config.toml'
    path.write_text('[profiles.default]\nmode = "recipe"\nparsl_monitoring = "false"\n')
    with pytest.raises(ValueError, match='parsl_monitoring must be a boolean'):
        config_loader.load_profile(path)


def test_rawdata_profile_path_is_config_relative_and_preserves_symlink(tmp_path, monkeypatch):
    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    (tmp_path / 'datasets').mkdir()
    alias = tmp_path / 'rawdata'
    alias.symlink_to(tmp_path / 'datasets', target_is_directory=True)
    config_path = config_dir / 'config.toml'
    config_path.write_text('[profiles.default]\nmode = "ppr"\nrawdata = "../rawdata"\n')
    monkeypatch.chdir(tmp_path / 'datasets')
    profile = config_loader.load_profile(config_path)
    assert profile.rawdata == str(alias)


@pytest.mark.parametrize('telescope, entry', [('alma', 'pac_man_executeppr'), ('vla', 'pac_man_executevlappr')])
def test_cli_forwards_explicit_rawdata_directory(tmp_path, monkeypatch, telescope, entry):
    monkeypatch.chdir(tmp_path)
    ppr = tmp_path / 'PPR.xml'
    ppr.write_text('<SciPipeRequest/>')
    rawdata = tmp_path / 'rawdata'
    rawdata.mkdir()
    config_path = tmp_path / 'config.toml'
    config_path.write_text(
        f'[profiles.default]\nmode = "ppr"\ntelescope = "{telescope}"\nppr = "{ppr}"\nrawdata = "rawdata"\n'
    )
    with patch(f'pac_man.runner.{entry}') as execute:
        assert cli.main(['--config', str(config_path)]) == 0
        assert execute.call_args.kwargs['rawdata_dir'] == str(rawdata)


@pytest.mark.parametrize('telescope, entry', [('alma', 'pac_man_executeppr'), ('vla', 'pac_man_executevlappr')])
@pytest.mark.parametrize('selection', ['profile', 'override'])
@pytest.mark.parametrize('spelling', ['project', 'legacy_working'])
def test_ppr_discovers_rawdata_beside_logical_workdir(tmp_path, monkeypatch, telescope, entry, selection, spelling):
    physical = tmp_path / 'project/archive/member/working'
    physical.mkdir(parents=True)
    logical = tmp_path / 'project/working'
    logical.symlink_to(physical, target_is_directory=True)
    datasets = tmp_path / 'datasets'
    datasets.mkdir()
    rawdata = tmp_path / 'project/rawdata'
    rawdata.symlink_to(datasets, target_is_directory=True)
    ppr = tmp_path / 'PPR.xml'
    ppr.write_text('<SciPipeRequest/>')
    config_path = tmp_path / 'config.toml'
    selected = logical.parent if spelling == 'project' else logical
    configured_workdir = selected if selection == 'profile' else tmp_path / 'other'
    config_path.write_text(
        f'[profiles.default]\nmode = "ppr"\ntelescope = "{telescope}"\nppr = "{ppr}"\nworkdir = "{configured_workdir}"\n'
    )
    monkeypatch.chdir(tmp_path)
    arguments = ['--config', str(config_path)]
    if selection == 'override':
        arguments += ['--workdir', str(selected)]
    with patch(f'pac_man.runner.{entry}') as execute:
        assert cli.main(arguments) == 0
        assert execute.call_args.kwargs['rawdata_dir'] == str(rawdata)
        assert Path.cwd() == physical


@pytest.mark.parametrize('explicit', [False, True])
def test_ppr_logical_rawdata_precedence_and_missing_candidate_fallback(tmp_path, monkeypatch, explicit):
    working = tmp_path / 'project/working'
    working.mkdir(parents=True)
    override = tmp_path / 'explicit-rawdata'
    override.mkdir()
    if explicit:
        (working.parent / 'rawdata').mkdir()
    ppr = tmp_path / 'PPR.xml'
    ppr.write_text('<SciPipeRequest/>')
    profile = config_loader.ResolvedProfile(
        name='test', mode='ppr', telescope='alma', ppr=str(ppr), workdir=str(working),
        rawdata=str(override) if explicit else None,
    )
    monkeypatch.chdir(tmp_path)
    with patch('pac_man.runner.pac_man_executeppr') as execute:
        cli.apply_preflight_environment(profile)
        cli.execute_profile(profile)
    assert execute.call_args.kwargs['rawdata_dir'] == (str(override) if explicit else None)


@pytest.mark.parametrize('value, message', [
    ('""', 'rawdata must be a nonempty path string'),
    ('42', 'rawdata must be a nonempty path string'),
    ('"$PACMAN_MISSING_RAWDATA"', 'rawdata contains an unset environment variable'),
])
def test_rawdata_profile_validates_path(tmp_path, monkeypatch, value, message):
    monkeypatch.delenv('PACMAN_MISSING_RAWDATA', raising=False)
    path = tmp_path / 'config.toml'
    path.write_text(f'[profiles.default]\nmode = "ppr"\nrawdata = {value}\n')
    with pytest.raises(ValueError, match=message):
        config_loader.load_profile(path)


@pytest.mark.parametrize('legacy', [False, True])
def test_config_relative_workdir_preserves_execution_symlink(tmp_path, monkeypatch, legacy):
    config_dir = tmp_path / 'configs'
    project = config_dir / 'project'
    project.mkdir(parents=True)
    execution = tmp_path / 'archive/member/execution'
    execution.mkdir(parents=True)
    (project / 'working').symlink_to(execution, target_is_directory=True)
    (project / 'rawdata').mkdir()
    (project / 'PPR.xml').write_text('<SciPipeRequest/>')
    config_path = config_dir / 'config.toml'
    selected = 'project/working' if legacy else 'project'
    config_path.write_text(
        f'[profiles.default]\nmode="ppr"\ntelescope="alma"\nppr="project/PPR.xml"\nworkdir="{selected}"\n'
    )
    caller = tmp_path / 'caller'
    caller.mkdir()
    monkeypatch.chdir(caller)
    profile = config_loader.load_profile(config_path)
    assert profile.workdir == str(config_dir / selected)
    with patch('pac_man.runner.pac_man_executeppr') as execute:
        assert cli.main(['--config', str(config_path)]) == 0
        assert Path.cwd() == execution
        assert execute.call_args.kwargs['rawdata_dir'] == str(project / 'rawdata')
    assert not (execution / 'working').exists()
