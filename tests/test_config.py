"""Unit tests for scheduler-specific Parsl configuration."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from parsl.jobs.states import JobState, JobStatus
from parsl.jobs.strategy import Strategy

from pac_man import config


def test_slurm_partition_inherits_from_parent_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """An enclosing Slurm allocation supplies the default worker partition."""
    monkeypatch.setenv("SLURM_JOB_PARTITION", "parent-partition")

    with (
        patch.object(config, "Config", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "HighThroughputExecutor", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "SlurmProvider", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "UDPRadio", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "address_by_hostname", return_value="127.0.0.1"),
        patch.object(config, "get_monitoring_hub", return_value=None),
    ):
        parsl_config = config.get_slurm_config()

    executors = parsl_config["executors"]
    assert [executor["label"] for executor in executors] == ["tier0_executor", "weblog_executor"]
    assert all(executor["provider"]["partition"] == "parent-partition" for executor in executors)


def test_explicit_slurm_partition_overrides_parent_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit profile value takes precedence over the enclosing allocation."""
    monkeypatch.setenv("SLURM_JOB_PARTITION", "parent-partition")

    with (
        patch.object(config, "Config", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "HighThroughputExecutor", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "SlurmProvider", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "UDPRadio", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "address_by_hostname", return_value="127.0.0.1"),
        patch.object(config, "get_monitoring_hub", return_value=None),
    ):
        parsl_config = config.get_slurm_config(partition="requested-partition")

    assert all(
        executor["provider"]["partition"] == "requested-partition"
        for executor in parsl_config["executors"]
    )


def test_slurm_partition_uses_site_default_outside_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """No partition directive is emitted when neither config nor a parent job supplies one."""
    monkeypatch.delenv("SLURM_JOB_PARTITION", raising=False)

    with (
        patch.object(config, "Config", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "HighThroughputExecutor", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "SlurmProvider", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "UDPRadio", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "address_by_hostname", return_value="127.0.0.1"),
        patch.object(config, "get_monitoring_hub", return_value=None),
    ):
        parsl_config = config.get_slurm_config()

    assert all(executor["provider"]["partition"] is None for executor in parsl_config["executors"])


@pytest.mark.parametrize(
    'settings, expected',
    [
        ({}, [(10, 32), (2, 8)]),
        (
            dict(max_tier0_workers=4, max_weblog_workers=1, tier0_memory_gb=64, weblog_memory_gb=4),
            [(4, 64), (1, 4)],
        ),
    ],
)
def test_slurm_dynamic_worker_submit_scripts(settings: dict, expected: list, tmp_path: Path) -> None:
    """Each demand-driven block requests one worker's resources without contacting Slurm."""
    with (
        patch.object(config.SlurmProvider, 'execute_wait', return_value=(0, 'Submitted batch job 123', '')),
        patch.object(config, 'address_by_hostname', return_value='127.0.0.1'),
        patch.object(config, 'get_monitoring_hub', return_value=None),
    ):
        parsl_config = config.get_slurm_config(**settings)
        assert parsl_config.strategy == 'htex_auto_scale'
        assert parsl_config.max_idletime == 120
        for executor, (workers, memory) in zip(parsl_config.executors, expected):
            provider = executor.provider
            assert executor.max_workers_per_node == 1
            assert executor.workers_per_node == 1
            assert executor.cores_per_worker == 1
            assert provider.init_blocks == provider.min_blocks == 0
            assert provider.max_blocks == workers
            assert provider.nodes_per_block == 1
            assert provider.exclusive is False
            provider.script_dir = str(tmp_path / executor.label)
            Path(provider.script_dir).mkdir()
            assert provider.submit('true', tasks_per_node=1) == '123'
            script = next(Path(provider.script_dir).glob('parsl.slurm.*')).read_text()
            assert '#SBATCH --ntasks-per-node=1\n' in script
            assert '#SBATCH --cpus-per-task=1\n' in script
            assert f'#SBATCH --mem={memory}g\n' in script
            assert '#SBATCH --exclusive' not in script


@pytest.mark.parametrize('setting', ['max_tier0_workers', 'max_weblog_workers', 'tier0_memory_gb', 'weblog_memory_gb'])
@pytest.mark.parametrize('value', [0, -1])
def test_slurm_rejects_nonpositive_resources(setting: str, value: int) -> None:
    """Invalid resources fail before any scheduler access."""
    with pytest.raises(ValueError, match=f'{setting} must be positive'):
        config.get_slurm_config(**{setting: value})


def test_slurm_initialize_forwards_memory_budgets() -> None:
    """Profile resource settings reach the Slurm configuration factory."""
    with patch.object(config, 'get_slurm_config') as factory, patch.object(config.parsl, 'load'):
        config.initialize_parsl(
            backend='slurm', max_tier0_workers=0, slurm_tier0_memory_gb=64, slurm_weblog_memory_gb=4
        )
    assert factory.call_args.kwargs['max_tier0_workers'] == 10
    assert factory.call_args.kwargs['tier0_memory_gb'] == 64
    assert factory.call_args.kwargs['weblog_memory_gb'] == 4


def test_slurm_scaling_tracks_demand_and_releases_idle_blocks() -> None:
    """The configured strategy starts empty, caps growth, and retires idle workers."""
    executor = Mock(spec=config.HighThroughputExecutor)
    executor.label = 'test_compute'
    executor.bad_state_is_set = False
    executor.provider = SimpleNamespace(init_blocks=0, min_blocks=0, max_blocks=8, nodes_per_block=1, parallelism=1)
    executor.workers_per_node = 1
    executor.status_facade = {}
    executor.outstanding.return_value = 0
    strategy = Strategy(strategy='htex_auto_scale', max_idletime=120)
    strategy.add_executors([executor])

    strategy.strategize([executor])
    executor.scale_out_facade.assert_called_once_with(0)
    executor.scale_out_facade.reset_mock()
    executor.outstanding.return_value = 3
    strategy.strategize([executor])
    executor.scale_out_facade.assert_called_once_with(3)

    executor.scale_out_facade.reset_mock()
    executor.outstanding.return_value = 12
    strategy.strategize([executor])
    executor.scale_out_facade.assert_called_once_with(8)

    executor.status_facade = {str(i): JobStatus(JobState.RUNNING) for i in range(3)}
    executor.outstanding.return_value = 1
    strategy.strategize([executor])
    executor.scale_in_facade.assert_called_once_with(2, max_idletime=120)

    executor.scale_in_facade.reset_mock()
    executor.outstanding.return_value = 0
    with patch('parsl.jobs.strategy.time.time', return_value=100):
        strategy.strategize([executor])
    executor.scale_in_facade.assert_not_called()
    with patch('parsl.jobs.strategy.time.time', return_value=221):
        strategy.strategize([executor])
    executor.scale_in_facade.assert_called_once_with(3)


def test_condor_routing_inherits_from_parent_job_ad(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Condor project and accounting metadata inherit from the enclosing job."""
    job_ad = tmp_path / ".job.ad"
    job_ad.write_text(
        'ProjectName = "demo"\nAcctGroup = "group.demo"\n'
        'AcctGroupUser = "alice"\nAccountingGroup = "group.demo.alice"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("_CONDOR_JOB_AD", str(job_ad))

    project, accounting_group = config._resolve_condor_routing(None, None)

    assert project == "demo"
    assert accounting_group == "group.demo"


def test_condor_routing_explicit_values_override_parent_job_ad(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit Condor routing values take precedence over the parent job ad."""
    job_ad = tmp_path / ".job.ad"
    job_ad.write_text(
        'ProjectName = "parent"\nAcctGroup = "group.parent"\n'
        'AcctGroupUser = "alice"\nAccountingGroup = "group.parent.alice"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("_CONDOR_JOB_AD", str(job_ad))

    project, accounting_group = config._resolve_condor_routing("child", "group.child")

    assert project == "child"
    assert accounting_group == "group.child"


def test_condor_config_applies_matching_and_accounting_controls() -> None:
    """Both Condor worker pools receive the requested routing controls."""
    with (
        patch.object(config, "Config", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "HighThroughputExecutor", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "CondorProvider", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "UDPRadio", side_effect=lambda **kwargs: kwargs),
        patch.object(config, "address_by_hostname", return_value="127.0.0.1"),
        patch.object(config, "get_monitoring_hub", return_value=None),
    ):
        parsl_config = config.get_condor_config(
            project="demo",
            requirements="TARGET.HasFeature == true",
            accounting_group="group.demo",
        )

    for executor in parsl_config["executors"]:
        provider = executor["provider"]
        assert provider["project"] == '"demo"'
        assert provider["requirements"] == "TARGET.HasFeature == true"
        assert 'accounting_group = group.demo\n' in provider["scheduler_options"]


def test_condor_accounting_group_rejects_newlines() -> None:
    """ClassAd configuration values cannot inject additional submit directives."""
    with pytest.raises(ValueError, match="must not contain newlines"):
        config._condor_scheduler_options(1024, "group.demo\nqueue")


def test_condor_routing_without_parent_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An accounting identity alone must not be reused as a group name."""
    job_ad = tmp_path / '.job.ad'
    job_ad.write_text('AccountingGroup = "alice"\n', encoding='utf-8')
    monkeypatch.setenv('_CONDOR_JOB_AD', str(job_ad))

    assert config._resolve_condor_routing(None, None) == (None, None)


@pytest.mark.skipif(shutil.which('condor_submit') is None, reason='condor_submit is not installed')
@pytest.mark.parametrize('inherit', [False, True])
def test_condor_accounting_group_submit_dry_run(
    inherit: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Condor parses explicit and inherited groups without quotes or repeated users."""
    parent_ad = tmp_path / 'parent.ad'
    parent_ad.write_text(
        'AcctGroup = "group.demo"\nAcctGroupUser = "alice"\n'
        'AccountingGroup = "group.demo.alice"\n',
        encoding='utf-8',
    )
    monkeypatch.setenv('_CONDOR_JOB_AD', str(parent_ad))
    _, group = config._resolve_condor_routing(None, None if inherit else 'group.demo')
    submit_file = tmp_path / 'synthetic.submit'
    submit_file.write_text(
        'universe = vanilla\nexecutable = /bin/true\n'
        + config._condor_scheduler_options(1024, group)
        + 'accounting_group_user = alice\nqueue\n',
        encoding='utf-8',
    )
    output_ad = tmp_path / 'child.ad'
    subprocess.run(
        ['condor_submit', '-dry-run', str(output_ad), str(submit_file)],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    monkeypatch.setenv('_CONDOR_JOB_AD', str(output_ad))
    attributes = config._read_condor_job_ad()
    assert attributes['acctgroup'] == 'group.demo'
    assert attributes['acctgroupuser'] == 'alice'
    assert attributes['accountinggroup'] == 'group.demo.alice'
