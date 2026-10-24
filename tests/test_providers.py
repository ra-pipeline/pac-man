"""Offline checks of scheduler display names and original Parsl identifiers."""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import subprocess
import shlex
from unittest.mock import patch

import pytest

from pac_man import config
from pac_man.providers import short_job_name
from pac_man.providers import NamedCondorProvider
from parsl.jobs.states import JobState
from parsl.providers.cluster_provider import ClusterProvider


@pytest.mark.parametrize(
    'profile, role, original, expected',
    [
        ('demo', 't0', 'parsl.tier0_executor.block-4.123.456', 'demo-t0-04'),
        ('demo', 'wl', 'parsl.weblog_executor.block-1.123.456', 'demo-wl-01'),
        ('demo', 't0', 'parsl.tier0_executor.block-123', 'demo-t0-123'),
        (' demo/one\nqueue ', 'wl', 'parsl.weblog_executor.block-0', 'demo-one-queue-wl-00'),
        ('!!!', 't0', 'parsl.slurm.123', 'pacman-t0-00'),
        ('a' * 80, 't0', 'parsl.tier0_executor.block-4', 'a' * 26 + '-t0-04'),
    ],
)
def test_short_job_name(profile: str, role: str, original: str, expected: str) -> None:
    """Names preserve roles and block numbers while sanitizing profile text."""
    assert short_job_name(profile, role, original) == expected


def test_invalid_role() -> None:
    """Only the two configured worker roles are accepted."""
    with pytest.raises(ValueError, match='Job role'):
        short_job_name('demo', 'unknown', 'parsl.block-0')


@pytest.mark.parametrize('backend', ['slurm', 'htcondor'])
def test_submit_display_names_preserve_internal_identifiers(backend: str, tmp_path: Path) -> None:
    """Actual Parsl templates contain short names and unique paths without submitting jobs."""
    provider_class = config.SlurmProvider if backend == 'slurm' else config.CondorProvider
    response = 'Submitted batch job 123' if backend == 'slurm' else '1 job(s) submitted to cluster 123.\n'
    with (
        patch.object(provider_class, 'execute_wait', return_value=(0, response, '')),
        patch.object(config, 'address_by_hostname', return_value='127.0.0.1'),
        patch.object(config, 'get_monitoring_hub', return_value=None),
    ):
        if backend == 'slurm':
            parsl_config = config.get_slurm_config(profile_name='demo')
        else:
            parsl_config = config.get_condor_config(profile_name='demo', project='demo', accounting_group='group.demo')
        for executor, role in zip(parsl_config.executors, ['t0', 'wl']):
            provider = executor.provider
            provider.script_dir = str(tmp_path / executor.label)
            Path(provider.script_dir).mkdir()
            original = f'parsl.{executor.label}.block-4'
            provider.submit('true', tasks_per_node=1, job_name=original)
            suffix = '.submit' if backend == 'htcondor' else ''
            prefix = 'parsl.' if backend == 'htcondor' else ''
            scripts = list(Path(provider.script_dir).glob(f'{prefix}{original}.*{suffix}'))
            if backend == 'slurm':
                script = scripts[0].read_text()
                assert '#SBATCH --job-name=demo-' + role + '-04\n' in script
                assert f'export JOBNAME="{original}.' in script
            else:
                assert len(scripts) == 1
                script = scripts[0].read_text()
                assert f'+JobBatchName = "demo-{role}-04"' in script
                if shutil.which('condor_submit'):
                    job_ad = tmp_path / f'{role}.ad'
                    subprocess.run(
                        ['condor_submit', '-dry-run', str(job_ad), str(scripts[0])],
                        check=True, capture_output=True, text=True,
                    )
                    assert re.search(rf'^JobBatchName\s*=\s*"demo-{role}-04"$', job_ad.read_text(), re.MULTILINE)
            assert f'{original}.' in script
            assert 'demo-' + role not in scripts[0].name
            assert repr(provider)


@pytest.mark.parametrize('backend, factory_name', [('slurm', 'get_slurm_config'), ('htcondor', 'get_condor_config')])
def test_initialize_forwards_profile(backend: str, factory_name: str) -> None:
    """The chosen profile reaches either scheduler factory."""
    with patch.object(config, factory_name) as factory, patch.object(config.parsl, 'load'):
        config.initialize_parsl(backend=backend, profile_name='demo')
    assert factory.call_args.kwargs['profile_name'] == 'demo'


@pytest.mark.parametrize('pool', [None, 'collector.example.org:9618'])
def test_remote_condor_lifecycle_routes_every_command(pool: str | None, tmp_path: Path) -> None:
    """Submit, poll, and both removal commands use the same remote queue without spooling."""
    commands = []
    state = '2'

    def execute(cmd: str, timeout: int | None = None) -> tuple[int, str, str]:
        commands.append(cmd)
        if cmd.startswith('condor_submit '):
            return 0, '1 job(s) submitted to cluster 123.\n', ''
        if cmd.startswith('condor_q '):
            return 0, f'123.0 {state}\n', ''
        return 0, '', ''

    with patch.object(ClusterProvider, 'execute_wait', side_effect=execute):
        provider = NamedCondorProvider(schedd='alice@schedd.example.org', pool=pool, profile_name='demo')
        provider.script_dir = str(tmp_path)
        job = provider.submit('true', 1, job_name='parsl.tier0_executor.block-0')
        assert job == '123.0'
        assert provider.status([job])[0].state == JobState.RUNNING
        state = '4'
        assert provider.status([job])[0].state == JobState.COMPLETED
        assert provider.status([job])[0].state == JobState.COMPLETED
        assert not any(cmd.startswith('condor_transfer_data ') for cmd in commands)
        assert provider.cancel([job]) == [True]
    programs = []
    for command in commands:
        for part in command.split(';'):
            args = shlex.split(part)
            programs.append(args[0])
            assert args[1:3] == ['-name', 'alice@schedd.example.org']
            if pool:
                assert args[3:5] == ['-pool', pool]
            else:
                assert '-pool' not in args
            if args[0] == 'condor_submit':
                assert '-spool' not in args
                assert '-remote' not in args
                assert Path(args[-1]).is_file()
    assert programs.count('condor_rm') == 2
    assert 'condor_submit' in programs and 'condor_q' in programs


def test_remote_condor_query_failure_is_actionable() -> None:
    """Query errors are surfaced rather than parsed as valid status output."""
    provider = NamedCondorProvider(schedd='schedd.example.org')
    provider._add_resource(['123.0'])
    with patch.object(ClusterProvider, 'execute_wait', return_value=(1, '', 'synthetic permission denied')):
        with pytest.raises(RuntimeError, match='authentication'):
            provider.status(['123.0'])


def test_remote_submit_quotes_entire_path(tmp_path: Path) -> None:
    """Spaces and shell characters in Parsl's generated path remain one argument."""
    path = tmp_path / 'demo files; echo injected.submit'
    provider = NamedCondorProvider(schedd='schedd.example.org')
    with patch.object(ClusterProvider, 'execute_wait', return_value=(0, '', '')) as execute:
        provider.execute_wait(f'condor_submit {path}', timeout=17)
    args = shlex.split(execute.call_args.args[0])
    assert args == ['condor_submit', '-name', 'schedd.example.org', str(path)]
    assert execute.call_args.kwargs['timeout'] == 17


def test_default_condor_commands_are_unchanged() -> None:
    """Opting out of remote routing preserves Parsl's local command behavior."""
    provider = NamedCondorProvider()
    for command in ('condor_submit demo.submit', 'condor_q 123.0 -af:jr JobStatus', 'condor_rm 123.0; condor_rm -forcex 123.0'):
        with patch.object(ClusterProvider, 'execute_wait', return_value=(0, '', '')) as execute:
            provider.execute_wait(command)
        assert execute.call_args.args[0] == command


@pytest.mark.parametrize('settings', [
    dict(pool='collector.example.org'),
    dict(schedd='-all'),
    dict(schedd='schedd.example.org\ncondor_rm -all'),
    dict(schedd='schedd.example.org;echo injected'),
    dict(schedd='schedd.example.org', pool='collector.example.org -all'),
])
def test_invalid_remote_targets_fail_before_commands(settings: dict) -> None:
    """Daemon selectors cannot inject command options or shell syntax."""
    with patch.object(ClusterProvider, 'execute_wait') as execute:
        with pytest.raises(ValueError):
            NamedCondorProvider(**settings)
        execute.assert_not_called()
