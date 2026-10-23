"""Offline checks of scheduler display names and original Parsl identifiers."""

from __future__ import annotations

from pathlib import Path
import re
import shutil
import subprocess
from unittest.mock import patch

import pytest

from pac_man import config
from pac_man.providers import short_job_name


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
