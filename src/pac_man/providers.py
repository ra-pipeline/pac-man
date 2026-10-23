"""Scheduler display names independent of Parsl's internal job identifiers."""

from __future__ import annotations

import re
from typing import Any

from parsl.providers import CondorProvider, SlurmProvider


def short_job_name(profile_name: str, role: str, parsl_job_name: str) -> str:
    """Build a safe profile-role-block name of at most 32 characters."""
    profile = re.sub(r'[^A-Za-z0-9_-]+', '-', profile_name).strip('-_') or 'pacman'
    if role not in ('t0', 'wl'):
        raise ValueError('Job role must be t0 or wl')
    match = re.search(r'\.block-(\d+)(?:\.|$)', parsl_job_name)
    block = f'{int(match.group(1)):02d}' if match else '00'
    suffix = f'-{role}-{block}'
    return profile[:max(1, 32 - len(suffix))] + suffix


class _JobNameMixin:
    """Carry display metadata while forwarding scheduler options to Parsl."""

    profile_name: str
    role: str

    def __init__(self, *, profile_name: str = 'pacman', role: str = 't0', **kwargs: Any) -> None:
        self.profile_name = profile_name
        self.role = role
        super().__init__(**kwargs)


class NamedSlurmProvider(_JobNameMixin, SlurmProvider):
    """Give Slurm jobs short display names while keeping unique script paths."""

    def _write_submit_script(self, template: str, script_filename: str, job_name: str, configs: dict[str, Any]) -> None:
        display_name = short_job_name(self.profile_name, self.role, job_name)
        template = template.replace('#SBATCH --job-name=${jobname}', f'#SBATCH --job-name={display_name}')
        super()._write_submit_script(template, script_filename, job_name, configs)


class NamedCondorProvider(_JobNameMixin, CondorProvider):
    """Expose short HTCondor batch names without changing script or log paths."""

    def _write_submit_script(self, template: str, script_filename: str, job_name: str, configs: dict[str, Any]) -> None:
        display_name = short_job_name(self.profile_name, self.role, job_name)
        configs = dict(configs)
        configs['scheduler_options'] += f'\n+JobBatchName = "{display_name}"\n'
        super()._write_submit_script(template, script_filename, job_name, configs)
