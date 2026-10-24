"""Scheduler display names independent of Parsl's internal job identifiers."""

from __future__ import annotations

import re
import shlex
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
    """Expose batch names and optionally manage workers through a remote schedd."""

    def __init__(self, *, schedd: str | None = None, pool: str | None = None, **kwargs: Any) -> None:
        self.schedd = self._clean_target(schedd, 'condor_schedd')
        self.pool = self._clean_target(pool, 'condor_pool')
        if self.pool and not self.schedd:
            raise ValueError('condor_pool requires condor_schedd to identify the worker queue')
        super().__init__(**kwargs)

    @staticmethod
    def _clean_target(value: str | None, setting: str) -> str | None:
        """Accept daemon names and collector host lists without command options."""
        if value is None:
            return None
        if not isinstance(value, str) or '\n' in value or '\r' in value:
            raise ValueError(f'{setting} must be a daemon name or hostname without newlines')
        value = value.strip()
        if not value:
            return None
        if value.startswith('-') or not re.fullmatch(r'[A-Za-z0-9_.@:\[\],-]+', value):
            raise ValueError(f'{setting} must be a daemon name or hostname, not command-line options')
        return value

    def execute_wait(self, cmd: str, timeout: int | None = None) -> tuple[int, str, str]:
        """Route every provider command, including both cancellation commands."""
        if not self.schedd:
            return super().execute_wait(cmd, timeout=timeout)
        flags = ['-name', self.schedd]
        if self.pool:
            flags += ['-pool', self.pool]
        if cmd.startswith('condor_submit '):
            # Parsl supplies the path without quoting; treat the entire suffix as one path.
            routed = shlex.join(['condor_submit', *flags, cmd[len('condor_submit '):]])
        else:
            commands = []
            for part in cmd.split(';'):
                args = shlex.split(part)
                if not args or args[0] not in ('condor_q', 'condor_rm'):
                    raise ValueError('Unexpected HTCondor provider command; remote routing was not applied')
                commands.append(shlex.join([args[0], *flags, *args[1:]]))
            routed = '; '.join(commands)
        result = super().execute_wait(routed, timeout=timeout)
        if cmd.startswith('condor_q ') and result[0] != 0:
            raise RuntimeError(
                f'Cannot query condor_schedd {self.schedd}; check connectivity, HTCondor authentication, and queue access'
            )
        return result

    def _write_submit_script(self, template: str, script_filename: str, job_name: str, configs: dict[str, Any]) -> None:
        display_name = short_job_name(self.profile_name, self.role, job_name)
        configs = dict(configs)
        configs['scheduler_options'] += f'\n+JobBatchName = "{display_name}"\n'
        super()._write_submit_script(template, script_filename, job_name, configs)
