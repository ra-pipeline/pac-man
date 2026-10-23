"""PAC-MAN Command-Line Interface.

Unified CLI entry point for executing ALMA and VLA pipeline workflows via PPR or recipe,
configured through YAML/TOML configuration profiles.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from pac_man import config_loader
from pac_man.config_loader import ResolvedProfile
from pac_man.logging_utils import configure_console_logging

LOG = logging.getLogger('pac-man')


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for PAC-MAN CLI."""
    parser = argparse.ArgumentParser(
        prog='pac-man',
        description='PAC-MAN: Pipeline Orchestration using Parsl for HPC/HTC execution.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '-c',
        '--config',
        dest='config_path',
        default=None,
        help='Path to YAML or TOML configuration file (defaults to ./pacman.yaml, ./config.yaml, etc.).',
    )
    parser.add_argument(
        '-p',
        '--profile',
        dest='profile_name',
        default=None,
        help='Profile name to load from configuration file.',
    )
    parser.add_argument(
        '--list-profiles',
        action='store_true',
        help='List available profiles in the configuration file and exit.',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration, dataset paths, and settings without executing the reduction.',
    )
    parser.add_argument(
        '-w',
        '--workdir',
        default=None,
        help='Override working directory for pipeline execution.',
    )
    parser.add_argument(
        '-b',
        '--backend',
        choices=['subprocess', 'htcondor', 'slurm', 'threads'],
        default=None,
        help='Override Parsl execution backend.',
    )
    parser.add_argument(
        '--exitstage',
        type=int,
        default=None,
        help='Override stage number at which to stop execution.',
    )
    parser.add_argument(
        '--loglevel',
        choices=['debug', 'info', 'warning', 'error'],
        default=None,
        help='Override pipeline logging verbosity.',
    )
    parser.add_argument(
        '--parsl-loglevel',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default=None,
        help='Override Parsl console verbosity independently of pipeline logging.',
    )
    parser.add_argument(
        '--ppr',
        default=None,
        help='Override path to PPR XML file.',
    )
    parser.add_argument(
        '--procedure',
        default=None,
        help='Override path to pipeline XML procedure/recipe file.',
    )
    parser.add_argument(
        '--vis',
        nargs='+',
        default=None,
        help='Override input MeasurementSets / ASDMs.',
    )
    return parser


def apply_preflight_environment(profile: ResolvedProfile, workdir_override: str | None = None) -> Path:
    """Apply environment variables and prepare execution working directory.

    Args:
        profile: Resolved profile configuration.
        workdir_override: Optional CLI workdir override.

    Returns:
        Path to resolved target working directory.
    """
    # Restrict linear algebra thread limits to avoid multi-process CPU contention
    thread_limit = profile.omp_num_threads or '1'
    for var in [
        'OMP_NUM_THREADS',
        'OPENBLAS_NUM_THREADS',
        'MKL_NUM_THREADS',
        'VECLIB_MAXIMUM_THREADS',
        'NUMEXPR_NUM_THREADS',
    ]:
        os.environ.setdefault(var, thread_limit)

    # Determine execution working directory
    pixi_root = os.environ.get('PIXI_PROJECT_ROOT')
    init_cwd = os.environ.get('INIT_CWD') or os.getcwd()

    if workdir_override:
        target_dir = Path(workdir_override).resolve()
    elif profile.workdir:
        target_dir = Path(profile.workdir).resolve()
    elif pixi_root and (Path(init_cwd).resolve() == Path(pixi_root).resolve()):
        target_dir = (Path(pixi_root) / 'working').resolve()
    else:
        target_dir = Path(init_cwd).resolve()

    target_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(target_dir)

    # Configure casaconfig log options before CASA imports
    try:
        from casaconfig import config

        if profile.casalog:
            config.logfile = os.path.abspath(profile.casalog)
        else:
            now_str = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d-%H%M%S')
            config.logfile = os.path.abspath(f'casa-{now_str}.log')

        if profile.log2term is not None:
            config.log2term = bool(profile.log2term)
    except Exception:
        pass

    return target_dir


def execute_profile(profile: ResolvedProfile, dry_run: bool = False):
    """Execute pipeline reduction driven by a resolved profile.

    Args:
        profile: Resolved execution profile.
        dry_run: When True, validates inputs without executing pipeline.

    Returns:
        Pipeline context or None if dry-run.
    """
    LOG.info('Selected profile: %s (mode: %s, telescope: %s)', profile.name, profile.mode, profile.telescope)
    LOG.info('Working directory: %s', os.getcwd())
    LOG.info('Parsl backend: %s', profile.backend)

    if profile.mode == 'ppr':
        if not profile.ppr:
            raise ValueError(f"Profile '{profile.name}' specifies mode='ppr' but no PPR file was provided.")
        if not os.path.exists(profile.ppr):
            raise FileNotFoundError(f'PPR file not found: {profile.ppr}')

        LOG.info('PPR XML file: %s', profile.ppr)
        LOG.info('Import only: %s', profile.importonly)
        LOG.info('Breakpoint: name=%s action=%s', profile.bp_name, profile.bpaction)

        if dry_run:
            LOG.info('Dry-run validation successful. Execution skipped.')
            return None

        from pac_man import runner

        if profile.telescope == 'vla':
            LOG.info('Dispatching to pac_man_executevlappr...')
            return runner.pac_man_executevlappr(
                pprXmlFile=profile.ppr,
                importonly=profile.importonly,
                bp_name=profile.bp_name,
                bpaction=profile.bpaction,
                loglevel=profile.loglevel,
                plotlevel=profile.plotlevel,
                backend=profile.backend,
                max_tier0_workers=profile.max_tier0_workers,
                max_weblog_workers=profile.max_weblog_workers,
                slurm_partition=profile.slurm_partition,
                slurm_account=profile.slurm_account,
                slurm_qos=profile.slurm_qos,
                slurm_constraint=profile.slurm_constraint,
                condor_project=profile.condor_project,
                condor_requirements=profile.condor_requirements,
                condor_accounting_group=profile.condor_accounting_group,
                slurm_tier0_memory_gb=profile.slurm_tier0_memory_gb,
                slurm_weblog_memory_gb=profile.slurm_weblog_memory_gb,
                slurm_walltime=profile.slurm_walltime,
                slurm_max_idletime=profile.slurm_max_idletime,
                parsl_loglevel=profile.parsl_loglevel,
                profile_name=profile.name,
                monitoring_local_dir=profile.monitoring_local_dir,
                slurm_mail_user=profile.slurm_mail_user,
                slurm_mail_type=profile.slurm_mail_type,
                condor_schedd=profile.condor_schedd,
                condor_pool=profile.condor_pool,
                casalog=profile.casalog,
                log2term=profile.log2term,
                startstage=profile.startstage,
                exitstage=profile.exitstage,
            )
        else:
            LOG.info('Dispatching to pac_man_executeppr...')
            return runner.pac_man_executeppr(
                pprXmlFile=profile.ppr,
                importonly=profile.importonly,
                bp_name=profile.bp_name,
                bpaction=profile.bpaction,
                loglevel=profile.loglevel,
                plotlevel=profile.plotlevel,
                backend=profile.backend,
                max_tier0_workers=profile.max_tier0_workers,
                max_weblog_workers=profile.max_weblog_workers,
                slurm_partition=profile.slurm_partition,
                slurm_account=profile.slurm_account,
                slurm_qos=profile.slurm_qos,
                slurm_constraint=profile.slurm_constraint,
                condor_project=profile.condor_project,
                condor_requirements=profile.condor_requirements,
                condor_accounting_group=profile.condor_accounting_group,
                slurm_tier0_memory_gb=profile.slurm_tier0_memory_gb,
                slurm_weblog_memory_gb=profile.slurm_weblog_memory_gb,
                slurm_walltime=profile.slurm_walltime,
                slurm_max_idletime=profile.slurm_max_idletime,
                parsl_loglevel=profile.parsl_loglevel,
                profile_name=profile.name,
                monitoring_local_dir=profile.monitoring_local_dir,
                slurm_mail_user=profile.slurm_mail_user,
                slurm_mail_type=profile.slurm_mail_type,
                condor_schedd=profile.condor_schedd,
                condor_pool=profile.condor_pool,
                casalog=profile.casalog,
                log2term=profile.log2term,
                startstage=profile.startstage,
                exitstage=profile.exitstage,
            )

    elif profile.mode == 'recipe':
        if not profile.procedure:
            raise ValueError(f"Profile '{profile.name}' specifies mode='recipe' but no procedure was provided.")

        LOG.info('Procedure: %s', profile.procedure)
        LOG.info('Datasets count: %d', len(profile.vis))
        for i, v in enumerate(profile.vis, 1):
            LOG.info('  Dataset [%d]: %s', i, v)
            if not os.path.exists(v):
                raise FileNotFoundError(f'Dataset not found: {v}')

        if dry_run:
            LOG.info('Dry-run validation successful. Execution skipped.')
            return None

        from pac_man import runner

        LOG.info('Dispatching to pac_man_reduce...')
        return runner.pac_man_reduce(
            vis=profile.vis,
            infiles=profile.infiles,
            procedure=profile.procedure,
            loglevel=profile.loglevel,
            plotlevel=profile.plotlevel,
            session=profile.session,
            exitstage=profile.exitstage,
            startstage=profile.startstage,
            backend=profile.backend,
            max_tier0_workers=profile.max_tier0_workers,
            max_weblog_workers=profile.max_weblog_workers,
            slurm_partition=profile.slurm_partition,
            slurm_account=profile.slurm_account,
            slurm_qos=profile.slurm_qos,
            slurm_constraint=profile.slurm_constraint,
            condor_project=profile.condor_project,
            condor_requirements=profile.condor_requirements,
            condor_accounting_group=profile.condor_accounting_group,
            slurm_tier0_memory_gb=profile.slurm_tier0_memory_gb,
            slurm_weblog_memory_gb=profile.slurm_weblog_memory_gb,
            slurm_walltime=profile.slurm_walltime,
            slurm_max_idletime=profile.slurm_max_idletime,
            parsl_loglevel=profile.parsl_loglevel,
            profile_name=profile.name,
            monitoring_local_dir=profile.monitoring_local_dir,
            slurm_mail_user=profile.slurm_mail_user,
            slurm_mail_type=profile.slurm_mail_type,
            condor_schedd=profile.condor_schedd,
            condor_pool=profile.condor_pool,
            casalog=profile.casalog,
            log2term=profile.log2term,
        )

    else:
        raise ValueError(f"Unsupported profile execution mode: '{profile.mode}'")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI main execution entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    configure_console_logging()

    # Locate config file
    if args.config_path:
        config_file = Path(args.config_path).expanduser().resolve()
        if not config_file.is_file():
            LOG.error('Specified config file does not exist: %s', config_file)
            return 1
    else:
        config_file = config_loader.find_default_config()
        if not config_file:
            LOG.error(
                'No configuration file specified or found in current directory. '
                'Specify one using --config / -c (e.g. pac-man --config config.yaml).'
            )
            return 1

    # Handle --list-profiles
    raw_cfg = config_loader.load_raw_config(config_file)
    profiles_dict = raw_cfg.get('profiles', {})
    if args.list_profiles:
        default_prof = raw_cfg.get('default_profile', 'default')
        print(f'Profiles in {config_file}:')
        for name, p_data in sorted(profiles_dict.items()):
            marker = '*' if name == default_prof else ' '
            mode = p_data.get('mode', 'ppr')
            desc = p_data.get('description', '')
            print(f' {marker} {name:<18} [mode: {mode}] {desc}')
        return 0

    try:
        profile = config_loader.load_profile(config_file, args.profile_name)

        # Apply CLI overrides if passed
        if args.backend:
            profile.backend = args.backend
        if args.exitstage is not None:
            profile.exitstage = args.exitstage
        if args.loglevel:
            profile.loglevel = args.loglevel
        if args.parsl_loglevel:
            profile.parsl_loglevel = args.parsl_loglevel
        if args.ppr:
            profile.ppr = str(Path(args.ppr).resolve())
        if args.procedure:
            profile.procedure = str(Path(args.procedure).resolve())
        if args.vis:
            profile.vis = [str(Path(v).resolve()) for v in args.vis]

        configure_console_logging(profile.parsl_loglevel)
        apply_preflight_environment(profile, workdir_override=args.workdir)
        execute_profile(profile, dry_run=args.dry_run)
        return 0

    except Exception as err:
        LOG.error('PAC-MAN execution failed: %s', err, exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
