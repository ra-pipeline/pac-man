from __future__ import annotations

import os
import re
from pathlib import Path

import parsl
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers.base import Launcher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.monitoring.radios.udp import UDPRadio
from parsl.providers import LocalProvider

from pac_man.logging_utils import configure_console_logging
from pac_man.monitoring import configure_local_monitoring, publish_monitoring_location
from pac_man.providers import NamedCondorProvider as CondorProvider, NamedSlurmProvider as SlurmProvider

_PACKAGE_SRC = str(Path(__file__).resolve().parent.parent)


def _clean_optional_setting(value: str | None) -> str | None:
    """Return a stripped scheduler setting, treating blank strings as unset."""
    if value is None:
        return None
    if '\n' in value or '\r' in value:
        raise ValueError('Scheduler settings must not contain newlines')
    stripped = value.strip()
    return stripped or None


def _slurm_mail_options(mail_user: str | None, mail_type: str | None) -> str:
    """Build validated Slurm mail directives for both worker pools."""
    user = _clean_optional_setting(mail_user)
    types = _clean_optional_setting(mail_type)
    options = []
    if user:
        if not re.fullmatch(r'[A-Za-z0-9_.+%@,-]+', user):
            raise ValueError('slurm_mail_user must be a login or email address (comma-separated recipients allowed)')
        options.append(f'#SBATCH --mail-user={user}')
    if types:
        events = list(dict.fromkeys(event.strip().upper() for event in types.split(',')))
        valid = {
            'NONE', 'BEGIN', 'END', 'FAIL', 'REQUEUE', 'ALL', 'INVALID_DEPEND',
            'STAGE_OUT', 'TIME_LIMIT', 'TIME_LIMIT_90', 'TIME_LIMIT_80', 'TIME_LIMIT_50', 'ARRAY_TASKS',
        }
        if any(event not in valid for event in events):
            raise ValueError('slurm_mail_type must contain comma-separated Slurm mail events, e.g. BEGIN,END,FAIL')
        if 'NONE' in events and len(events) != 1:
            raise ValueError('slurm_mail_type NONE cannot be combined with other events')
        options.append(f"#SBATCH --mail-type={','.join(events)}")
    return '\n'.join(options) + ('\n' if options else '')


def _quote_condor_string(value: str) -> str:
    """Quote a string for use as an HTCondor ClassAd value."""
    escaped = value.replace('\\', '\\\\').replace('"', r'\"')
    return f'"{escaped}"'


def _read_condor_job_ad() -> dict[str, str]:
    """Read simple string attributes from the enclosing HTCondor job ad."""
    job_ad_path = os.environ.get('_CONDOR_JOB_AD')
    if not job_ad_path:
        return {}

    attributes: dict[str, str] = {}
    try:
        with Path(job_ad_path).open(encoding='utf-8') as job_ad:
            for line in job_ad:
                match = re.match(r'^\s*([A-Za-z][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$', line)
                if not match:
                    continue
                key, value = match.groups()
                if len(value) >= 2 and value[0] == value[-1] == '"':
                    value = value[1:-1].replace(r'\"', '"').replace(r'\\', '\\')
                if value and value.lower() not in {'undefined', 'error'}:
                    attributes[key.casefold()] = value
    except (OSError, UnicodeError):
        return {}

    return attributes


def _resolve_condor_routing(
    project: str | None,
    accounting_group: str | None,
) -> tuple[str | None, str | None]:
    """Resolve explicit Condor routing settings or inherit them from a parent job."""
    resolved_project = _clean_optional_setting(project)
    resolved_accounting_group = _clean_optional_setting(accounting_group)
    if resolved_project is not None and resolved_accounting_group is not None:
        return resolved_project, resolved_accounting_group

    parent_ad = _read_condor_job_ad()
    if resolved_project is None:
        resolved_project = parent_ad.get('projectname')
    if resolved_accounting_group is None:
        # AccountingGroup includes the user suffix; condor_submit adds that
        # suffix itself, so inherit only the actual group component.
        resolved_accounting_group = parent_ad.get('acctgroup')
    return resolved_project, resolved_accounting_group


def _condor_scheduler_options(memory_mb: int, accounting_group: str | None) -> str:
    """Build safe HTCondor submit options shared by worker pools."""
    options = ['getenv = true', f'request_memory = {memory_mb}']
    if accounting_group:
        cleaned_accounting_group = _clean_optional_setting(accounting_group)
        if cleaned_accounting_group:
            # This submit command takes a raw group name, not a ClassAd string.
            options.append(f'accounting_group = {cleaned_accounting_group}')
    return '\n'.join(options) + '\n'


def get_monitoring_hub() -> MonitoringHub:
    """Returns a pre-configured monitoring hub for SQLite database logging."""
    return MonitoringHub(
        hub_address=address_by_hostname(),
        hub_port=55055,
        monitoring_debug=False,
        resource_monitoring_interval=10,
    )


class XvfbLauncher(Launcher):
    """
    Wraps the worker command in xvfb-run to provide a virtual X11 display.
    This prevents CASA tasks like plotms from hanging or crashing when rendering
    plots headlessly.
    """

    def __init__(self, debug: bool = True):
        super().__init__(debug=debug)

    def __call__(self, command: str, tasks_per_node: int, nodes_per_block: int, script_dir: str = '') -> str:
        # -a: find a free server number; QT_X11_NO_MITSHM=1 prevents Qt deadlock on /dev/shm-restricted Condor nodes.
        # DBUS and OMP are already set by WORKER_THREAD_LIMIT_ENV via worker_init; -u here is belt-and-suspenders for xvfb subprocesses.
        return f"env -u DBUS_SESSION_BUS_ADDRESS QT_X11_NO_MITSHM=1 xvfb-run -a -s '-screen 0 1024x768x24' {command}"


def get_local_threads_config() -> Config:
    """Returns a Parsl Config using local Python threads.

    WARNING: Not safe for CASA execution as CASA is not thread-safe.
    """
    return Config(
        executors=[
            ThreadPoolExecutor(
                max_threads=4,
                label='tier0_executor',
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
            ThreadPoolExecutor(
                max_threads=2,
                label='weblog_executor',
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
            ThreadPoolExecutor(
                max_threads=4,
                label='local_threads',
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
        ],
        monitoring=get_monitoring_hub(),
    )


# Prevent casaplotms/Qt from connecting to the SSH-forwarded DBus, which causes hangs on headless workers.
WORKER_THREAD_LIMIT_ENV = (
    'export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 '
    'VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1 DBUS_SESSION_BUS_ADDRESS=/dev/null'
)


def get_local_subprocess_config(max_tier0_workers: int = 4, max_weblog_workers: int = 2) -> Config:
    """Returns a Parsl Config using local subprocesses.

    Safe for CASA execution since each task gets its own memory space.
    Provides dedicated pools for Tier0 pipeline compute and background Weblog rendering.
    """
    return Config(
        executors=[
            HighThroughputExecutor(
                label='tier0_executor',
                max_workers_per_node=max_tier0_workers,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                    worker_init=WORKER_THREAD_LIMIT_ENV,
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
            HighThroughputExecutor(
                label='weblog_executor',
                max_workers_per_node=max_weblog_workers,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                    worker_init=WORKER_THREAD_LIMIT_ENV,
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
        ],
        monitoring=get_monitoring_hub(),
    )


def get_condor_config(
    max_tier0_workers: int = 50,
    max_weblog_workers: int = 2,
    project: str | None = None,
    requirements: str | None = None,
    accounting_group: str | None = None,
    profile_name: str = 'pacman',
    schedd: str | None = None,
    pool: str | None = None,
) -> Config:
    """Returns a Parsl Config tailored for HTCondor OSG execution.

    Args:
        max_tier0_workers: Dynamic upper limit for the number of pipeline crunching workers.
        max_weblog_workers: Dynamic upper limit for the number of concurrent weblog rendering workers.
        project: HTCondor project name, inherited from an enclosing Condor job when omitted.
        requirements: Optional HTCondor ClassAd requirements expression.
        accounting_group: Accounting group, inherited from an enclosing Condor job when omitted.
        profile_name: Short profile prefix for scheduler display names.
        schedd: Optional remote schedd daemon name.
        pool: Optional collector host for locating the schedd.
    """
    project, accounting_group = _resolve_condor_routing(project, accounting_group)
    provider_project = _quote_condor_string(project) if project else 'undefined'
    provider_requirements = _clean_optional_setting(requirements) or ''
    return Config(
        executors=[
            # 1. High-Memory Executor for pipeline crunching tasks (Tier0)
            HighThroughputExecutor(
                label='tier0_executor',
                address=address_by_hostname(),
                provider=CondorProvider(
                    profile_name=profile_name,
                    schedd=schedd,
                    pool=pool,
                    role='t0',
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=max_tier0_workers,  # Dynamically scales up to this limit based on queued tasks
                    project=provider_project,
                    requirements=provider_requirements,
                    worker_init=f'{WORKER_THREAD_LIMIT_ENV} && export PATH={os.environ.get("PATH", "")}:$PATH && export PYTHONPATH={_PACKAGE_SRC}:$PYTHONPATH',
                    scheduler_options=_condor_scheduler_options(32768, accounting_group),
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
            # 2. Lower-Memory Executor dedicated for weblog rendering
            HighThroughputExecutor(
                label='weblog_executor',
                address=address_by_hostname(),
                provider=CondorProvider(
                    profile_name=profile_name,
                    schedd=schedd,
                    pool=pool,
                    role='wl',
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=max_weblog_workers,  # Renderers don't need to scale infinitely
                    project=provider_project,
                    requirements=provider_requirements,
                    worker_init=f'{WORKER_THREAD_LIMIT_ENV} && export PATH={os.environ.get("PATH", "")}:$PATH && export PYTHONPATH={_PACKAGE_SRC}:$PYTHONPATH',
                    scheduler_options=_condor_scheduler_options(8192, accounting_group),
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
        ],
        monitoring=get_monitoring_hub(),
    )


def get_slurm_config(
    max_tier0_workers: int = 10,
    max_weblog_workers: int = 2,
    partition: str | None = None,
    account: str | None = None,
    qos: str | None = None,
    constraint: str | None = None,
    tier0_memory_gb: int = 32,
    weblog_memory_gb: int = 8,
    profile_name: str = 'pacman',
    mail_user: str | None = None,
    mail_type: str | None = None,
) -> Config:
    """Scale nonexclusive Slurm blocks on demand, with one CPU and worker per block.

    Memory settings apply to each worker's block. Worker limits cap the
    number of blocks in each pool.
    Optional mail settings request scheduler notifications for each worker job.
    """
    for name, value in (
        ('max_tier0_workers', max_tier0_workers),
        ('max_weblog_workers', max_weblog_workers),
        ('tier0_memory_gb', tier0_memory_gb),
        ('weblog_memory_gb', weblog_memory_gb),
    ):
        if value < 1:
            raise ValueError(f'{name} must be positive')
    mail_options = _slurm_mail_options(mail_user, mail_type)
    effective_partition = _clean_optional_setting(partition) or _clean_optional_setting(
        os.environ.get('SLURM_JOB_PARTITION')
    )
    return Config(
        executors=[
            HighThroughputExecutor(
                label='tier0_executor',
                address=address_by_hostname(),
                max_workers_per_node=1,
                cores_per_worker=1,
                provider=SlurmProvider(
                    profile_name=profile_name,
                    role='t0',
                    exclusive=False,
                    nodes_per_block=1,
                    cores_per_node=1,
                    mem_per_node=tier0_memory_gb,
                    scheduler_options=mail_options,
                    partition=effective_partition,
                    account=_clean_optional_setting(account),
                    qos=_clean_optional_setting(qos),
                    constraint=_clean_optional_setting(constraint),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=max_tier0_workers,
                    worker_init=WORKER_THREAD_LIMIT_ENV,
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
            HighThroughputExecutor(
                label='weblog_executor',
                address=address_by_hostname(),
                max_workers_per_node=1,
                cores_per_worker=1,
                provider=SlurmProvider(
                    profile_name=profile_name,
                    role='wl',
                    exclusive=False,
                    nodes_per_block=1,
                    cores_per_node=1,
                    mem_per_node=weblog_memory_gb,
                    scheduler_options=mail_options,
                    partition=effective_partition,
                    account=_clean_optional_setting(account),
                    qos=_clean_optional_setting(qos),
                    constraint=_clean_optional_setting(constraint),
                    init_blocks=0,
                    min_blocks=0,
                    max_blocks=max_weblog_workers,
                    worker_init=WORKER_THREAD_LIMIT_ENV,
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
        ],
        strategy='htex_auto_scale',
        max_idletime=120,
        monitoring=get_monitoring_hub(),
    )


def initialize_parsl(
    backend: str = 'subprocess',
    max_tier0_workers: int | None = None,
    max_weblog_workers: int = 2,
    slurm_partition: str | None = None,
    slurm_account: str | None = None,
    slurm_qos: str | None = None,
    slurm_constraint: str | None = None,
    condor_project: str | None = None,
    condor_requirements: str | None = None,
    condor_accounting_group: str | None = None,
    slurm_tier0_memory_gb: int = 32,
    slurm_weblog_memory_gb: int = 8,
    parsl_loglevel: str = 'info',
    profile_name: str = 'pacman',
    monitoring_local_dir: str | None = None,
    slurm_mail_user: str | None = None,
    slurm_mail_type: str | None = None,
    condor_schedd: str | None = None,
    condor_pool: str | None = None,
) -> None:
    """Initializes Parsl with the appropriate configuration.

    Args:
        backend: The execution backend to use ("threads", "subprocess", "htcondor", "slurm").
        max_tier0_workers: Maximum workers for tier0 tasks (default: 4 for subprocess, 50 for htcondor).
        max_weblog_workers: Maximum workers for background weblog rendering.
        slurm_partition: Slurm partition; inherits the current job partition when omitted.
        slurm_account: Optional Slurm account.
        slurm_qos: Optional Slurm quality of service.
        slurm_constraint: Optional Slurm node constraint.
        condor_project: Optional HTCondor project name.
        condor_requirements: Optional HTCondor ClassAd requirements expression.
        condor_accounting_group: Optional HTCondor accounting group.
        slurm_tier0_memory_gb: Memory requested per Slurm compute worker.
        slurm_weblog_memory_gb: Memory requested per Slurm weblog worker.
        parsl_loglevel: Parsl console verbosity, independent of pipeline logging.
        profile_name: Profile prefix for Slurm job names and HTCondor batch names.
        monitoring_local_dir: Local scratch root for the live monitoring database.
        slurm_mail_user: Worker notification recipient (login or email address).
        slurm_mail_type: Comma-separated Slurm notification events, e.g. BEGIN,END,FAIL.
        condor_schedd: Remote schedd daemon name for worker job management.
        condor_pool: Collector host used to locate the remote schedd.
    """
    configure_console_logging(parsl_loglevel)
    for var in [
        'OMP_NUM_THREADS',
        'OPENBLAS_NUM_THREADS',
        'MKL_NUM_THREADS',
        'VECLIB_MAXIMUM_THREADS',
        'NUMEXPR_NUM_THREADS',
    ]:
        os.environ.setdefault(var, '1')

    if backend == 'threads':
        config = get_local_threads_config()
    elif backend == 'subprocess':
        effective_tier0 = 4 if max_tier0_workers is None else max_tier0_workers
        config = get_local_subprocess_config(
            max_tier0_workers=effective_tier0,
            max_weblog_workers=max_weblog_workers,
        )
    elif backend == 'htcondor':
        effective_tier0 = 50 if not max_tier0_workers else max_tier0_workers
        config = get_condor_config(
            max_tier0_workers=effective_tier0,
            max_weblog_workers=max_weblog_workers,
            project=condor_project,
            requirements=condor_requirements,
            accounting_group=condor_accounting_group,
            schedd=condor_schedd,
            pool=condor_pool,
            profile_name=profile_name,
        )
    elif backend == 'slurm':
        effective_tier0 = 10 if not max_tier0_workers else max_tier0_workers
        config = get_slurm_config(
            max_tier0_workers=effective_tier0,
            max_weblog_workers=max_weblog_workers,
            partition=slurm_partition,
            account=slurm_account,
            qos=slurm_qos,
            constraint=slurm_constraint,
            tier0_memory_gb=slurm_tier0_memory_gb,
            weblog_memory_gb=slurm_weblog_memory_gb,
            mail_user=slurm_mail_user,
            mail_type=slurm_mail_type,
            profile_name=profile_name,
        )
    else:
        raise ValueError(
            f'Unknown Parsl backend requested: {backend}. Choose from: threads, subprocess, htcondor, slurm'
        )

    configure_local_monitoring(config.monitoring, monitoring_local_dir)
    dfk = parsl.load(config)
    publish_monitoring_location(config.monitoring, dfk.run_dir)
