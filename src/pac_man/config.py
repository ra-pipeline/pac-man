import os
from pathlib import Path

import parsl
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers.base import Launcher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.monitoring.radios.udp import UDPRadio
from parsl.providers import CondorProvider, LocalProvider, SlurmProvider

_PACKAGE_SRC = str(Path(__file__).resolve().parent.parent)


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

    def __call__(self, command: str, tasks_per_node: int, nodes_per_block: int, script_dir: str = "") -> str:
        # -a: Automatically find a free server number
        # -s: Pass arguments to Xvfb (e.g. screen resolution)
        # We use `env -u DBUS_SESSION_BUS_ADDRESS` to prevent plotms from connecting to the interactive session's DBus when getenv=true in Condor.
        # We also set QT_X11_NO_MITSHM=1 because HTCondor restricts /dev/shm which causes Qt apps to deadlock when drawing to Xvfb.
        # OMP_NUM_THREADS=1 prevents openMP thread deadlocks in restricted Condor cgroups.
        return f"env -u DBUS_SESSION_BUS_ADDRESS QT_X11_NO_MITSHM=1 OMP_NUM_THREADS=1 xvfb-run -a -s '-screen 0 1024x768x24' {command}"


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
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
            HighThroughputExecutor(
                label='weblog_executor',
                max_workers_per_node=max_weblog_workers,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1,
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0),
            ),
        ],
        monitoring=get_monitoring_hub(),
    )

def get_condor_config(max_tier0_workers: int = 50, max_weblog_workers: int = 2) -> Config:
    """Returns a Parsl Config tailored for HTCondor OSG execution.
    
    Args:
        max_tier0_workers: Dynamic upper limit for the number of pipeline crunching workers.
        max_weblog_workers: Dynamic upper limit for the number of concurrent weblog rendering workers.
    """
    return Config(
        executors=[
            # 1. High-Memory Executor for pipeline crunching tasks (Tier0)
            HighThroughputExecutor(
                label='tier0_executor',
                address=address_by_hostname(),
                provider=CondorProvider(
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=max_tier0_workers, # Dynamically scales up to this limit based on queued tasks
                    worker_init=f"export PATH={os.environ.get('PATH', '')}:$PATH && export PYTHONPATH={_PACKAGE_SRC}:$PYTHONPATH",
                    scheduler_options="getenv = true\nrequest_memory = 32768\n" # Heavy tasks get 32GB RAM
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0)
            ),
            # 2. Lower-Memory Executor dedicated for weblog rendering
            HighThroughputExecutor(
                label='weblog_executor',
                address=address_by_hostname(),
                provider=CondorProvider(
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=max_weblog_workers, # Renderers don't need to scale infinitely
                    worker_init=f"export PATH={os.environ.get('PATH', '')}:$PATH && export PYTHONPATH={_PACKAGE_SRC}:$PYTHONPATH",
                    scheduler_options="getenv = true\nrequest_memory = 8192\n" # Weblogs only need 8GB RAM
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=0)
            )
        ],
        monitoring=get_monitoring_hub()
    )

def get_slurm_config() -> Config:
    """Returns a Parsl Config tailored for Slurm HPC execution."""
    return Config(
        executors=[
            HighThroughputExecutor(
                label='slurm_executor',
                address=address_by_hostname(),
                provider=SlurmProvider(
                    partition='compute', # Update with correct partition if needed
                    init_blocks=1,
                    max_blocks=10,
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=55055)
            )
        ],
        monitoring=get_monitoring_hub()
    )

def initialize_parsl(backend: str = "subprocess", max_tier0_workers: int | None = None, max_weblog_workers: int = 2):
    """Initializes Parsl with the appropriate configuration.

    Args:
        backend: The execution backend to use ("threads", "subprocess", "htcondor", "slurm").
        max_tier0_workers: Maximum workers for tier0 tasks (default: 4 for subprocess, 50 for htcondor).
        max_weblog_workers: Maximum workers for background weblog rendering.
    """
    if backend == "threads":
        config = get_local_threads_config()
    elif backend == "subprocess":
        effective_tier0 = 4 if max_tier0_workers is None else max_tier0_workers
        config = get_local_subprocess_config(
            max_tier0_workers=effective_tier0,
            max_weblog_workers=max_weblog_workers,
        )
    elif backend == "htcondor":
        effective_tier0 = 50 if max_tier0_workers is None else max_tier0_workers
        config = get_condor_config(max_tier0_workers=effective_tier0, max_weblog_workers=max_weblog_workers)
    elif backend == "slurm":
        config = get_slurm_config()
    else:
        raise ValueError(f"Unknown Parsl backend requested: {backend}. Choose from: threads, subprocess, htcondor, slurm")

    parsl.load(config)
