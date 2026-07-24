import os
import parsl
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor, HighThroughputExecutor
from parsl.providers import LocalProvider, CondorProvider, SlurmProvider
from parsl.addresses import address_by_hostname
from parsl.launchers.base import Launcher
from parsl.monitoring.monitoring import MonitoringHub
from parsl.monitoring.radios.udp import UDPRadio

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
    """
    Returns a Parsl Config using local Python threads. 
    WARNING: Not safe for CASA execution as CASA is not thread-safe.
    """
    return Config(
        executors=[
            ThreadPoolExecutor(
                max_threads=4,
                label='local_threads',
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=55055)
            )
        ],
        monitoring=get_monitoring_hub()
    )

def get_local_subprocess_config() -> Config:
    """
    Returns a Parsl Config using local subprocesses. 
    Safe for CASA execution since each task gets its own memory space.
    """
    return Config(
        executors=[
            HighThroughputExecutor(
                label='local_subprocess',
                max_workers_per_node=4,
                provider=LocalProvider(
                    init_blocks=1,
                    max_blocks=1
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=55055)
            )
        ],
        monitoring=get_monitoring_hub()
    )

def get_condor_config() -> Config:
    """Returns a Parsl Config tailored for HTCondor OSG execution."""
    return Config(
        executors=[
            HighThroughputExecutor(
                label='htcondor_executor',
                address=address_by_hostname(),
                provider=CondorProvider(
                    init_blocks=1,
                    max_blocks=10,
                    worker_init=f"export PATH={os.environ.get('PATH', '')}:$PATH && export PYTHONPATH=/home/rxue/Workspace/nvme/nrao/github/pac-man/src:$PYTHONPATH",
                    scheduler_options="getenv = true\nrequest_memory = 16384\n"
                ),
                remote_monitoring_radio=UDPRadio(address=address_by_hostname(), port=55055)
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

def initialize_parsl(backend: str = "subprocess"):
    """
    Initializes Parsl with the appropriate configuration.
    
    Args:
        backend (str): The execution backend to use. 
                       Options: "threads", "subprocess", "htcondor", "slurm"
    """
    if backend == "threads":
        config = get_local_threads_config()
    elif backend == "subprocess":
        config = get_local_subprocess_config()
    elif backend == "htcondor":
        config = get_condor_config()
    elif backend == "slurm":
        config = get_slurm_config()
    else:
        raise ValueError(f"Unknown Parsl backend requested: {backend}. Choose from: threads, subprocess, htcondor, slurm")
        
    parsl.load(config)
