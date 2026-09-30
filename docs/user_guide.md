# User Guide

This guide describes how to configure and run the [CASA](https://casa.nrao.edu/) pipeline using PAC-MAN.

## 1. Environment Setup

PAC-MAN uses [Pixi](https://pixi.sh/) to manage CASA and pipeline dependencies.

Install dependencies and enter the environment shell:
```bash
pixi install
pixi shell
```

## 2. Executing a Pipeline Workflow

PAC-MAN provides `pac_man_reduce()` as a replacement for standard `recipereducer`. It accepts standard pipeline procedure definitions and MeasurementSets:

```python
from pac_man.runner import pac_man_reduce

context = pac_man_reduce(
    vis=['uid___A002_X123456_X789.ms'],
    procedure='procedure_hifv.xml',
    backend='subprocess'
)
```

### Supported Execution Backends

Specify the execution backend with the `backend` argument:

- `subprocess`: Spawns local worker processes via [Parsl HighThroughputExecutor](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html) (default for workstation runs).
- `htcondor`: Submits heavy compute jobs to [HTCondor](https://htcondor.org/) worker nodes with dedicated memory allocations (32 GB for compute, 8 GB for rendering).
- `slurm`: Submits jobs to a configured [Slurm](https://slurm.schedmd.com/) partition.
- `threads`: Runs tasks via `ThreadPoolExecutor`. **Not recommended for production runs** due to thread-safety limitations in CASA C++ libraries.

Example using HTCondor:
```python
context = pac_man_reduce(
    vis=['uid___A002_X123456_X789.ms'],
    procedure='procedure_hifv.xml',
    backend='htcondor',
    max_tier0_workers=50,
    max_weblog_workers=2
)
```

### CLI Pipeline Runner

The repository provides a consolidated test and execution script at `scripts/test_pipeline.py`. It uses `casa_tools.utils.resolve` to discover standard test MeasurementSets and maps telescope targets to default procedures:

```bash
# Run VLA reduction (default: subprocess backend, procedure_hifv.xml)
pixi run python scripts/test_pipeline.py --telescope vla

# Run ALMA reduction with HTCondor backend
pixi run python scripts/test_pipeline.py --telescope alma --backend htcondor

# Dry-run validation (checks dataset discovery and configuration without executing)
pixi run python scripts/test_pipeline.py --telescope alma --dry-run
```

Supported CLI options:

- `-t, --telescope`: Target telescope (`vla` or `alma`). Default: `vla`. Can also be passed positionally.
- `-b, --backend`: Parsl execution backend (`subprocess`, `htcondor`, `slurm`, `threads`). Default: `subprocess`.
- `--vis`: File path or relative CASA data path to a custom MeasurementSet.
- `-p, --procedure`: Pipeline recipe XML filename or path.
- `--exitstage`: Stage number at which to stop execution.
- `--dry-run`: Resolves dataset and validates parameters without executing reduction.
- `--loglevel`: Logging verbosity (`debug`, `info`, `warning`, `error`). Default: `info`.

## 3. Real-Time Monitoring

PAC-MAN provides multiple interfaces to track execution and resource utilization.

### Streamlit Dashboard
Launch the [Streamlit](https://streamlit.io/) monitoring dashboard to track stage progress, execution durations, and background task states:

```bash
pixi run dashboard
```
The dashboard runs at `http://localhost:8502` and reads telemetry directly from `runinfo/monitoring.db`.

### Parsl Visualizer
Launch Parsl's built-in DAG [visualizer](https://parsl.readthedocs.io/en/stable/userguide/monitoring.html):

```bash
pixi run visualize
```
The visualizer runs at `http://localhost:8080`, displaying active worker nodes, task status graphs, and runtime resource metrics.

### Incremental Weblog
The pipeline weblog is rendered asynchronously to `html/index.html`. As each background stage finishes, refreshing the browser page reflects newly rendered stage details and QA scoring.

### CASA Log Output
The main console and `casapy.log` continue to record pipeline execution output according to standard [CASAdocs logging](https://casadocs.readthedocs.io/):
```bash
tail -f casapy.log
```

## References

For formal citations, DOIs, and developer manuals, see the [References & Citations](references.md) section.

