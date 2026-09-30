# Developer Guide

This document outlines the design, pipeline hook points, concurrency model, and cluster execution requirements for PAC-MAN.

## 1. Concurrency and Process Isolation

[CASA](https://casa.nrao.edu/) C++ extension modules (`casatools`, `casatasks`, `casaplotms`) rely on global mutable state and are **not thread-safe**. Attempting to invoke CASA tools concurrently within the same Python process using threads leads to segmentation faults and memory corruption.

PAC-MAN enforces process isolation through Parsl's [HighThroughputExecutor](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html). Each worker runs in an independent Python process space.

### Worker Scope and Imports
Because workers are spawned in fresh process contexts (often using the `spawn` start method), modules required by remote tasks must be imported inside the task scope:

```python
def _execute_qa_and_weblog(context_path: str, working_dir: str):
    import os
    import pipeline.cli as cli
    import pipeline.infrastructure.pipelineqa as pipelineqa
    from pipeline.infrastructure.renderer import htmlrenderer

    os.chdir(working_dir)
    context = cli.h_resume(filename=context_path)
    # ...
```

## 2. Pipeline Integration Touchpoints

PAC-MAN intercepts synchronous pipeline execution using three primary mechanisms:

1. **Synchronous QA Bypass**: In `pipeline.infrastructure.basetask`, `DISABLE_QA = True` prevents `Result.accept()` from computing QA scores synchronously on the main thread.
2. **WebLog Generation Bypass**: `htmlrenderer.WebLogGenerator.render` is monkey-patched to a no-op on the main orchestrator thread. `DISABLE_WEBLOG = False` is maintained so that tasks continue generating intermediate plot assets without triggering full HTML template compilation.
3. **State Checkpointing**: Stage state is persisted to disk using `cli.h_save(filename=checkpoint_path)` at `saved_state/context-stage-<stage_number>.pickle`.

## 3. Dependency Chaining & Dynamic Task Naming

### Sequential Weblog Rendering
Multiple background stages running concurrently must not write to `html/index.html` simultaneously. PAC-MAN serializes background rendering by chaining Parsl futures:

```python
stage_metadata = f"Stage_{result.stage_number:03d}_{task_name}"
stage_app = create_stage_qa_and_weblog_app(stage_metadata)

bg_future = stage_app(
    checkpoint_path,
    os.getcwd(),
    inputs=[last_background_future] if last_background_future else []
)
background_futures.append(bg_future)
last_background_future = bg_future
```

Passing `inputs=[last_background_future]` registers an explicit DAG edge in Parsl's [DataFlowKernel](https://parsl.readthedocs.io/en/stable/userguide/workflows.html), guaranteeing that Stage $N$ finishes writing to the weblog before Stage $N+1$ begins rendering.

### Dynamic Stage Apps
`create_stage_qa_and_weblog_app()` constructs a dedicated `python_app` instance for each stage. Naming the app dynamically (e.g. `Stage_003_hifv_importdata`) ensures that Parsl's [MonitoringHub](https://parsl.readthedocs.io/en/stable/userguide/monitoring.html) records stage-specific task names rather than a generic function label.

## 4. Cluster Execution (HTCondor & Slurm)

### Memory Specifications
In multi-executor configurations, tasks are divided between compute and rendering pools:
- `tier0_executor`: Configured with 32 GB of memory (`request_memory = 32768`) to accommodate memory-intensive tasks such as `tclean` and `imdev`.
- `weblog_executor`: Configured with 8 GB of memory (`request_memory = 8192`) dedicated to QA calculation and HTML rendering.

### Scratch Directory Handling
[HTCondor](https://htcondor.readthedocs.io/) and [Slurm](https://slurm.schedmd.com/) nodes execute jobs in isolated scratch directories (e.g., `/var/lib/condor/execute/dir_XXXXX`). Workers must:
1. Receive absolute file paths for context checkpoints.
2. Invoke `os.chdir(working_dir)` upon task startup to ensure relative paths to MeasurementSets and calibration tables resolve correctly.

### Headless Display Handling
Plot generation tasks (such as `plotms`) require a valid X11 display. In cluster environments without a running display server, workers use `XvfbLauncher`:
- Wraps the command in `xvfb-run -a -s '-screen 0 1024x768x24'`.
- Unsets `DBUS_SESSION_BUS_ADDRESS` to prevent worker processes from connecting to host session buses when `getenv = true`.
- Sets `QT_X11_NO_MITSHM=1` to bypass shared memory restrictions in containerized or cgroup-constrained nodes.
- Sets `OMP_NUM_THREADS=1` to prevent OpenMP thread deadlocks.

## 5. Testing & Verification

PAC-MAN incorporates unit testing and pipeline reduction verification across local and cluster environments:

### Unit Tests
Run offline unit tests via Pixi:
```bash
pixi run test
```
This executes `pytest tests/` with local mock adapters and temporary directory fixtures, verifying process isolation, executor lifecycle management, and task wrappers.

### Integration & End-to-End Verification
The runner script `scripts/test_pipeline.py` supports end-to-end reduction verification against standard pipeline datasets:

```bash
# Verify dataset resolution and configuration without executing
pixi run python scripts/test_pipeline.py --telescope vla --dry-run
pixi run python scripts/test_pipeline.py --telescope alma --dry-run

# Run reduction through stage N (e.g. stage 2) to test early pipeline execution
pixi run python scripts/test_pipeline.py --telescope vla --exitstage 2
```

Dataset discovery relies on `casa_tools.utils.resolve()`, resolving standard pipeline regression and unit test datasets (such as `pl-regressiontest/...` and `pl-unittest/...`) registered in `~/.casa/config.py` `datapath`.

## References

For full citation keys, BibTeX records, and upstream manual links, refer to the [References & Citations](references.md) guide.


