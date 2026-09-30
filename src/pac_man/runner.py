"""PAC-MAN Pipeline Runner

This script provides a drop-in replacement for the standard pipeline's `recipereducer.reduce()` 
or `executeppr`. It executes pipeline tasks synchronously but offloads QA calculation and 
Weblog rendering to background Parsl tasks using the Checkpoint & Detach pattern.
"""
import os
import logging
import sqlite3
import platform
import datetime
import parsl
from parsl.app.app import python_app

# Import pac-man config to initialize parsl
from pac_man.config import initialize_parsl

# Pipeline imports moved into pac_man_reduce to avoid early CASA initialization in remote workers
LOG = logging.getLogger(__name__)


def _log_stage_execution(
    db_path: str,
    run_id: str,
    stage_number: int,
    task_name: str,
    stage_label: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime | None,
    status: str,
) -> None:
    """Records synchronous main-process pipeline compute stage events into monitoring database."""
    try:
        abs_db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(abs_db_path), exist_ok=True)
        conn = sqlite3.connect(abs_db_path, timeout=10.0)
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stage_execution (
                    run_id TEXT,
                    stage_number INTEGER,
                    task_name TEXT,
                    stage_label TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status TEXT,
                    hostname TEXT
                )
            """)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT rowid FROM stage_execution WHERE run_id = ? AND stage_number = ?",
                (run_id, stage_number),
            )
            row = cursor.fetchone()
            if row:
                conn.execute(
                    "UPDATE stage_execution SET end_time = ?, status = ? WHERE rowid = ?",
                    (end_time, status, row[0]),
                )
            else:
                conn.execute(
                    """INSERT INTO stage_execution 
                       (run_id, stage_number, task_name, stage_label, start_time, end_time, status, hostname)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (run_id, stage_number, task_name, stage_label, start_time, end_time, status, platform.node()),
                )
        conn.close()
    except Exception as e:
        LOG.debug("Failed to record stage telemetry: %s", e)


def _execute_qa_and_weblog(context_path: str, working_dir: str) -> bool:
    """
    Core QA and Weblog execution logic run inside background Parsl worker subprocesses.
    """
    import os
    # Condor tasks run in a scratch directory. Change back to the project working dir IMMEDIATELY
    # before importing pipeline modules, as CASA initializes its logfiles upon import.
    os.chdir(working_dir)

    import logging
    import pipeline.cli as cli
    import pipeline.infrastructure.pipelineqa as pipelineqa
    from pipeline.infrastructure.renderer import htmlrenderer
    import pipeline.infrastructure.basetask as basetask
    
    LOG = logging.getLogger(__name__)
    LOG.info(f"Background Task: Loading checkpoint from {context_path}")

    # Re-enable QA and Weblog for THIS isolated subprocess
    basetask.DISABLE_QA = False
    basetask.DISABLE_WEBLOG = False

    # Resume the context from the checkpoint
    context = cli.h_resume(filename=context_path)
    
    # Identify the most recently added result
    if not context.results:
        LOG.warning("No results found in context checkpoint.")
        return False
        
    latest_result_proxy = context.results[-1]
    # For logging purposes, extract stage number
    latest_stage_number = latest_result_proxy.stage_number if hasattr(latest_result_proxy, 'stage_number') else 'unknown'
    
    # 1. Unpickle result, calculate QA in background, and persist back to disk
    result = latest_result_proxy.read() if hasattr(latest_result_proxy, 'read') else latest_result_proxy
    LOG.info(f"Background Task: Calculating QA for stage {latest_stage_number}")
    pipelineqa.qa_registry.do_qa(context, result)
    if hasattr(latest_result_proxy, 'write'):
        latest_result_proxy.write(result)
    
    # 2. Render the Weblog incrementally
    LOG.info(f"Background Task: Rendering Weblog for stage {latest_stage_number}")
    htmlrenderer.WebLogGenerator.render(context)
    
    LOG.info(f"Background Task: Finished QA and Weblog for stage {latest_stage_number}")
    return True


def create_stage_qa_and_weblog_app(stage_name: str, fn=_execute_qa_and_weblog):
    """
    Factory creating a Parsl PythonApp dynamically named for the specific pipeline stage.
    This ensures that Parsl's MonitoringHub records the actual stage name and number
    (e.g., 'Stage_001_hifv_importdata') in the database rather than a generic function name.
    """
    def stage_task(context_path: str, working_dir: str, inputs=()):
        return fn(context_path, working_dir)

    stage_task.__name__ = stage_name
    stage_task.__qualname__ = stage_name
    return python_app(stage_task, executors=['weblog_executor'])


# Backward compatibility alias
async_qa_and_weblog = create_stage_qa_and_weblog_app("async_qa_and_weblog")


def pac_man_reduce(
        vis: list[str] | None = None,
        infiles: list[str] | None = None,
        procedure: str = 'procedure_hifv.xml',
        context=None,
        name: str | None = None,
        loglevel: str = 'info',
        plotlevel: str = 'default',
        session: list[str] | None = None,
        exitstage: int | None = None,
        startstage: int | None = None,
        backend: str = 'subprocess',
        max_tier0_workers: int = 50,
        max_weblog_workers: int = 2
):
    """
    Executes a CASA Pipeline data reduction procedure using the PAC-MAN orchestrator.
    
    Args:
        max_tier0_workers: Maximum number of HTC Condor nodes to allocate for pipeline Tier0 execution.
        max_weblog_workers: Maximum number of Condor nodes to allocate for background weblog rendering.
    """
    # 1. Pipeline imports (Localizing them here prevents Condor workers from triggering
    # CASA initialization when unpickling the runner module)
    import pipeline.infrastructure.basetask as basetask
    import pipeline.cli as cli
    from pipeline import recipereducer

    # 2. Initialize Parsl
    initialize_parsl(backend=backend)
    dfk = parsl.dfk()
    run_id = dfk.run_id
    monitoring_endpoint = getattr(dfk.config.monitoring, 'logging_endpoint', 'sqlite:///runinfo/monitoring.db') or 'sqlite:///runinfo/monitoring.db'
    monitoring_db_path = monitoring_endpoint.replace('sqlite:///', '')
    
    # 2. Neuter the synchronous pipeline slowdowns globally for the main thread!
    basetask.DISABLE_QA = True
    basetask.DISABLE_WEBLOG = False
    # Instead of disabling plotting entirely, we monkey-patch the HTML renderer 
    # to be a no-op in the main process. This allows tasks to generate plots, 
    # but defers the slow HTML templating to the background worker.
    from pipeline.infrastructure.renderer import htmlrenderer
    htmlrenderer.WebLogGenerator.render = lambda context: None
    LOG.info("PAC-MAN: Monkey-patched WebLogGenerator to defer HTML generation to background.")

    if vis is None: vis = []
    if infiles is None: infiles = []

    if context is None:
        # Fallback context naming
        name = name if name else recipereducer._get_context_name(procedure)
        # Using recipereducer's hidden create_context
        context = recipereducer._create_context(loglevel, plotlevel, name)
        procedure_title = recipereducer._get_procedure_title(procedure)
        context.set_state('ProjectStructure', 'recipe_name', procedure_title)

    recipereducer._register_context(loglevel, plotlevel, context)

    if session is None:
        session = ['default'] * len(vis)

    if startstage is None:
        startstage = 0

    task_args = recipereducer.TaskArgs(vis, infiles, session)
    task_generator = recipereducer._get_tasks(context, task_args, procedure)
    
    # PAC-MAN Specific Tracking
    last_background_future = None
    background_futures = []
    
    try:
        procedure_stage_nr = 0
        while True:
            task, task_args = next(task_generator)
            procedure_stage_nr += 1
            if procedure_stage_nr < startstage:
                continue

            # 3. Synchronously Execute Condor Task Crunching
            task_name = task.__name__
            
            # ExportData must package the Weblog. We MUST wait for all background weblog renders
            # to finish before we allow ExportData to run, otherwise it will package an incomplete weblog.
            if task_name in ['hifa_exportdata', 'hifv_exportdata', 'hif_exportdata']:
                if background_futures:
                    LOG.info(f"PAC-MAN: Waiting for all background Weblog renders to finish before {task_name}...")
                    for f in background_futures:
                        f.result()
                    LOG.info(f"PAC-MAN: Background Weblogs finished. Proceeding with {task_name}.")

            LOG.info(f"\n{'='*60}\nPAC-MAN Executing Stage {procedure_stage_nr}: {task_name}\n{'='*60}")
            
            stage_metadata = f"Stage_{procedure_stage_nr:03d}_{task_name}"
            stage_start_time = datetime.datetime.now()
            _log_stage_execution(
                db_path=monitoring_db_path,
                run_id=run_id,
                stage_number=procedure_stage_nr,
                task_name=task_name,
                stage_label=f"{stage_metadata} [compute]",
                start_time=stage_start_time,
                end_time=None,
                status="running",
            )

            # Explicitly bypass the CLI wrapper and global context
            try:
                from pipeline.infrastructure import task_registry, argmapper, vdp, utils
                from pipeline.infrastructure import exceptions

                pipeline_task_class = task_registry.get_pipeline_class_for_task(task_name)
                mapped_args = argmapper.convert_args(pipeline_task_class, task_args)
                inputs = vdp.InputsContainer(pipeline_task_class, context, **mapped_args)

                pipeline_task = pipeline_task_class(inputs)
                result = pipeline_task.execute()

                result.taskname = task_name
                result.accept(context)

                _log_stage_execution(
                    db_path=monitoring_db_path,
                    run_id=run_id,
                    stage_number=procedure_stage_nr,
                    task_name=task_name,
                    stage_label=f"{stage_metadata} [compute]",
                    start_time=stage_start_time,
                    end_time=datetime.datetime.now(),
                    status="done",
                )

                tracebacks = utils.get_tracebacks(result)
                if tracebacks:
                    previous_tracebacks_as_string = '\n'.join(tracebacks)
                    raise exceptions.PipelineException(previous_tracebacks_as_string)

            except Exception as e:
                _log_stage_execution(
                    db_path=monitoring_db_path,
                    run_id=run_id,
                    stage_number=procedure_stage_nr,
                    task_name=task_name,
                    stage_label=f"{stage_metadata} [compute]",
                    start_time=stage_start_time,
                    end_time=datetime.datetime.now(),
                    status="failed",
                )
                LOG.error(f"Task {task_name} failed. Returning context early.")
                import traceback
                traceback.print_exc()
                return context

            # 4. Save Checkpoint Context
            checkpoint_name = f'context-stage-{result.stage_number}.pickle'
            checkpoint_path = os.path.abspath(os.path.join(context.output_dir, context.name, 'saved_state', checkpoint_name))
            
            # Use cli.h_save but move it or rename it? Actually, cli.h_save() writes to standard path.
            # We can use the explicit h_save filename parameter.
            cli.h_save(filename=checkpoint_path)
            LOG.info(f"PAC-MAN: Checkpoint saved to {checkpoint_path}")

            # 5. Launch Background QA and Weblog (Chaining the futures to prevent Race Conditions)
            LOG.info(f"PAC-MAN: Launching asynchronous QA & Weblog render for stage {result.stage_number}")
            
            # Pass the previous future into the special `inputs` kwarg to build a visualizable DAG edge
            stage_metadata = f"Stage_{result.stage_number:03d}_{task_name}"
            stage_app = create_stage_qa_and_weblog_app(stage_metadata)
            
            bg_future = stage_app(
                checkpoint_path, 
                os.getcwd(),
                inputs=[last_background_future] if last_background_future else []
            )
            
            background_futures.append(bg_future)
            last_background_future = bg_future

            if result.stage_number == exitstage:
                break

    except StopIteration:
        pass
    finally:
        LOG.info('Saving final context...')
        cli.h_save()
        
        # Wait for all background tasks to cleanly finish before fully exiting
        LOG.info("PAC-MAN: Waiting for any remaining background rendering tasks to finish...")
        for future in background_futures:
            future.result()
        LOG.info("PAC-MAN: All background tasks complete. Workflow finished!")
    
    # Gracefully shutdown Parsl engine to prevent dirty exit warnings
    parsl.dfk().cleanup()
    
    return context
