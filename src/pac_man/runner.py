"""PAC-MAN Pipeline Runner

This module provides drop-in replacements for standard pipeline interfaces:
- `pac_man_reduce` / `reduce`: drop-in replacement for `pipeline.recipereducer.reduce()`
- `pac_man_executeppr` / `executeppr`: drop-in replacement for `pipeline.infrastructure.executeppr.executeppr()`
- `pac_man_executevlappr` / `executevlappr`: drop-in replacement for `pipeline.infrastructure.executevlappr.executeppr()`

It executes pipeline tasks synchronously but offloads QA calculation and
Weblog rendering to background Parsl tasks using the Checkpoint & Detach pattern.
"""

import datetime
import logging
import os
import platform
import sqlite3

import parsl
from parsl.app.app import python_app

try:
    from atrium.adapters.pipeline import PipelineResultsBridge
    from atrium.core.client import RunContext

    HAS_ATRIUM = True
except ImportError:
    PipelineResultsBridge = None
    RunContext = None
    HAS_ATRIUM = False

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
            start_time_val = start_time.isoformat() if isinstance(start_time, datetime.datetime) else start_time
            end_time_val = end_time.isoformat() if isinstance(end_time, datetime.datetime) else end_time

            cursor = conn.cursor()
            cursor.execute(
                'SELECT rowid FROM stage_execution WHERE run_id = ? AND stage_number = ?',
                (run_id, stage_number),
            )
            row = cursor.fetchone()
            if row:
                conn.execute(
                    'UPDATE stage_execution SET end_time = ?, status = ? WHERE rowid = ?',
                    (end_time_val, status, row[0]),
                )
            else:
                conn.execute(
                    """INSERT INTO stage_execution 
                       (run_id, stage_number, task_name, stage_label, start_time, end_time, status, hostname)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        stage_number,
                        task_name,
                        stage_label,
                        start_time_val,
                        end_time_val,
                        status,
                        platform.node(),
                    ),
                )
        conn.close()
    except Exception as e:
        LOG.debug('Failed to record stage telemetry: %s', e)


def _configure_casalog(
    casalog_file: str | None = None,
    log2term: bool | None = None,
) -> tuple[str | None, bool | None]:
    """Configure casaconfig and synchronize casa_tools.casalog without deleting previous logs.

    Args:
        casalog_file: Target log file path.
        log2term: Whether to print log output to terminal/console.

    Returns:
        Tuple of (effective_logfile_path, effective_log2term_setting).
    """
    effective_log = None
    effective_log2term = None

    if casalog_file or log2term is not None:
        try:
            from casaconfig import config

            if casalog_file:
                config.logfile = os.path.abspath(casalog_file)
            if log2term is not None:
                config.log2term = bool(log2term)
            effective_log = getattr(config, 'logfile', None)
            effective_log2term = getattr(config, 'log2term', None)
        except ImportError:
            pass
        except Exception as e:
            LOG.debug('Error updating casaconfig: %s', e)

    target_log = effective_log or (os.path.abspath(casalog_file) if casalog_file else None)
    target_log2term = effective_log2term if effective_log2term is not None else log2term

    try:
        from pipeline.infrastructure import casa_tools

        if target_log:
            cur_log = casa_tools.casalog.logfile()
            if cur_log != target_log:
                casa_tools.casalog.setlogfile(target_log)
        if target_log2term is not None:
            casa_tools.casalog.showconsole(bool(target_log2term))
    except Exception as e:
        LOG.debug('Error synchronizing casa_tools.casalog: %s', e)

    return target_log, target_log2term


def _unpack_ppr_field(raw: tuple | list | str | None, default: str = 'unknown') -> str:
    """Extract string value from a PPR (key, value) pair or plain string."""
    if isinstance(raw, (tuple, list)):
        return raw[1] if len(raw) > 1 else default
    return raw or default


_WORKER_CACHED_CONTEXT = None


def _execute_qa_and_weblog(
    context_path: str,
    working_dir: str,
    casalog_file: str | None = None,
    log2term: bool | None = None,
    stage_number: int | None = None,
    task_name: str | None = None,
    atrium_storage_dir: str | None = None,
) -> bool:
    """Core QA and Weblog execution logic run inside background Parsl worker subprocesses."""
    global _WORKER_CACHED_CONTEXT
    import os
    import logging

    # Condor tasks run in a scratch directory. Change back to the project working dir IMMEDIATELY
    # before importing pipeline modules, as CASA initializes its logfiles upon import.
    os.chdir(working_dir)

    # Configure casaconfig and synchronize casalog before importing pipeline modules
    _configure_casalog(casalog_file=casalog_file, log2term=log2term)

    from pipeline import cli
    from pipeline.infrastructure import basetask, pipelineqa
    from pipeline.infrastructure.basetask import ResultsProxy
    from pipeline.infrastructure.renderer import htmlrenderer

    LOG = logging.getLogger(__name__)

    # Re-enable QA and Weblog for THIS isolated subprocess
    basetask.DISABLE_QA = False
    basetask.DISABLE_WEBLOG = False

    # 1. Obtain or resume resident context
    if _WORKER_CACHED_CONTEXT is None:
        LOG.info('Background Worker: Loading base context once from %s', context_path)
        _WORKER_CACHED_CONTEXT = cli.h_resume(filename=context_path)
    context = _WORKER_CACHED_CONTEXT

    # 2. Determine target stage and load its result
    if stage_number is not None:
        target_stage = stage_number
        result_basename = f'result-stage{target_stage}.pickle'
        result_path = os.path.join(context.output_dir, context.name, 'saved_state', result_basename)

        if not os.path.exists(result_path):
            LOG.error('Background Worker: Result pickle not found at %s', result_path)
            return False

        # Ensure any intervening stages are loaded into context.results
        existing_stage_nums: set[int] = set()
        for p in context.results:
            bname = getattr(p, '_basename', '')
            if bname.startswith('result-stage') and bname.endswith('.pickle'):
                try:
                    sn = int(bname.replace('result-stage', '').replace('.pickle', ''))
                    existing_stage_nums.add(sn)
                except ValueError:
                    pass

        for prev_stage in range(1, target_stage):
            if prev_stage not in existing_stage_nums:
                prev_bname = f'result-stage{prev_stage}.pickle'
                prev_path = os.path.join(context.output_dir, context.name, 'saved_state', prev_bname)
                if os.path.exists(prev_path):
                    prev_proxy = ResultsProxy(context)
                    prev_proxy._basename = prev_bname
                    try:
                        prev_res = prev_proxy.read()
                        prev_proxy.uuid = prev_res.uuid
                    except Exception as e:
                        LOG.debug('Could not load intervening stage %d: %s', prev_stage, e)
                        continue
                    if hasattr(prev_res, 'merge_with_context'):
                        try:
                            prev_res.merge_with_context(context)
                        except Exception as e:
                            LOG.debug('Could not merge intervening stage %d with context: %s', prev_stage, e)
                    context.results.append(prev_proxy)
                    existing_stage_nums.add(prev_stage)

        # Check if proxy for target stage is already registered
        proxy = None
        for p in context.results:
            bname = getattr(p, '_basename', '')
            if bname == result_basename:
                proxy = p
                break

        if proxy is None:
            proxy = ResultsProxy(context)
            proxy._basename = result_basename
            result = proxy.read()
            proxy.uuid = result.uuid
            if hasattr(result, 'merge_with_context'):
                try:
                    result.merge_with_context(context)
                except Exception as merge_err:
                    LOG.debug('Could not merge result with context: %s', merge_err)
            context.results.append(proxy)
        else:
            result = proxy.read()
    else:
        # Fallback for callers passing only context_path
        if not context.results:
            LOG.warning('No results found in context checkpoint.')
            return False
        proxy = context.results[-1]
        target_stage = getattr(proxy, 'stage_number', 'unknown')
        result = proxy.read() if hasattr(proxy, 'read') else proxy

    # 3. Calculate QA in background
    LOG.info(
        'Background Worker: Calculating QA for stage %s (%s)', target_stage, task_name or result.__class__.__name__
    )
    pipelineqa.qa_registry.do_qa(context, result)
    if hasattr(proxy, 'write'):
        proxy.write(result)

    # 4. Sync QA scores into Atrium if storage dir provided
    if atrium_storage_dir:
        try:
            from atrium.core.client import RunContext
            from atrium.adapters.pipeline import PipelineResultsBridge

            atrium_ctx = RunContext.load(run_id=context.name, storage_dir=atrium_storage_dir)
            qa_delta = PipelineResultsBridge.extract_delta(result, task_name=task_name)
            atrium_ctx.commit_delta(qa_delta)
            LOG.info('Background Worker: Synced background QA metrics into Atrium WAL for stage %s', target_stage)
        except Exception as atrium_err:
            LOG.warning(
                'Background Worker: Could not sync QA to Atrium for stage %s: %s',
                target_stage,
                atrium_err,
                exc_info=True,
            )

    # 5. Render the Weblog incrementally
    LOG.info('Background Worker: Rendering Weblog for stage %s', target_stage)
    htmlrenderer.WebLogGenerator.render(context)

    LOG.info('Background Worker: Finished QA and Weblog for stage %s', target_stage)
    return True


def create_stage_qa_and_weblog_app(stage_name: str, fn=_execute_qa_and_weblog):
    """Factory creating a Parsl PythonApp dynamically named for the specific pipeline stage.

    This ensures that Parsl's MonitoringHub records the actual stage name and number
    (e.g., 'Stage_001_hifv_importdata') in the database rather than a generic function name.
    """

    def stage_task(
        context_path: str,
        working_dir: str,
        casalog_file: str | None = None,
        log2term: bool | None = None,
        stage_number: int | None = None,
        task_name: str | None = None,
        atrium_storage_dir: str | None = None,
        inputs=(),
    ):
        return fn(
            context_path,
            working_dir,
            casalog_file=casalog_file,
            log2term=log2term,
            stage_number=stage_number,
            task_name=task_name,
            atrium_storage_dir=atrium_storage_dir,
        )

    stage_task.__name__ = stage_name
    stage_task.__qualname__ = stage_name
    return python_app(stage_task, executors=['weblog_executor'])


# Backward compatibility alias
async_qa_and_weblog = create_stage_qa_and_weblog_app('async_qa_and_weblog')


def _run_task_sequence(
    task_sequence,
    context,
    backend: str = 'subprocess',
    max_tier0_workers: int | None = None,
    max_weblog_workers: int = 2,
    casalog: str | None = None,
    log2term: bool | None = None,
    startstage: int | None = None,
    exitstage: int | None = None,
    importonly: bool = False,
    import_tasks: tuple[str, ...] = (
        'h_importdata',
        'hifa_importdata',
        'hifv_importdata',
        'hsd_importdata',
        'hsdn_importdata',
    ),
    working_dir: str | None = None,
    *,
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
):
    """Core stage execution engine running synchronous task compute with detached QA and weblog."""
    main_casalog_file, main_log2term = _configure_casalog(casalog_file=casalog, log2term=log2term)
    weblog_casalog_file = main_casalog_file.replace('.log', '-weblog.log') if main_casalog_file else None

    from pipeline import cli
    from pipeline.infrastructure import basetask
    from pipeline.infrastructure.renderer import htmlrenderer

    try:
        initialize_parsl(
            backend=backend,
            max_tier0_workers=max_tier0_workers,
            max_weblog_workers=max_weblog_workers,
            slurm_partition=slurm_partition,
            slurm_account=slurm_account,
            slurm_qos=slurm_qos,
            slurm_constraint=slurm_constraint,
            condor_project=condor_project,
            condor_requirements=condor_requirements,
            condor_accounting_group=condor_accounting_group,
            slurm_tier0_memory_gb=slurm_tier0_memory_gb,
            slurm_weblog_memory_gb=slurm_weblog_memory_gb,
            parsl_loglevel=parsl_loglevel,
        )
    except Exception as e:
        LOG.debug('Parsl already initialized or notice: %s', e)

    dfk = parsl.dfk()
    run_id = dfk.run_id
    monitoring_endpoint = (
        getattr(dfk.config.monitoring, 'logging_endpoint', 'sqlite:///runinfo/monitoring.db')
        or 'sqlite:///runinfo/monitoring.db'
    )
    monitoring_db_path = monitoring_endpoint.replace('sqlite:///', '')

    basetask.DISABLE_QA = True
    basetask.DISABLE_WEBLOG = False
    htmlrenderer.WebLogGenerator.render = lambda context: None
    LOG.info('PAC-MAN: Monkey-patched WebLogGenerator to defer HTML generation to background.')

    atrium_ctx = getattr(context, '_atrium_ctx', None)
    atrium_storage_dir = None
    if HAS_ATRIUM and RunContext is not None and atrium_ctx is None:
        try:
            atrium_storage_dir = os.path.abspath(os.path.join(context.output_dir, context.name, 'atrium_state'))
            atrium_ctx = RunContext.create(
                run_id=context.name,
                storage_dir=atrium_storage_dir,
            )
            context._atrium_ctx = atrium_ctx
            LOG.info(
                'Atrium: Initialized RunContext (run_id=%s, storage_dir=%s)',
                context.name,
                atrium_storage_dir,
            )
        except Exception as e:
            LOG.warning('Atrium: Could not initialize RunContext: %s', e)
    elif atrium_ctx is not None:
        atrium_storage_dir = getattr(atrium_ctx, 'storage_dir', None)

    if startstage is None:
        startstage = 0

    last_background_future = None
    background_futures = []

    try:
        procedure_stage_nr = 0
        for task_item in task_sequence:
            task, task_args = task_item
            task_name = task if isinstance(task, str) else getattr(task, '__name__', str(task))
            procedure_stage_nr += 1
            if procedure_stage_nr < startstage:
                continue

            if task_name in ['hifa_exportdata', 'hifv_exportdata', 'hif_exportdata'] and background_futures:
                LOG.info('PAC-MAN: Waiting for all background Weblog renders to finish before %s...', task_name)
                for f in background_futures:
                    f.result()
                LOG.info('PAC-MAN: Background Weblogs finished. Proceeding with %s.', task_name)

            LOG.info('\n%s\nPAC-MAN Executing Stage %d: %s\n%s', '=' * 60, procedure_stage_nr, task_name, '=' * 60)

            stage_metadata = f'Stage_{procedure_stage_nr:03d}_{task_name}'
            stage_start_time = datetime.datetime.now()
            _log_stage_execution(
                db_path=monitoring_db_path,
                run_id=run_id,
                stage_number=procedure_stage_nr,
                task_name=task_name,
                stage_label=f'{stage_metadata} [compute]',
                start_time=stage_start_time,
                end_time=None,
                status='running',
            )

            try:
                from pipeline.infrastructure import (
                    argmapper,
                    exceptions,
                    task_registry,
                    utils,
                    vdp,
                )

                pipeline_task_class = task_registry.get_pipeline_class_for_task(task_name)
                mapped_args = argmapper.convert_args(pipeline_task_class, task_args)
                inputs = vdp.InputsContainer(pipeline_task_class, context, **mapped_args)

                pipeline_task = pipeline_task_class(inputs)
                result = pipeline_task.execute()

                result.taskname = task_name
                result.accept(context)

                if atrium_ctx is not None and PipelineResultsBridge is not None:
                    try:
                        delta = PipelineResultsBridge.extract_delta(result, task_name=task_name)
                        atrium_ctx.commit_delta(delta)
                        atrium_checkpoint_path = atrium_ctx.checkpoint()
                        LOG.info(
                            'Atrium: Committed Stage %03d delta (%s) -> version %d (checkpoint: %s)',
                            procedure_stage_nr,
                            delta.delta_id[:8],
                            atrium_ctx.version,
                            atrium_checkpoint_path.name,
                        )
                    except Exception as delta_err:
                        LOG.warning(
                            'Atrium: Failed to extract/commit delta for stage %03d (%s): %s',
                            procedure_stage_nr,
                            task_name,
                            delta_err,
                            exc_info=True,
                        )

                _log_stage_execution(
                    db_path=monitoring_db_path,
                    run_id=run_id,
                    stage_number=procedure_stage_nr,
                    task_name=task_name,
                    stage_label=f'{stage_metadata} [compute]',
                    start_time=stage_start_time,
                    end_time=datetime.datetime.now(),
                    status='done',
                )

                tracebacks = utils.get_tracebacks(result)
                if tracebacks:
                    previous_tracebacks_as_string = '\n'.join(tracebacks)
                    raise exceptions.PipelineException(previous_tracebacks_as_string)

            except Exception:
                _log_stage_execution(
                    db_path=monitoring_db_path,
                    run_id=run_id,
                    stage_number=procedure_stage_nr,
                    task_name=task_name,
                    stage_label=f'{stage_metadata} [compute]',
                    start_time=stage_start_time,
                    end_time=datetime.datetime.now(),
                    status='failed',
                )
                LOG.error('Task %s failed. Returning context early.', task_name)
                import traceback

                traceback.print_exc()
                if working_dir:
                    try:
                        from pipeline.infrastructure import utils

                        utils.write_errorexit_file(working_dir, 'errorexit', 'txt')
                    except Exception as e_err:
                        LOG.debug('Failed writing errorexit file: %s', e_err)
                return context

            base_context_name = 'context-base.pickle'
            base_context_path = os.path.abspath(
                os.path.join(context.output_dir, context.name, 'saved_state', base_context_name)
            )
            if not os.path.exists(base_context_path):
                cli.h_save(filename=base_context_path)
                LOG.info('PAC-MAN: Saved immutable base context to %s', base_context_path)
            else:
                LOG.info(
                    'PAC-MAN: Bypassing monolithic context serialization for stage %d (using Atrium state & base context)',
                    result.stage_number,
                )

            LOG.info('PAC-MAN: Launching asynchronous QA & Weblog render for stage %s', result.stage_number)

            stage_metadata = f'Stage_{result.stage_number:03d}_{task_name}'
            stage_app = create_stage_qa_and_weblog_app(stage_metadata)

            bg_future = stage_app(
                base_context_path,
                os.getcwd(),
                casalog_file=weblog_casalog_file,
                log2term=main_log2term,
                stage_number=result.stage_number,
                task_name=task_name,
                atrium_storage_dir=atrium_storage_dir if atrium_ctx else None,
                inputs=[last_background_future] if last_background_future else [],
            )

            background_futures.append(bg_future)
            last_background_future = bg_future

            if importonly and task_name in import_tasks:
                LOG.info('PAC-MAN: Terminating execution after running import task %s (importonly=True)', task_name)
                break

            if exitstage is not None and result.stage_number == exitstage:
                LOG.info('PAC-MAN: Reached exitstage %d. Stopping execution.', exitstage)
                break

    finally:
        LOG.info('Saving final context...')
        cli.h_save()

        LOG.info('PAC-MAN: Waiting for any remaining background rendering tasks to finish...')
        for future in background_futures:
            future.result()
        LOG.info('PAC-MAN: All background tasks complete. Workflow finished!')

        if atrium_ctx is not None and atrium_storage_dir:
            try:
                from atrium.adapters.pipeline import export_timetracker_json

                atrium_ctx = RunContext.load(run_id=context.name, storage_dir=atrium_storage_dir)
                final_chk = atrium_ctx.checkpoint()
                LOG.info(
                    'Atrium: Final context state version %d (%d datasets, %d cal entries, checkpoint: %s)',
                    atrium_ctx.version,
                    len(atrium_ctx.snapshot.datasets),
                    len(atrium_ctx.snapshot.cal_library),
                    final_chk.name,
                )
                summary_df = atrium_ctx.metrics.task_summary()
                if not summary_df.is_empty():
                    LOG.info('Atrium Task Execution Summary:\n%s', summary_df)

                tt_path = os.path.join(context.output_dir, f'{context.name}.timetracker.json')
                export_timetracker_json(atrium_ctx, tt_path)
                LOG.info('Atrium: Exported compatibility timetracker JSON to %s', tt_path)
            except Exception as metrics_err:
                LOG.debug('Atrium metrics summary skipped: %s', metrics_err)

        try:
            parsl.dfk().cleanup()
        except Exception as parsl_err:
            LOG.debug('Parsl cleanup exception: %s', parsl_err)

    return context


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
    max_tier0_workers: int | None = None,
    max_weblog_workers: int = 2,
    casalog: str | None = None,
    log2term: bool | None = None,
    *,
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
):
    """Executes a CASA Pipeline data reduction procedure using the PAC-MAN orchestrator.

    Args:
        vis: Optional input MeasurementSets/ASDMs.
        infiles: Optional input single-dish files.
        procedure: Pipeline recipe XML procedure filename or path.
        context: Optional preexisting pipeline context.
        name: Optional context name.
        loglevel: Logging verbosity ('debug', 'info', 'warning', 'error').
        plotlevel: Plot generation level ('default', 'summary', 'all').
        session: Optional session list.
        exitstage: Optional stage number at which to stop execution.
        startstage: Optional stage number at which to start execution.
        backend: Parsl execution backend ('subprocess', 'htcondor', 'slurm', 'threads').
        max_tier0_workers: Maximum workers to allocate for pipeline Tier0 execution.
        max_weblog_workers: Maximum workers to allocate for background weblog rendering.
        slurm_partition: Slurm partition, inherited from the enclosing Slurm job when omitted.
        slurm_account: Optional Slurm account.
        slurm_qos: Optional Slurm quality of service.
        slurm_constraint: Optional Slurm node constraint.
        condor_project: Optional HTCondor project name.
        condor_requirements: Optional HTCondor ClassAd requirements expression.
        condor_accounting_group: Optional HTCondor accounting group.
        slurm_tier0_memory_gb: Memory requested per Slurm compute worker.
        slurm_weblog_memory_gb: Memory requested per Slurm weblog worker.
        parsl_loglevel: Parsl console verbosity, independent of pipeline logging.
        casalog: Optional custom CASA log file path to aggregate all logging into a single file.
        log2term: Optional boolean to enable/disable printing CASA log output to terminal.
    """
    from pipeline import recipereducer

    if vis is None:
        vis = []
    if infiles is None:
        infiles = []

    if context is None:
        name = name if name else recipereducer._get_context_name(procedure)
        context = recipereducer._create_context(loglevel, plotlevel, name)
        procedure_title = recipereducer._get_procedure_title(procedure)
        context.set_state('ProjectStructure', 'recipe_name', procedure_title)

    recipereducer._register_context(loglevel, plotlevel, context)

    if session is None:
        session = ['default'] * len(vis)

    task_args = recipereducer.TaskArgs(vis, infiles, session)
    task_generator = recipereducer._get_tasks(context, task_args, procedure)

    return _run_task_sequence(
        task_sequence=task_generator,
        context=context,
        backend=backend,
        max_tier0_workers=max_tier0_workers,
        max_weblog_workers=max_weblog_workers,
        slurm_partition=slurm_partition,
        slurm_account=slurm_account,
        slurm_qos=slurm_qos,
        slurm_constraint=slurm_constraint,
        condor_project=condor_project,
        condor_requirements=condor_requirements,
        condor_accounting_group=condor_accounting_group,
        slurm_tier0_memory_gb=slurm_tier0_memory_gb,
        slurm_weblog_memory_gb=slurm_weblog_memory_gb,
        parsl_loglevel=parsl_loglevel,
        casalog=casalog,
        log2term=log2term,
        startstage=startstage,
        exitstage=exitstage,
        working_dir=os.getcwd(),
    )


def pac_man_executeppr(
    pprXmlFile: str,
    importonly: bool = False,
    bp_name: str = 'breakpoint',
    bpaction: str = 'ignore',
    loglevel: str = 'info',
    plotlevel: str = 'default',
    backend: str = 'subprocess',
    max_tier0_workers: int | None = None,
    max_weblog_workers: int = 2,
    casalog: str | None = None,
    log2term: bool | None = None,
    startstage: int | None = None,
    exitstage: int | None = None,
    *,
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
):
    """Executes a Pipeline Processing Request (PPR) using the PAC-MAN orchestrator.

    Drop-in replacement for `pipeline.infrastructure.executeppr.executeppr`.

    Args:
        pprXmlFile: Path to PPR XML file.
        importonly: Whether to stop processing after importing data (default: False).
        bp_name: Name of command considered as a break point.
        bpaction: Action at breakpoint ('ignore', 'break', 'resume').
        loglevel: Logging verbosity ('debug', 'info', 'warning', 'error').
        plotlevel: Plot generation level ('default', 'summary', 'all').
        backend: Parsl execution backend ('subprocess', 'htcondor', 'slurm', 'threads').
        max_tier0_workers: Maximum workers to allocate for pipeline Tier0 execution.
        max_weblog_workers: Maximum workers to allocate for background weblog rendering.
        slurm_partition: Slurm partition, inherited from the enclosing Slurm job when omitted.
        slurm_account: Optional Slurm account.
        slurm_qos: Optional Slurm quality of service.
        slurm_constraint: Optional Slurm node constraint.
        condor_project: Optional HTCondor project name.
        condor_requirements: Optional HTCondor ClassAd requirements expression.
        condor_accounting_group: Optional HTCondor accounting group.
        slurm_tier0_memory_gb: Memory requested per Slurm compute worker.
        slurm_weblog_memory_gb: Memory requested per Slurm weblog worker.
        parsl_loglevel: Parsl console verbosity, independent of pipeline logging.
        casalog: Optional custom CASA log file path.
        log2term: Optional boolean to enable/disable printing CASA log output to terminal.
        startstage: Optional stage number at which to start execution.
        exitstage: Optional stage number at which to stop execution.

    Returns:
        The pipeline Context object.
    """
    from pipeline.infrastructure import executeppr as pipe_eppr
    from pipeline.infrastructure import filenamer, project
    from pipeline import cli

    pipe_eppr.save_existing_context()

    info, structure, relativePath, intentsDict, asdmList, procedureName, commandsList = pipe_eppr._getFirstRequest(
        pprXmlFile
    )
    processing_intents = pipe_eppr._getProcessingIntents(intentsDict, procedureName)

    if 'SCIPIPE_ROOTDIR' in os.environ:
        working_dir = os.path.join(os.path.expandvars('$SCIPIPE_ROOTDIR'), relativePath, 'working')
        raw_dir = os.path.join(os.path.expandvars('$SCIPIPE_ROOTDIR'), relativePath, 'rawdata')
    else:
        working_dir = os.path.abspath(os.path.join('..', 'working'))
        raw_dir = os.path.abspath(os.path.join('..', 'rawdata'))

    bpset = False
    if bp_name != '':
        for command in commandsList:
            if command[0] == bp_name:
                bpset = True
                break

    if bpset and bpaction == 'resume':
        context = cli.h_resume(filename='last')
        context.processing_intents.update(processing_intents)
    else:
        context = cli.h_init(loglevel=loglevel, plotlevel=plotlevel, processing_intents=processing_intents)

    ds_info = dict(info)
    context.project_summary = project.ProjectSummary(
        proposal_code=_unpack_ppr_field(ds_info.get('proposal_code')),
        proposal_title='unknown',
        piname='unknown',
        observatory=_unpack_ppr_field(ds_info.get('observatory')),
        telescope=_unpack_ppr_field(ds_info.get('telescope')),
    )

    ppr_str = pprXmlFile if isinstance(pprXmlFile, str) else 'ppr.xml'
    if structure:
        ds_struct = dict(structure)
        context.project_structure = project.ProjectStructure(
            ous_entity_id=_unpack_ppr_field(ds_struct.get('ous_entity_id'), default=''),
            ous_part_id=_unpack_ppr_field(ds_struct.get('ous_part_id'), default=''),
            ous_title=_unpack_ppr_field(ds_struct.get('ous_title'), default=''),
            ous_type=_unpack_ppr_field(ds_struct.get('ous_type'), default=''),
            ps_entity_id=_unpack_ppr_field(ds_struct.get('ps_entity_id'), default=''),
            ousstatus_entity_id=_unpack_ppr_field(ds_struct.get('ousstatus_entity_id'), default=''),
            ppr_file=ppr_str,
            recipe_name=procedureName,
        )
    else:
        context.project_structure = project.ProjectStructure(
            ppr_file=ppr_str,
            recipe_name=procedureName,
        )

    context.project_performance_parameters = pipe_eppr._getPerformanceParameters(intentsDict)

    sessions_dict = pipe_eppr._getSessions(intentsDict) if len(intentsDict) > 0 else {}
    files = []
    sessions = []
    defsession = 'session_1'
    for asdm in asdmList:
        asdm_name = asdm[1] if isinstance(asdm, (tuple, list)) else asdm
        session = defsession
        for key, value in sessions_dict.items():
            if filenamer.sanitize_for_ms(asdm_name) in value:
                session = key.lower()
                break
        sessions.append(session)
        asdm_path = os.path.join(raw_dir, asdm_name)
        if not os.path.exists(asdm_path) and os.path.exists(asdm_name):
            asdm_path = os.path.abspath(asdm_name)
        files.append(asdm_path)

    import_tasks = ('h_importdata', 'hifa_importdata', 'hifv_importdata', 'hsd_importdata', 'hsdn_importdata')
    restore_tasks = ('h_restoredata', 'hifa_restoredata', 'hifv_restoredata', 'hsd_restoredata')
    restore_tasks_no_session = ('hsdn_restoredata',)

    tasks_to_run = []
    foundbp = False

    for command in commandsList:
        task_name = command[0]
        task_args = dict(command[1])

        if bpset:
            if task_name == bp_name:
                foundbp = True
                if bpaction == 'ignore':
                    continue
                elif bpaction == 'break':
                    break
                elif bpaction == 'resume':
                    continue
            elif not foundbp and bpaction == 'resume':
                continue

        if task_name in import_tasks or task_name in restore_tasks:
            task_args['vis'] = files
            task_args['session'] = sessions
        elif task_name in restore_tasks_no_session:
            task_args['vis'] = files

        tasks_to_run.append((task_name, task_args))

    return _run_task_sequence(
        task_sequence=tasks_to_run,
        context=context,
        backend=backend,
        max_tier0_workers=max_tier0_workers,
        max_weblog_workers=max_weblog_workers,
        slurm_partition=slurm_partition,
        slurm_account=slurm_account,
        slurm_qos=slurm_qos,
        slurm_constraint=slurm_constraint,
        condor_project=condor_project,
        condor_requirements=condor_requirements,
        condor_accounting_group=condor_accounting_group,
        slurm_tier0_memory_gb=slurm_tier0_memory_gb,
        slurm_weblog_memory_gb=slurm_weblog_memory_gb,
        parsl_loglevel=parsl_loglevel,
        casalog=casalog,
        log2term=log2term,
        startstage=startstage,
        exitstage=exitstage,
        importonly=importonly,
        import_tasks=import_tasks,
        working_dir=working_dir,
    )


def pac_man_executevlappr(
    pprXmlFile: str,
    importonly: bool = False,
    loglevel: str = 'info',
    plotlevel: str = 'summary',
    backend: str = 'subprocess',
    max_tier0_workers: int | None = None,
    max_weblog_workers: int = 2,
    casalog: str | None = None,
    log2term: bool | None = None,
    startstage: int | None = None,
    exitstage: int | None = None,
    *,
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
):
    """Executes a VLA Pipeline Processing Request (PPR) using the PAC-MAN orchestrator.

    Drop-in replacement for `pipeline.infrastructure.executevlappr.executeppr`.

    Args:
        pprXmlFile: Path to PPR XML file.
        importonly: Whether to stop processing after importing data (default: False).
        loglevel: Logging verbosity ('debug', 'info', 'warning', 'error').
        plotlevel: Plot generation level ('summary', 'default', 'all').
        backend: Parsl execution backend ('subprocess', 'htcondor', 'slurm', 'threads').
        max_tier0_workers: Maximum workers to allocate for pipeline Tier0 execution.
        max_weblog_workers: Maximum workers to allocate for background weblog rendering.
        slurm_partition: Slurm partition, inherited from the enclosing Slurm job when omitted.
        slurm_account: Optional Slurm account.
        slurm_qos: Optional Slurm quality of service.
        slurm_constraint: Optional Slurm node constraint.
        condor_project: Optional HTCondor project name.
        condor_requirements: Optional HTCondor ClassAd requirements expression.
        condor_accounting_group: Optional HTCondor accounting group.
        slurm_tier0_memory_gb: Memory requested per Slurm compute worker.
        slurm_weblog_memory_gb: Memory requested per Slurm weblog worker.
        parsl_loglevel: Parsl console verbosity, independent of pipeline logging.
        casalog: Optional custom CASA log file path.
        log2term: Optional boolean to enable/disable printing CASA log output to terminal.
        startstage: Optional stage number at which to start execution.
        exitstage: Optional stage number at which to stop execution.

    Returns:
        The pipeline Context object.
    """
    from pipeline.infrastructure import executevlappr as pipe_vlappr
    from pipeline.infrastructure import project
    from pipeline import cli

    pipe_vlappr.save_existing_context()

    info, _, relativePath, intentsDict, asdmList, procedureName, commandsList = pipe_vlappr._getFirstRequest(
        pprXmlFile
    )
    processing_intents = pipe_vlappr._getProcessingIntents(intentsDict, procedureName)

    if 'SCIPIPE_ROOTDIR' in os.environ:
        working_dir = os.path.join(os.path.expandvars('$SCIPIPE_ROOTDIR'), relativePath, 'working')
        raw_dir = os.path.join(os.path.expandvars('$SCIPIPE_ROOTDIR'), relativePath, 'rawdata')
    else:
        working_dir = os.path.abspath(os.path.join('..', 'working'))
        raw_dir = os.path.abspath(os.path.join('..', 'rawdata'))

    context = cli.h_init(loglevel=loglevel, plotlevel=plotlevel, processing_intents=processing_intents)

    ds_info = dict(info)
    context.project_summary = project.ProjectSummary(
        proposal_code=_unpack_ppr_field(ds_info.get('proposal_code')),
        proposal_title='unknown',
        piname='unknown',
        observatory=_unpack_ppr_field(ds_info.get('observatory')),
        telescope=_unpack_ppr_field(ds_info.get('telescope')),
    )

    ppr_str = pprXmlFile if isinstance(pprXmlFile, str) else 'ppr.xml'
    context.project_structure = project.ProjectStructure(
        ppr_file=ppr_str,
        recipe_name=procedureName,
    )
    context.project_performance_parameters = pipe_vlappr._getPerformanceParameters(intentsDict)

    defsession = 'session_1'
    files = []
    sessions = []
    for asdm in asdmList:
        asdm_name = asdm[1] if isinstance(asdm, (tuple, list)) else asdm
        session = defsession
        sessions.append(session)
        asdm_path = os.path.join(raw_dir, asdm_name)
        if not os.path.exists(asdm_path) and os.path.exists(asdm_name):
            asdm_path = os.path.abspath(asdm_name)
        files.append(asdm_path)

    import_tasks = ('h_importdata', 'hifa_importdata', 'hifv_importdata')
    restore_tasks = ('h_restoredata', 'hifv_restoredata')
    breakpoint_task = 'breakpoint'

    tasks_to_run = []
    for command in commandsList:
        task_name = command[0]
        task_args = dict(command[1])

        if task_name == breakpoint_task:
            LOG.info('Found %s, skipping.', breakpoint_task)
            continue

        if task_name in import_tasks or task_name in restore_tasks:
            task_args['vis'] = files
            task_args['session'] = sessions

        spectral_mode = intentsDict.get('SPECTRAL_MODE', False)
        if spectral_mode and task_name == 'hifv_hanning':
            LOG.info('SPECTRAL_MODE=True. Hanning smoothing will not be executed.')
            continue

        tasks_to_run.append((task_name, task_args))

    return _run_task_sequence(
        task_sequence=tasks_to_run,
        context=context,
        backend=backend,
        max_tier0_workers=max_tier0_workers,
        max_weblog_workers=max_weblog_workers,
        slurm_partition=slurm_partition,
        slurm_account=slurm_account,
        slurm_qos=slurm_qos,
        slurm_constraint=slurm_constraint,
        condor_project=condor_project,
        condor_requirements=condor_requirements,
        condor_accounting_group=condor_accounting_group,
        slurm_tier0_memory_gb=slurm_tier0_memory_gb,
        slurm_weblog_memory_gb=slurm_weblog_memory_gb,
        parsl_loglevel=parsl_loglevel,
        casalog=casalog,
        log2term=log2term,
        startstage=startstage,
        exitstage=exitstage,
        importonly=importonly,
        import_tasks=import_tasks,
        working_dir=working_dir,
    )


# Drop-in compatibility aliases
executeppr = pac_man_executeppr
executevlappr = pac_man_executevlappr
reduce = pac_man_reduce
