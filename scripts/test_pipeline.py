#!/usr/bin/env python
"""PAC-MAN Pipeline Reduction Test Script.

Consolidated end-to-end test runner for ALMA and VLA pipeline reduction
workflows with CASA data path discovery and configurable execution backends.

Usage:
    # Run VLA regression test:
    pixi run pipeline-test --telescope vla

    # Run ALMA single-EB calimage test:
    pixi run pipeline-test --telescope alma --backend htcondor

    # Run ALMA 3-EB parallel import test:
    pixi run pipeline-test --telescope alma-3eb
    pixi run pipeline-test 3eb --dry-run

    # Run from an external directory:
    pixi run --manifest-path "$PACMAN_ROOT" pipeline-test --telescope vla

    # With a custom working directory:
    pixi run pipeline-test --telescope alma-3eb --workdir /path/to/workdir
"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from typing import NamedTuple

LOG = logging.getLogger("pac-man-test")


class WorkflowSpec(NamedTuple):
    name: str
    datasets: list[str | list[str]]
    procedure: str
    default_backend: str
    description: str = ""


WORKFLOWS: dict[str, WorkflowSpec] = {
    "vla": WorkflowSpec(
        name="VLA",
        datasets=[
            ["pl-regressiontest/13A-537/13A-537.sb24066356.eb24324502.56514.05971091435"],
        ],
        procedure="procedure_hifv.xml",
        default_backend="subprocess",
        description="VLA standard single-EB regression test (13A-537, procedure_hifv.xml)",
    ),
    "alma": WorkflowSpec(
        name="ALMA",
        datasets=[
            [
                "pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms",
                "pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms",
            ],
        ],
        procedure="procedure_hifa_calimage.xml",
        default_backend="subprocess",
        description="ALMA single-EB calimage test (procedure_hifa_calimage.xml)",
    ),
    "alma-3eb": WorkflowSpec(
        name="ALMA-3EB",
        datasets=[
            "alma_if/2019.1.00847.S/rawdata/uid___A002_Xe1f219_X1457",
            "alma_if/2019.1.00847.S/rawdata/uid___A002_Xe1f219_X9dbf",
            "alma_if/2019.1.00847.S/rawdata/uid___A002_Xe27761_X74f8",
        ],
        procedure="alma_if/recipes/procedure_hifa_calimage_parallel.xml",
        default_backend="subprocess",
        description="ALMA 3-EB parallel import test (2019.1.00847.S / PIPE-2013)",
    ),
}

WORKFLOW_ALIASES: dict[str, str] = {
    "3eb": "alma-3eb",
    "alma_3eb": "alma-3eb",
    "pipe2013": "alma-3eb",
}


def resolve_single_path(path_str: str) -> str:
    """Resolve a relative, repository recipe, or CASA data path via casa_tools.utils.resolve."""
    if os.path.exists(path_str):
        return os.path.abspath(path_str)
    try:
        from pipeline.infrastructure import casa_tools
        resolved = casa_tools.utils.resolve(path_str)
        if os.path.exists(resolved):
            return resolved
    except Exception:
        pass
    # Fallback to local recipes directory in the repository
    repo_recipe = os.path.join(os.path.dirname(os.path.dirname(__file__)), "recipes", os.path.basename(path_str))
    if os.path.exists(repo_recipe):
        return os.path.abspath(repo_recipe)
    return path_str


def resolve_dataset(candidates: Sequence[str]) -> str:
    """Resolve the first existing dataset path from candidate relative paths.

    Args:
        candidates: Candidate relative dataset paths in priority order.

    Returns:
        Absolute filesystem path to the resolved dataset.

    Raises:
        FileNotFoundError: If none of the candidate paths exist on disk.
    """
    resolved_paths: list[str] = []
    for candidate in candidates:
        path = resolve_single_path(candidate)
        resolved_paths.append(path)
        if os.path.exists(path):
            return path

    paths_str = "\n  - ".join(resolved_paths)
    raise FileNotFoundError(
        f"Unable to resolve test dataset. Checked candidates:\n  - {paths_str}\n"
        "Ensure data directories are registered in ~/.casa/config.py or CASA datapath."
    )


def resolve_workflow_datasets(dataset_specs: Sequence[str | Sequence[str]]) -> list[str]:
    """Resolve all dataset paths defined for a workflow."""
    resolved_paths: list[str] = []
    for spec in dataset_specs:
        candidates = [spec] if isinstance(spec, str) else list(spec)
        resolved_paths.append(resolve_dataset(candidates))
    return resolved_paths


def run_pipeline(
    telescope: str = "vla",
    backend: str | None = None,
    vis: Sequence[str] | str | None = None,
    procedure: str | None = None,
    exitstage: int | None = None,
    loglevel: str = "info",
    casalog: str | None = None,
    log2term: bool | None = None,
    dry_run: bool = False,
):
    """Execute a pipeline reduction workflow for the specified telescope.

    Args:
        telescope: Target workflow key ('vla', 'alma', 'alma-3eb', or alias '3eb').
        backend: Parsl execution backend ('subprocess', 'htcondor', 'slurm', 'threads').
        vis: Optional path(s) to custom MeasurementSet/ASDM. If omitted, workflow defaults are resolved.
        procedure: Optional pipeline procedure XML. If omitted, standard recipe for telescope is used.
        exitstage: Optional stage number at which to stop execution.
        loglevel: Pipeline logging level ('debug', 'info', 'warning', 'error').
        casalog: Optional custom path for unified CASA logging across processes.
        log2term: Optional boolean to enable/disable printing CASA log output to terminal.
        dry_run: When True, resolves datasets and verifies environment without running reduction.
    """
    canonical_key = WORKFLOW_ALIASES.get(telescope.lower(), telescope.lower())
    if canonical_key not in WORKFLOWS:
        valid = ", ".join(sorted(list(WORKFLOWS.keys()) + list(WORKFLOW_ALIASES.keys())))
        raise ValueError(f"Unknown workflow '{telescope}'. Available choices: {valid}")

    spec = WORKFLOWS[canonical_key]
    exec_backend = backend or spec.default_backend
    exec_procedure = resolve_single_path(procedure or spec.procedure)

    if vis:
        if isinstance(vis, str):
            vis_list = [vis]
        else:
            vis_list = list(vis)
        vis_paths = [resolve_single_path(v) for v in vis_list]
        for v in vis_paths:
            if not os.path.exists(v):
                raise FileNotFoundError(f"Specified dataset does not exist: {v}")
    else:
        vis_paths = resolve_workflow_datasets(spec.datasets)

    LOG.info("Working directory: %s", os.getcwd())
    LOG.info("Selected workflow: %s (%s)", spec.name, canonical_key)
    LOG.info("Dataset count: %d", len(vis_paths))
    for i, path in enumerate(vis_paths, 1):
        LOG.info("  Dataset [%d]: %s", i, path)
    LOG.info("Procedure: %s", exec_procedure)
    LOG.info("Backend: %s", exec_backend)

    if dry_run:
        LOG.info("Dry-run mode enabled: dataset(s) and recipe resolved successfully, skipping execution.")
        return None

    LOG.info("Starting PAC-MAN reduction...")
    from pac_man.runner import pac_man_reduce
    context = pac_man_reduce(
        vis=vis_paths,
        procedure=exec_procedure,
        backend=exec_backend,
        loglevel=loglevel,
        exitstage=exitstage,
        casalog=casalog,
        log2term=log2term,
    )
    LOG.info("PAC-MAN reduction completed. Final context: %s", context.name)
    return context


def parse_args(args: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for pipeline test execution."""
    available_choices = sorted(list(WORKFLOWS.keys()) + list(WORKFLOW_ALIASES.keys()))

    parser = argparse.ArgumentParser(
        description="PAC-MAN Pipeline Reduction Test Runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "telescope_pos",
        nargs="?",
        choices=available_choices,
        default=None,
        help=f"Workflow to run ({', '.join(available_choices)}).",
    )
    parser.add_argument(
        "-t",
        "--telescope",
        dest="telescope_opt",
        choices=available_choices,
        default=None,
        help=f"Workflow to run ({', '.join(available_choices)}). Overrides positional argument.",
    )
    parser.add_argument(
        "-b",
        "--backend",
        choices=["subprocess", "htcondor", "slurm", "threads"],
        default=None,
        help="Parsl execution backend (defaults to workflow default: 'subprocess').",
    )
    parser.add_argument(
        "--vis",
        nargs="+",
        default=None,
        help="One or more custom MeasurementSet / ASDM paths to reduce.",
    )
    parser.add_argument(
        "-p",
        "--procedure",
        default=None,
        help="Pipeline XML procedure filename or path.",
    )
    parser.add_argument(
        "--exitstage",
        type=int,
        default=None,
        help="Stage number at which to stop execution.",
    )
    parser.add_argument(
        "--loglevel",
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Pipeline logging verbosity.",
    )
    parser.add_argument(
        "-w",
        "--workdir",
        default=None,
        help="Target working directory for execution (defaults to working/ if invoked from repo root).",
    )
    parser.add_argument(
        "-l",
        "--casalog",
        default=None,
        help="Custom CASA log file path. If omitted, uses standard session log file.",
    )
    parser.add_argument(
        "--log2term",
        dest="log2term",
        action="store_true",
        default=None,
        help="Print CASA log output to terminal.",
    )
    parser.add_argument(
        "--no-log2term",
        dest="log2term",
        action="store_false",
        help="Suppress CASA log output from terminal.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and validate dataset paths and settings without executing reduction.",
    )

    parsed = parser.parse_args(args)
    telescope = parsed.telescope_opt or parsed.telescope_pos or "vla"
    parsed.telescope = telescope
    return parsed


def main(args: Sequence[str] | None = None):
    """Entry point for CLI execution."""
    parsed = parse_args(args)

    # Check environment variable overrides
    if not parsed.casalog and "CASA_LOGFILE" in os.environ:
        parsed.casalog = os.environ["CASA_LOGFILE"]
    if parsed.log2term is None and "CASA_LOG2TERM" in os.environ:
        parsed.log2term = os.environ["CASA_LOG2TERM"].lower() in ("1", "true", "yes")

    # Determine execution working directory:
    # 1. Explicit --workdir CLI argument takes highest priority.
    # 2. If invoked via Pixi from the repository root, route to working/ to prevent polluting the repo.
    # 3. Otherwise, preserve the caller's initial invocation directory (INIT_CWD).
    pixi_root = os.environ.get("PIXI_PROJECT_ROOT")
    init_cwd = os.environ.get("INIT_CWD") or os.getcwd()

    if parsed.workdir:
        target_dir = os.path.abspath(parsed.workdir)
    elif pixi_root and (os.path.abspath(init_cwd) == os.path.abspath(pixi_root)):
        target_dir = os.path.join(pixi_root, "working")
    else:
        target_dir = os.path.abspath(init_cwd)

    os.makedirs(target_dir, exist_ok=True)
    os.chdir(target_dir)

    # Inject casaconfig attributes BEFORE any pipeline, casatools, or casatasks imports!
    try:
        from casaconfig import config
        if parsed.casalog:
            config.logfile = os.path.abspath(parsed.casalog)
        else:
            import datetime
            now_str = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d-%H%M%S")
            config.logfile = os.path.abspath(f"casa-{now_str}.log")
        if parsed.log2term is not None:
            config.log2term = bool(parsed.log2term)
    except Exception:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return run_pipeline(
        telescope=parsed.telescope,
        backend=parsed.backend,
        vis=parsed.vis,
        procedure=parsed.procedure,
        exitstage=parsed.exitstage,
        loglevel=parsed.loglevel,
        casalog=parsed.casalog,
        log2term=parsed.log2term,
        dry_run=parsed.dry_run,
    )


if __name__ == "__main__":
    main()
