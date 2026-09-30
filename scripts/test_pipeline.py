#!/usr/bin/env python
"""PAC-MAN Pipeline Reduction Test Script.

Consolidated end-to-end test runner for ALMA and VLA pipeline reduction
workflows with CASA data path discovery and configurable execution backends.

Usage (from an isolated working directory):
    pixi run --manifest-path "${PACMAN_ROOT:-../..}" pipeline-test --telescope vla
    pixi run --manifest-path "${PACMAN_ROOT:-../..}" pipeline-test --telescope alma
    pixi run --manifest-path "${PACMAN_ROOT:-../..}" pipeline-test --telescope alma --backend htcondor
    pixi run --manifest-path "${PACMAN_ROOT:-../..}" pipeline-test --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import NamedTuple, Sequence

from pac_man.runner import pac_man_reduce
from pipeline.infrastructure import casa_tools

LOG = logging.getLogger("pac-man-test")


class WorkflowSpec(NamedTuple):
    telescope: str
    dataset_relpaths: list[str]
    procedure: str
    default_backend: str


WORKFLOWS: dict[str, WorkflowSpec] = {
    "vla": WorkflowSpec(
        telescope="vla",
        dataset_relpaths=[
            "pl-regressiontest/13A-537/13A-537.sb24066356.eb24324502.56514.05971091435",
        ],
        procedure="procedure_hifv.xml",
        default_backend="subprocess",
    ),
    "alma": WorkflowSpec(
        telescope="alma",
        dataset_relpaths=[
            "pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms",
            "pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms",
        ],
        procedure="procedure_hifa_calimage.xml",
        default_backend="subprocess",
    ),
}


def resolve_dataset(relpaths: list[str]) -> str:
    """Resolve the first existing dataset path using casa_tools.utils.resolve.

    Args:
        relpaths: Relative dataset paths within registered CASA data directories.

    Returns:
        Absolute filesystem path to the resolved dataset.

    Raises:
        FileNotFoundError: If none of the candidate paths exist on disk.
    """
    resolved_paths: list[str] = []
    for relpath in relpaths:
        path = casa_tools.utils.resolve(relpath)
        resolved_paths.append(path)
        if os.path.exists(path):
            return path

    paths_str = "\n  - ".join(resolved_paths)
    raise FileNotFoundError(
        f"Unable to resolve test dataset. Checked candidates:\n  - {paths_str}\n"
        "Ensure data directories are registered in ~/.casa/config.py or CASA datapath."
    )


def run_pipeline(
    telescope: str = "vla",
    backend: str | None = None,
    vis: str | None = None,
    procedure: str | None = None,
    exitstage: int | None = None,
    loglevel: str = "info",
    dry_run: bool = False,
):
    """Execute a pipeline reduction workflow for the specified telescope.

    Args:
        telescope: Target telescope ('vla' or 'alma').
        backend: Parsl execution backend ('subprocess', 'htcondor', 'slurm', 'threads').
        vis: Optional path to custom MeasurementSet. If omitted, standard test dataset is resolved.
        procedure: Optional pipeline procedure XML. If omitted, standard recipe for telescope is used.
        exitstage: Optional stage number at which to stop execution.
        loglevel: Pipeline logging level ('debug', 'info', 'warning', 'error').
        dry_run: When True, resolves datasets and verifies environment without running reduction.
    """
    spec = WORKFLOWS[telescope]
    exec_backend = backend or spec.default_backend
    exec_procedure = procedure or spec.procedure

    if vis:
        vis_path = os.path.abspath(vis) if os.path.exists(vis) else casa_tools.utils.resolve(vis)
        if not os.path.exists(vis_path):
            raise FileNotFoundError(f"Specified dataset does not exist: {vis_path}")
    else:
        vis_path = resolve_dataset(spec.dataset_relpaths)

    LOG.info("Selected workflow: %s", telescope.upper())
    LOG.info("Dataset path: %s", vis_path)
    LOG.info("Procedure: %s", exec_procedure)
    LOG.info("Backend: %s", exec_backend)

    if dry_run:
        LOG.info("Dry-run mode enabled: dataset resolved successfully, skipping execution.")
        return None

    LOG.info("Starting PAC-MAN reduction...")
    context = pac_man_reduce(
        vis=[vis_path],
        procedure=exec_procedure,
        backend=exec_backend,
        loglevel=loglevel,
        exitstage=exitstage,
    )
    LOG.info("PAC-MAN reduction completed. Final context: %s", context.name)
    return context


def parse_args(args: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for pipeline test execution."""
    parser = argparse.ArgumentParser(
        description="PAC-MAN Pipeline Reduction Test Runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "telescope_pos",
        nargs="?",
        choices=["vla", "alma"],
        default=None,
        help="Telescope workflow to run ('vla' or 'alma').",
    )
    parser.add_argument(
        "-t",
        "--telescope",
        dest="telescope_opt",
        choices=["vla", "alma"],
        default=None,
        help="Telescope workflow to run ('vla' or 'alma'). Overrides positional argument.",
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
        default=None,
        help="Path or relative CASA path to custom MeasurementSet to reduce.",
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parsed = parse_args(args)
    return run_pipeline(
        telescope=parsed.telescope,
        backend=parsed.backend,
        vis=parsed.vis,
        procedure=parsed.procedure,
        exitstage=parsed.exitstage,
        loglevel=parsed.loglevel,
        dry_run=parsed.dry_run,
    )


if __name__ == "__main__":
    main()
