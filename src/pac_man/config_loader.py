"""PAC-MAN Configuration Loader.

Loads, merges, and validates configuration files (YAML or TOML) with profile inheritance
for driving PAC-MAN pipeline executions.
"""

from __future__ import annotations

import logging
import os
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)

DEFAULT_CONFIG_FILENAMES: list[str] = [
    'pacman.yaml',
    'pacman.yml',
    'pacman.toml',
    'config.yaml',
    'config.yml',
    'config.toml',
]


@dataclass
class ResolvedProfile:
    """Resolved execution profile containing all validated settings."""

    name: str
    mode: str  # 'ppr' or 'recipe'
    telescope: str = 'alma'  # 'alma' or 'vla'
    # PPR settings
    ppr: str | None = None
    importonly: bool = False
    bp_name: str = 'breakpoint'
    bpaction: str = 'ignore'
    # Recipe settings
    procedure: str | None = None
    vis: list[str] = field(default_factory=list)
    session: list[str] | None = None
    infiles: list[str] = field(default_factory=list)
    # Execution & Runtime settings
    backend: str = 'subprocess'
    workdir: str | None = None
    loglevel: str = 'info'
    plotlevel: str = 'default'
    casalog: str | None = None
    log2term: bool | None = None
    exitstage: int | None = None
    startstage: int | None = None
    max_tier0_workers: int | None = None
    max_weblog_workers: int = 2
    omp_num_threads: str = '1'
    slurm_partition: str | None = None
    slurm_account: str | None = None
    slurm_qos: str | None = None
    slurm_constraint: str | None = None
    condor_project: str | None = None
    condor_requirements: str | None = None
    condor_accounting_group: str | None = None
    raw_config: dict[str, Any] = field(default_factory=dict)
    slurm_tier0_memory_gb: int = 32
    slurm_weblog_memory_gb: int = 8


USER_CONFIG_CANDIDATES: list[Path] = [
    Path('~/.pacman/config.toml').expanduser(),
    Path('~/.pacman/config.yaml').expanduser(),
    Path('~/.pacman/config.yml').expanduser(),
    Path('~/.pacman.toml').expanduser(),
    Path('~/.config/pacman/config.toml').expanduser(),
    Path('~/.config/pacman/config.yaml').expanduser(),
]


def find_default_config(start_dir: str | Path | None = None) -> Path | None:
    """Search for a default configuration file in current directory, project root, and user home.

    Discovery order:
    1. Explicit environment variable: ``$PACMAN_CONFIG``
    2. Working directory: ``./pacman.toml``, ``./pacman.yaml``, ``./config.toml``, etc.
    3. Project / Pixi root (if inside a repository): ``$PIXI_PROJECT_ROOT/scripts/config.toml``, etc.
    4. User home directory: ``~/.pacman/config.toml``, ``~/.config/pacman/config.toml``, ``~/.pacman.toml``
    """
    # 1. Environment variable override
    env_config = os.environ.get('PACMAN_CONFIG')
    if env_config:
        env_path = Path(env_config).expanduser().resolve()
        if env_path.is_file():
            return env_path

    # 2. Local directory candidates
    base_dir = Path(start_dir or Path.cwd()).resolve()
    for candidate_name in DEFAULT_CONFIG_FILENAMES:
        candidate_path = base_dir / candidate_name
        if candidate_path.is_file():
            return candidate_path

    # 3. Project / Pixi root candidates (if running inside repo)
    pixi_root = os.environ.get('PIXI_PROJECT_ROOT')
    if pixi_root:
        proj_dir = Path(pixi_root).resolve()
        if proj_dir != base_dir:
            for candidate_name in DEFAULT_CONFIG_FILENAMES:
                cand = proj_dir / candidate_name
                if cand.is_file():
                    return cand
            scripts_cand = proj_dir / 'scripts' / 'config.toml'
            if scripts_cand.is_file():
                return scripts_cand

    # 4. User home directory candidates (~/.pacman/config.toml, ~/.config/pacman/config.toml)
    for user_cand in USER_CONFIG_CANDIDATES:
        if user_cand.is_file():
            return user_cand.resolve()

    return None


def load_raw_config(config_path: str | Path) -> dict[str, Any]:
    """Load configuration from a YAML or TOML file.

    Args:
        config_path: Path to YAML or TOML configuration file.

    Returns:
        Dictionary representation of configuration.

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
        ValueError: If file format is unsupported or parsing fails.
    """
    path = Path(config_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f'Configuration file not found: {path}')

    suffix = path.suffix.lower()
    content = path.read_text(encoding='utf-8')

    if suffix in ('.yaml', '.yml'):
        try:
            import yaml

            parsed = yaml.safe_load(content)
            return parsed if isinstance(parsed, dict) else {}
        except Exception as err:
            raise ValueError(f"Failed to parse YAML configuration '{path}': {err}") from err

    elif suffix == '.toml':
        try:
            import tomllib  # Python 3.11+ standard library
        except ImportError:
            try:
                import tomli as tomllib  # Fallback for Python < 3.11
            except ImportError:
                import toml as tomllib

        try:
            return tomllib.loads(content)
        except Exception as err:
            raise ValueError(f"Failed to parse TOML configuration '{path}': {err}") from err

    else:
        raise ValueError(
            f"Unsupported configuration file extension '{suffix}' for {path}. Expected .yaml, .yml, or .toml"
        )


def _deep_merge_dict(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override dictionary into base dictionary."""
    result = deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def resolve_profile_data(
    profiles_dict: dict[str, Any],
    profile_name: str,
    visited: list[str] | None = None,
) -> dict[str, Any]:
    """Recursively resolve profile inheritance and merge settings.

    Args:
        profiles_dict: Dictionary mapping profile names to settings.
        profile_name: Name of the profile to resolve.
        visited: Internal stack of visited profile names for cycle detection.

    Returns:
        Merged profile settings dictionary.

    Raises:
        ValueError: If profile does not exist or cyclic inheritance is detected.
    """
    if visited is None:
        visited = []

    if profile_name in visited:
        cycle_chain = ' -> '.join(visited + [profile_name])
        raise ValueError(f'Cyclic profile inheritance detected: {cycle_chain}')

    if profile_name not in profiles_dict:
        available = ', '.join(sorted(profiles_dict.keys()))
        raise ValueError(f"Profile '{profile_name}' not found. Available profiles: {available}")

    current_data = deepcopy(profiles_dict[profile_name])
    parent_name = current_data.pop('inherits', None)

    if parent_name:
        parent_merged = resolve_profile_data(profiles_dict, parent_name, visited + [profile_name])
        merged = _deep_merge_dict(parent_merged, current_data)
    else:
        merged = current_data

    return merged


def resolve_path_relative_to_config(
    file_path: str | None,
    config_dir: Path,
) -> str | None:
    """Resolve a relative path against the configuration file's parent directory if not existing in CWD."""
    if not file_path:
        return file_path
    if os.path.isabs(file_path) or os.path.exists(file_path):
        return os.path.abspath(file_path)
    # Check relative to config directory
    candidate = (config_dir / file_path).resolve()
    if candidate.exists():
        return str(candidate)
    return file_path


def load_profile(
    config_path: str | Path,
    profile_name: str | None = None,
) -> ResolvedProfile:
    """Load configuration and resolve a target profile.

    Args:
        config_path: Path to YAML or TOML configuration file.
        profile_name: Name of profile to resolve. If None, uses 'default_profile' or 'default'.

    Returns:
        ResolvedProfile instance with validated settings.
    """
    cfg_file = Path(config_path).expanduser().resolve()
    config_dir = cfg_file.parent
    raw_cfg = load_raw_config(cfg_file)

    profiles = raw_cfg.get('profiles', {})
    if not isinstance(profiles, dict) or not profiles:
        # If top level keys look like a single profile definition
        if 'mode' in raw_cfg or 'ppr' in raw_cfg or 'procedure' in raw_cfg:
            profiles = {'default': raw_cfg}
        else:
            raise ValueError(f"Configuration file '{cfg_file}' has no 'profiles' section defined.")

    target_profile = profile_name or raw_cfg.get('default_profile') or 'default'
    merged = resolve_profile_data(profiles, target_profile)

    # Determine execution mode: 'ppr' or 'recipe'
    mode = merged.get('mode')
    if not mode:
        if 'ppr' in merged or 'pprXmlFile' in merged:
            mode = 'ppr'
        elif 'procedure' in merged or 'vis' in merged:
            mode = 'recipe'
        else:
            mode = 'ppr'

    mode = str(mode).lower()
    if mode not in ('ppr', 'recipe'):
        raise ValueError(f"Invalid execution mode '{mode}' in profile '{target_profile}'. Must be 'ppr' or 'recipe'.")

    # Telescope
    telescope = str(merged.get('telescope', 'alma')).lower()

    # PPR resolution
    ppr_raw = merged.get('ppr') or merged.get('pprXmlFile') or merged.get('ppr_file')
    ppr_resolved = resolve_path_relative_to_config(ppr_raw, config_dir) if ppr_raw else None

    # Recipe procedure resolution
    proc_raw = merged.get('procedure')
    proc_resolved = resolve_path_relative_to_config(proc_raw, config_dir) if proc_raw else None

    # Datasets (vis)
    vis_raw = merged.get('vis', [])
    if isinstance(vis_raw, str):
        vis_raw = [vis_raw]
    vis_resolved = [resolve_path_relative_to_config(v, config_dir) or v for v in vis_raw]

    # Infiles (single dish)
    infiles_raw = merged.get('infiles', [])
    if isinstance(infiles_raw, str):
        infiles_raw = [infiles_raw]
    infiles_resolved = [resolve_path_relative_to_config(f, config_dir) or f for f in infiles_raw]

    # Working directory
    workdir_raw = merged.get('workdir')
    workdir_resolved = resolve_path_relative_to_config(workdir_raw, config_dir) if workdir_raw else None

    # CASA log
    casalog_raw = merged.get('casalog')
    casalog_resolved = resolve_path_relative_to_config(casalog_raw, config_dir) if casalog_raw else None

    return ResolvedProfile(
        name=target_profile,
        mode=mode,
        telescope=telescope,
        ppr=ppr_resolved,
        importonly=bool(merged.get('importonly', False)),
        bp_name=str(merged.get('bp_name', 'breakpoint')),
        bpaction=str(merged.get('bpaction', 'ignore')),
        procedure=proc_resolved,
        vis=vis_resolved,
        session=merged.get('session'),
        infiles=infiles_resolved,
        backend=str(merged.get('backend', 'subprocess')),
        workdir=workdir_resolved,
        loglevel=str(merged.get('loglevel', 'info')),
        plotlevel=str(merged.get('plotlevel', 'default')),
        casalog=casalog_resolved,
        log2term=merged.get('log2term'),
        exitstage=merged.get('exitstage'),
        startstage=merged.get('startstage'),
        max_tier0_workers=merged.get('max_tier0_workers'),
        max_weblog_workers=int(merged.get('max_weblog_workers', 2)),
        omp_num_threads=str(merged.get('omp_num_threads', '1')),
        slurm_partition=merged.get('slurm_partition'),
        slurm_account=merged.get('slurm_account'),
        slurm_qos=merged.get('slurm_qos'),
        slurm_constraint=merged.get('slurm_constraint'),
        condor_project=merged.get('condor_project'),
        condor_requirements=merged.get('condor_requirements'),
        condor_accounting_group=merged.get('condor_accounting_group'),
        raw_config=merged,
        slurm_tier0_memory_gb=int(merged.get('slurm_tier0_memory_gb', 32)),
        slurm_weblog_memory_gb=int(merged.get('slurm_weblog_memory_gb', 8)),
    )
