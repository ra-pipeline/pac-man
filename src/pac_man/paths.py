"""Project directory conventions shared by PAC-MAN entry points."""
from __future__ import annotations

import os
from pathlib import Path


def project_directories(workdir: str | Path) -> tuple[Path, Path, Path]:
    """Return logical project, execution and input paths without resolving symlinks.

    A legacy path ending in ``working`` selects that directory's parent as the
    project root. Classify the logical name before resolving its physical target.
    """
    expanded = os.path.expandvars(os.path.expanduser(os.fspath(workdir)))
    if '$' in expanded:
        raise ValueError('workdir contains an unset environment variable')
    selected = Path(os.path.abspath(expanded))
    root = selected.parent if selected.name == 'working' else selected
    return root, root / 'working', root / 'rawdata'
