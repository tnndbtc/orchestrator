"""Utilities to locate and invoke agent entry points in the same virtual environment."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

# Maps CLI binary name → (importable module, function name)
# Used as a fallback when the binary is not found on PATH.
_MODULE_MAP: dict[str, tuple[str, str]] = {
    "writing-agent": ("writing_agent.cli", "main"),
    "world-engine": ("world_engine.cli", "main"),
    "media": ("scripts.media", "main"),
    "video": ("tools.cli", "main"),
}


def find_agent_bin(name: str) -> Path | None:
    """Return the Path to an agent binary, or None if not found.

    Search order:
    1. Same bin directory as sys.executable — reliable when all agents are
       pip-installed into the same virtual environment (the expected setup).
    2. shutil.which — fallback for system-wide or PATH-activated installs.
    """
    venv_bin = Path(sys.executable).parent / name
    if venv_bin.exists():
        return venv_bin

    found = shutil.which(name)
    return Path(found) if found else None


def call_agent(name: str, args: list[str], **run_kwargs: Any) -> subprocess.CompletedProcess:
    """Invoke an agent, searching the same venv bin first then falling back to sys.executable.

    Args:
        name:       Binary name (e.g. "media", "world-engine").
        args:       Arguments to pass after the binary name.
        run_kwargs: Passed through to subprocess.run.

    Returns:
        CompletedProcess from subprocess.run.

    Raises:
        FileNotFoundError: If the agent cannot be located via any method.
    """
    bin_path = find_agent_bin(name)
    if bin_path:
        return subprocess.run([str(bin_path)] + args, **run_kwargs)

    # Fallback: invoke via sys.executable so the call works even when the
    # binary is not on PATH (e.g. orchestrator is in a different venv than
    # the agents, but all packages are importable via the same Python).
    if name in _MODULE_MAP:
        module, func = _MODULE_MAP[name]
        argv = [name] + args
        code = (
            f"import sys; sys.argv = {argv!r}; "
            f"from {module} import {func}; {func}()"
        )
        return subprocess.run([sys.executable, "-c", code], **run_kwargs)

    raise FileNotFoundError(
        f"Agent '{name}' not found in venv bin directory or PATH, "
        f"and no module fallback is configured. "
        f"Make sure it is installed in the same environment as orchestrator:\n"
        f"  pip install -e /path/to/{name}"
    )
