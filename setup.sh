#!/usr/bin/env sh
set -e

# Repo root = directory containing this script
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

# Change to repo root so relative paths work regardless of invocation dir
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

find_python() {
    for candidate in python3.12 python3 python; do
        if command -v "$candidate" > /dev/null 2>&1; then
            ver=$("$candidate" -c \
              "import sys; print('%d%02d' % sys.version_info[:2])" 2>/dev/null)
            if [ "$ver" -ge 312 ] 2>/dev/null; then
                echo "$candidate"
                return 0
            fi
        fi
    done
    printf 'ERROR: Python 3.12+ is required but was not found.\n' >&2
    exit 1
}

run_cmd() {
    printf '  + %s\n' "$*"
    "$@"
}

PYTHON="$(find_python)"

# ---------------------------------------------------------------------------
# Menu actions
# ---------------------------------------------------------------------------

do_install_and_verify() {
    printf '\n[1] Installing and verifying...\n'
    printf '    Using: %s\n' "$PYTHON"
    run_cmd "$PYTHON" -m pip install -e ".[dev]"
    printf '\n    Running example pipeline to verify install...\n'
    run_cmd "$PYTHON" -m orchestrator.cli run --project examples/phase0/project.json
    printf '    Done.\n'
}

do_test() {
    printf '\n[2] Running tests...\n'
    run_cmd "$PYTHON" -m pytest tests/ -v
}

# ---------------------------------------------------------------------------
# Interactive menu loop
# ---------------------------------------------------------------------------

while true; do
    printf '\nOrchestrator — setup menu\n'
    printf '  1) Install & verify\n'
    printf '  2) Run tests\n'
    printf '  0) Exit\n'
    printf 'Choice: '
    read -r choice
    case "$choice" in
        1) do_install_and_verify ;;
        2) do_test ;;
        0) exit 0 ;;
        *) printf 'Unknown option: %s\n' "$choice" ;;
    esac
done
