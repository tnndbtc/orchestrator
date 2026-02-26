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
ORCHESTRATOR="$(dirname "$(command -v "$PYTHON")")/orchestrator"

# ---------------------------------------------------------------------------
# Menu actions
# ---------------------------------------------------------------------------

do_install_and_verify() {
    printf '\n[1] Installing and verifying...\n'

    # If not already inside a virtual environment, create or reuse .venv.
    # This avoids the "externally-managed-environment" error on Debian/Ubuntu.
    VENV_DIR="$REPO_ROOT/.venv"
    if [ -z "${VIRTUAL_ENV:-}" ]; then
        if [ ! -x "$VENV_DIR/bin/python" ]; then
            printf '    Creating virtual environment at .venv ...\n'
            run_cmd "$PYTHON" -m venv "$VENV_DIR"
        fi
        INSTALL_PYTHON="$VENV_DIR/bin/python"
        INSTALL_ORCHESTRATOR="$VENV_DIR/bin/orchestrator"
    else
        INSTALL_PYTHON="$PYTHON"
        INSTALL_ORCHESTRATOR="$ORCHESTRATOR"
    fi

    printf '    Using: %s\n' "$INSTALL_PYTHON"
    run_cmd "$INSTALL_PYTHON" -m pip install -e ".[dev]"
    printf '\n    Running example pipeline to verify install...\n'
    run_cmd "$INSTALL_ORCHESTRATOR" run --project examples/phase0/project.json --stub
    printf '    Done.\n'
}

do_show_usage() {
    printf '\n'
    printf '══════════════════════════════════════════════════════════════════\n'
    printf '  Orchestrator — Command Reference\n'
    printf '══════════════════════════════════════════════════════════════════\n'

    printf '\n── GENERATE ARTIFACTS (main pipeline) ───────────────────────────\n\n'

    printf '  orchestrator write\n'
    printf '      --prompt   StoryPrompt.json    story title, genre, constraints\n'
    printf '      --out      Script.json         where to write the output\n'
    printf '      [--writing-agent-cmd <cmd>]    default: "writing-agent generate"\n'
    printf '\n'
    printf '    Calls an external writing agent to generate Script.json from a\n'
    printf '    story prompt.  The agent must accept --prompt and --out flags.\n'
    printf '    Produces: Script.json\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator write \\\n'
    printf '        --prompt my_story.json \\\n'
    printf '        --out artifacts/Script.json\n'

    printf '\n  ──\n\n'

    printf '  orchestrator run  --media  AssetManifest.media.json\n'
    printf '      --media  path/to/AssetManifest.media.json\n'
    printf '      [--force]   overwrite existing RenderPlan.json\n'
    printf '\n'
    printf '    Media mode — Stage 4 only (generate RenderPlan from resolved assets):\n'
    printf '      Point at the AssetManifest.media.json produced by the media agent.\n'
    printf '      Everything else is auto-discovered from the same directory:\n'
    printf '\n'
    printf '        AssetManifest.media.json          ← YOU PROVIDE (base / shared)\n'
    printf '        AssetManifest.media.zh-Hans.json  ← YOU PROVIDE (locale VO)\n'
    printf '        AssetManifest_draft.json          ← already present\n'
    printf '        AssetManifest_draft.zh-Hans.json  ← already present\n'
    printf '        ShotList.json                     ← already present\n'
    printf '\n'
    printf '    Outputs (written to the same directory):\n'
    printf '        AssetManifest_final.json          ← base manifest\n'
    printf '        AssetManifest_final.zh-Hans.json  ← locale manifest\n'
    printf '        RenderPlan.json                   ← single file with locale_tracks\n'
    printf '                                             (VO per locale) and\n'
    printf '                                             resolved_assets (shared visual\n'
    printf '                                             sfx, music — reused by all)\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator run \\\n'
    printf '        --media projects/the-pharaoh-who-defied-death/episodes/s01e02/AssetManifest.media.json\n'

    printf '\n  ──\n\n'

    printf '  orchestrator run  --project  project.json  (legacy full-pipeline mode)\n'
    printf '\n'
    printf '      --project  project.json        project config (id, genre, budget...)\n'
    printf '      [--artifacts-dir ./artifacts]  where to store all run artifacts\n'
    printf '      [--run-id  <id>]               override the auto-generated run ID\n'
    printf '      [--force]                      re-run all stages (ignore cache)\n'
    printf '      [--from-stage 1-5]             run only stage N (skips all others)\n'
    printf '      [--to-last-stage]              with --from-stage N, run N through last\n'
    printf '      [--stub]                       auto-create missing external inputs:\n'
    printf '                                     AssetManifest.media.json (empty) before stage 4\n'
    printf '                                     CanonDecision.json (allow)  before stage 5\n'
    printf '                                     Useful for CI and local e2e tests.\n'
    printf '\n'
    printf '    Runs the full 5-stage pipeline in sequence:\n'
    printf '      Stage 1  generate_script          → Script.json\n'
    printf '      Stage 2  script_to_shotlist        → ShotList.json\n'
    printf '      Stage 3  shotlist_to_assetmanifest → AssetManifest_draft.json\n'
    printf '      Stage 4  build_renderplan          → AssetManifest_final.json + RenderPlan.json\n'
    printf '      Stage 5  render_preview            → RenderOutput.json\n'
    printf '\n'
    printf '    Stages whose artifacts already exist are skipped automatically.\n'
    printf '    Stage 5 requires CanonDecision.json (see CANON GATE below).\n'
    printf '\n'
    printf '    Example — full run:\n'
    printf '      orchestrator run --project examples/phase0/project.json\n'
    printf '\n'
    printf '    Example — run only stage 3:\n'
    printf '      orchestrator run \\\n'
    printf '        --project examples/phase0/project.json \\\n'
    printf '        --from-stage 3\n'
    printf '\n'
    printf '    Example — run stage 3 through the last stage:\n'
    printf '      orchestrator run \\\n'
    printf '        --project examples/phase0/project.json \\\n'
    printf '        --from-stage 3 --to-last-stage\n'
    printf '\n'
    printf '    Example — force re-run of stage 3 only (artifact already exists):\n'
    printf '      orchestrator run \\\n'
    printf '        --project examples/phase0/project.json \\\n'
    printf '        --from-stage 3 --force\n'

    printf '\n── CANON GATE (required before Stage 5 runs) ────────────────────\n\n'

    printf '    After Stage 4 completes, the pipeline pauses before rendering\n'
    printf '    until you place a CanonDecision.json in the run directory:\n'
    printf '\n'
    printf '      artifacts/<project_id>/<run_id>/CanonDecision.json\n'
    printf '\n'
    printf '    Minimum contents to allow rendering:\n'
    printf '      {\n'
    printf '        "schema_id": "CanonDecision",\n'
    printf '        "schema_version": "1.0.0",\n'
    printf '        "decision": "allow",\n'
    printf '        "decision_id": "your-decision-id"\n'
    printf '      }\n'
    printf '\n'
    printf '    Set "decision": "deny" to block rendering and record the rejection.\n'
    printf '    Then resume with:\n'
    printf '      orchestrator run --project <project.json> --from-stage 5\n'
    printf '\n'
    printf '    Note: the --draft mode does not run Stage 5.  Canon gate only\n'
    printf '    applies to the legacy --project pipeline.\n'

    printf '\n── INSPECT A RUN ────────────────────────────────────────────────\n\n'

    printf '  orchestrator explain --run <run_dir>\n'
    printf '\n'
    printf '    Prints each stage with its input files and output files (paths\n'
    printf '    and SHA-256 hashes) as recorded in RunIndex.json.\n'
    printf '    Use this to understand exactly what each stage consumed and produced.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator explain \\\n'
    printf '        --run artifacts/my-project/run-abc123/\n'

    printf '\n  ──\n\n'

    printf '  orchestrator validate-run --run <run_dir>\n'
    printf '\n'
    printf '    Re-hashes every artifact and checks schema_id/schema_version\n'
    printf '    presence.  Fails if any file is missing, corrupt, or lacks\n'
    printf '    schema metadata.  Use after any manual edits to verify integrity.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator validate-run \\\n'
    printf '        --run artifacts/my-project/run-abc123/\n'

    printf '\n  ──\n\n'

    printf '  orchestrator diff\n'
    printf '      --run     <run_dir_A>\n'
    printf '      --against <run_dir_B>\n'
    printf '\n'
    printf '    Compares two runs field by field (SHA-256 + JSON diff per artifact).\n'
    printf '    Exits 0 if identical, 1 if any difference found.\n'
    printf '    Use to confirm two runs are deterministically equal.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator diff \\\n'
    printf '        --run     artifacts/my-project/run-abc123/ \\\n'
    printf '        --against artifacts/my-project/run-def456/\n'

    printf '\n── RESUME / REPAIR ──────────────────────────────────────────────\n\n'

    printf '  orchestrator replay --run <run_dir>\n'
    printf '\n'
    printf '    Re-hashes all outputs in RunIndex.json.  Deletes any file whose\n'
    printf '    hash does not match, then re-runs only those stages.  Safe to run\n'
    printf '    after partial failures or accidental file edits.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator replay \\\n'
    printf '        --run artifacts/my-project/run-abc123/\n'

    printf '\n── BUNDLE (package a completed run for distribution) ────────────\n\n'

    printf '  orchestrator package\n'
    printf '      --run        <run_dir>     finished run directory\n'
    printf '      --episode-id <id>          stable episode identifier\n'
    printf '      --out        <out_dir>     parent directory for the bundle\n'
    printf '      [--mode copy|hardlink]     default: copy\n'
    printf '\n'
    printf '    Assembles all run artifacts into a portable EpisodeBundle directory\n'
    printf '    and writes EpisodeBundle.json with per-file hashes and a bundle_hash.\n'
    printf '    Use before handing the episode off to a video-engine or for archival.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator package \\\n'
    printf '        --run        artifacts/my-project/run-abc123/ \\\n'
    printf '        --episode-id ep-001 \\\n'
    printf '        --out        bundles/\n'

    printf '\n  ──\n\n'

    printf '  orchestrator validate-bundle --bundle <bundle_dir>\n'
    printf '\n'
    printf '    Re-verifies every artifact file hash and the bundle_hash inside\n'
    printf '    EpisodeBundle.json.  Use after transferring a bundle to confirm\n'
    printf '    nothing was corrupted in transit.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator validate-bundle --bundle bundles/ep-001/\n'

    printf '\n── QUALITY & DIAGNOSTICS ────────────────────────────────────────\n\n'

    printf '  orchestrator investigate-determinism\n'
    printf '      --project <project.json>\n'
    printf '      --out     <out_dir>\n'
    printf '\n'
    printf '    Runs the pipeline twice with the same project config, then compares\n'
    printf '    all contract artifacts (ShotList, AssetManifest, RenderPlan,\n'
    printf '    RenderOutput).  Writes DeterminismReport.json.  Exits 1 if any\n'
    printf '    semantic field differs between the two runs.\n'
    printf '    Run this after changing any stage to confirm determinism is intact.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator investigate-determinism \\\n'
    printf '        --project examples/phase0/project.json \\\n'
    printf '        --out     /tmp/det-check/\n'

    printf '\n  ──\n\n'

    printf '  orchestrator verify-system\n'
    printf '\n'
    printf '    End-to-end health check: probes world-engine, media, and video\n'
    printf '    binaries (skips any that are not installed), then runs the full\n'
    printf '    pipeline against the example project, validate-run, and diff\n'
    printf '    against itself.  Exits 0 only if every check passes.\n'
    printf '    Run this after install or before a release.\n'
    printf '\n'
    printf '    Example:\n'
    printf '      orchestrator verify-system\n'

    printf '\n══════════════════════════════════════════════════════════════════\n'
    printf '  Tip: every command supports --help for full flag documentation.\n'
    printf '  Example: orchestrator run --help\n'
    printf '══════════════════════════════════════════════════════════════════\n\n'
}

do_test_from_stage_workflows() {
    # Run orchestrator with --from-stage 3 and --from-stage 4 using the e2e golden
    # ShotList/AssetManifest from contracts/.  Each invocation runs only the single
    # named stage (--to-last-stage is NOT set), so each test is independent.

    GOLDENS="contracts/goldens/e2e/example_episode"
    PROJECT="examples/phase0/project.json"
    PROJECT_ID="phase0-demo"
    TMP_DIR="$(mktemp -d)"
    WORKFLOW_ERRORS=0

    printf '\n── Workflow A: --from-stage 3  (ShotList → AssetManifest only) ──────────\n'

    RUN_ID_A="run-s3-goldens-test"
    RUN_DIR_A="$TMP_DIR/$PROJECT_ID/$RUN_ID_A"
    run_cmd mkdir -p "$RUN_DIR_A"
    run_cmd cp "$GOLDENS/ShotList.json" "$RUN_DIR_A/ShotList.json"

    printf '  + orchestrator run --project %s \\\n' "$PROJECT"
    printf '        --artifacts-dir %s \\\n' "$TMP_DIR"
    printf '        --run-id %s \\\n' "$RUN_ID_A"
    printf '        --from-stage 3\n'
    "$ORCHESTRATOR" run \
        --project "$PROJECT" \
        --artifacts-dir "$TMP_DIR" \
        --run-id "$RUN_ID_A" \
        --from-stage 3

    if [ -f "$RUN_DIR_A/AssetManifest_draft.json" ]; then
        printf '  ✓  AssetManifest_draft.json produced\n'
    else
        printf '  ✗  AssetManifest_draft.json NOT produced — stage 3 failed\n'
        WORKFLOW_ERRORS=$((WORKFLOW_ERRORS + 1))
    fi
    if [ -f "$RUN_DIR_A/RenderPlan.json" ]; then
        printf '  ✗  RenderPlan.json unexpectedly produced (stage 4 should be skipped)\n'
        WORKFLOW_ERRORS=$((WORKFLOW_ERRORS + 1))
    else
        printf '  ✓  RenderPlan.json absent (stage 4 correctly skipped)\n'
    fi

    printf '\n── Workflow B: --from-stage 4  (AssetManifest + ShotList → RenderPlan) ──\n'

    RUN_ID_B="run-s4-goldens-test"
    RUN_DIR_B="$TMP_DIR/$PROJECT_ID/$RUN_ID_B"
    run_cmd mkdir -p "$RUN_DIR_B"
    run_cmd cp "$GOLDENS/ShotList.json"              "$RUN_DIR_B/ShotList.json"
    run_cmd cp "$GOLDENS/AssetManifest_draft.json"   "$RUN_DIR_B/AssetManifest_draft.json"
    run_cmd cp "$GOLDENS/AssetManifest.media.json"   "$RUN_DIR_B/AssetManifest.media.json"

    printf '  + orchestrator run --project %s \\\n' "$PROJECT"
    printf '        --artifacts-dir %s \\\n' "$TMP_DIR"
    printf '        --run-id %s \\\n' "$RUN_ID_B"
    printf '        --from-stage 4\n'
    "$ORCHESTRATOR" run \
        --project "$PROJECT" \
        --artifacts-dir "$TMP_DIR" \
        --run-id "$RUN_ID_B" \
        --from-stage 4

    if [ -f "$RUN_DIR_B/AssetManifest_final.json" ]; then
        printf '  ✓  AssetManifest_final.json produced\n'
    else
        printf '  ✗  AssetManifest_final.json NOT produced — stage 4 failed\n'
        WORKFLOW_ERRORS=$((WORKFLOW_ERRORS + 1))
    fi
    if [ -f "$RUN_DIR_B/RenderPlan.json" ]; then
        printf '  ✓  RenderPlan.json produced\n'
    else
        printf '  ✗  RenderPlan.json NOT produced — stage 4 failed\n'
        WORKFLOW_ERRORS=$((WORKFLOW_ERRORS + 1))
    fi

    rm -rf "$TMP_DIR"

    if [ "$WORKFLOW_ERRORS" -gt 0 ]; then
        printf '\n  ✗  From-stage workflow tests FAILED (%d error(s))\n' "$WORKFLOW_ERRORS"
        exit 1
    fi
    printf '\n  ✓  From-stage workflow tests passed\n'
}

do_test_e2e_determinism() {
    # Locate the orchestrator binary: .venv (created by option 1) → directory
    # next to $PYTHON → PATH.  Skip gracefully if none is found so that option 2
    # can still be run without having run option 1 first.
    DET_BIN=""
    if [ -x "$REPO_ROOT/.venv/bin/orchestrator" ]; then
        DET_BIN="$REPO_ROOT/.venv/bin/orchestrator"
    elif [ -x "$(dirname "$(command -v "$PYTHON")")/orchestrator" ]; then
        DET_BIN="$(dirname "$(command -v "$PYTHON")")/orchestrator"
    elif command -v orchestrator > /dev/null 2>&1; then
        DET_BIN="$(command -v orchestrator)"
    fi

    if [ -z "$DET_BIN" ]; then
        printf '    SKIP  orchestrator binary not found — run option 1 first\n'
        return 0
    fi

    DET_TMP="$(mktemp -d)"
    printf '  + orchestrator investigate-determinism \\\n'
    printf '        --project examples/phase0/project.json \\\n'
    printf '        --out %s\n' "$DET_TMP"
    "$DET_BIN" investigate-determinism \
        --project examples/phase0/project.json \
        --out "$DET_TMP"
    rm -rf "$DET_TMP"
    printf '  ✓  investigate-determinism: pass\n'
}

do_test() {
    printf '\n[2] Running tests...\n'
    run_cmd "$PYTHON" -m pytest tests/ -v
    printf '\n    Running integration verification...\n'
    if [ -n "${VIDEO_RENDERER_REPO:-}" ]; then
        run_cmd "$PYTHON" scripts/verify_integration.py
    else
        printf '    SKIP  verify_integration.py (VIDEO_RENDERER_REPO not set)\n'
    fi
    printf '\n    Running contract verification...\n'
    run_cmd "$PYTHON" contracts/tools/verify_contracts.py
    printf '\n    Running lint (syntax check)...\n'
    run_cmd "$PYTHON" -m py_compile \
        orchestrator/cli.py \
        orchestrator/pipeline.py \
        orchestrator/registry.py \
        orchestrator/validator.py \
        orchestrator/utils/hashing.py \
        orchestrator/stages/stage1_generate_script.py \
        orchestrator/stages/stage2_script_to_shotlist.py \
        orchestrator/stages/stage3_shotlist_to_assetmanifest.py \
        orchestrator/stages/stage4_build_renderplan.py \
        orchestrator/stages/stage5_render_preview.py
    printf '\n    Running from-stage workflow tests...\n'
    do_test_from_stage_workflows
    printf '\n    Running e2e determinism check...\n'
    do_test_e2e_determinism
    printf '\n    All checks passed.\n'
}

# ---------------------------------------------------------------------------
# Interactive menu loop
# ---------------------------------------------------------------------------

while true; do
    printf '\nOrchestrator — setup menu\n'
    printf '  1) Install & verify\n'
    printf '  2) Run tests\n'
    printf '  3) Show usage\n'
    printf '  0) Exit\n'
    printf 'Choice: '
    read -r choice
    case "$choice" in
        1) do_install_and_verify ;;
        2) do_test ;;
        3) do_show_usage ;;
        0) exit 0 ;;
        *) printf 'Unknown option: %s\n' "$choice" ;;
    esac
done
