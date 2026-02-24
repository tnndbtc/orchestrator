# Orchestrator — Phase 0 Workstream A

A deterministic, schema-validated, resumable 5-stage pipeline that converts a
project config into a full render output — screenplay → shot-list → asset
manifest → render plan → render output.  All stages run locally with no
external AI calls, no network I/O, and no ffmpeg required.

---

## Prerequisites

- Python 3.12+ on PATH.  No pre-created venv needed — `setup.sh` creates
  `.venv` automatically.

---

## Quick Start

```bash
./setup.sh
```

Interactive menu:

```
Orchestrator — setup menu
  1) Install & verify
  2) Run tests
  3) Show usage
  0) Exit
```

**Option 1** creates `.venv`, installs the package (`pip install -e ".[dev]"`),
and runs the example pipeline end-to-end to verify the install.

**Option 2** runs the full test suite (pytest + contract verification + lint +
from-stage workflow tests + e2e determinism check).

**Option 3** prints a full CLI reference for every supported command.

---

## Install (manual)

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

---

## Pipeline Overview

The pipeline runs five stages in sequence.  Each stage reads the outputs of
earlier stages and writes a new schema-validated artifact to the run directory.

| # | Stage | Reads | Writes |
|---|-------|-------|--------|
| 1 | `stage1_generate_script` | — | `Script.json` |
| 2 | `stage2_script_to_shotlist` | `Script.json` | `ShotList.json` |
| 3 | `stage3_shotlist_to_assetmanifest` | `ShotList.json` | `AssetManifest_draft.json` |
| 4 | `stage4_build_renderplan` | `AssetManifest_draft.json`, `AssetManifest.media.json`¹, `ShotList.json` | `AssetManifest_final.json`, `RenderPlan.json` |
| 5 | `stage5_render_preview` | `RenderPlan.json`, `AssetManifest_final.json` | `RenderOutput.json` |

¹ `AssetManifest.media.json` is an **external input** written by the media
agent.  Use `--stub` in development to auto-create an empty placeholder.

### Canon Gate

The pipeline pauses before Stage 5 until `CanonDecision.json` is present in
the run directory.  Minimum contents to allow rendering:

```json
{
  "schema_id": "CanonDecision",
  "schema_version": "1.0.0",
  "decision": "allow",
  "decision_id": "your-decision-id"
}
```

Set `"decision": "deny"` to block rendering and record the rejection.

---

## Artifact Layout

```
artifacts/
└── <project_id>/
    └── <run_id>/
        ├── Script.json
        ├── Script.meta.json
        ├── ShotList.json
        ├── ShotList.meta.json
        ├── AssetManifest_draft.json
        ├── AssetManifest.media.json       ← written by media agent
        ├── AssetManifest_final.json
        ├── AssetManifest_final.meta.json
        ├── CanonDecision.json
        ├── RenderPlan.json
        ├── RenderPlan.meta.json
        ├── RenderOutput.json
        ├── RenderOutput.meta.json
        ├── RunIndex.json
        └── run_summary.json
```

Each `.meta.json` records the artifact hash, schema version, parent references,
creation parameters, and creation timestamp.

The `run_id` is derived deterministically from the SHA-256 of the project
config (canonical JSON), so the same config always maps to the same run
directory.

---

## CLI Reference

All commands are accessed via the `orchestrator` binary installed into the
active Python environment.

### `orchestrator run` — execute the pipeline

```bash
orchestrator run \
  --project  examples/phase0/project.json \
  [--artifacts-dir ./artifacts] \
  [--run-id  <id>] \
  [--from-stage 1-5] \
  [--to-last-stage] \
  [--force] \
  [--stub]
```

Runs the full 5-stage pipeline.  Stages whose artifact already exists and is
schema-valid are skipped automatically.

| Flag | Purpose |
|------|---------|
| `--from-stage N` | Start at stage N; earlier stages are skipped |
| `--to-last-stage` | Combined with `--from-stage N`, run N through the last stage |
| `--force` | Re-run all eligible stages even if artifacts already exist |
| `--stub` | Auto-create missing external inputs (`AssetManifest.media.json`, `CanonDecision.json`); useful for development and CI |

Examples:

```bash
# Full run
orchestrator run --project examples/phase0/project.json

# Re-run only stage 3
orchestrator run --project examples/phase0/project.json --from-stage 3

# Re-run stages 3 through 5
orchestrator run --project examples/phase0/project.json --from-stage 3 --to-last-stage

# Force re-run of all stages
orchestrator run --project examples/phase0/project.json --force

# Development run — stubs out all external inputs
orchestrator run --project examples/phase0/project.json --stub
```

---

### `orchestrator write` — generate a script via writing-agent

```bash
orchestrator write \
  --prompt StoryPrompt.json \
  --out    Script.json \
  [--writing-agent-cmd "writing-agent generate"]
```

Calls the external writing agent to produce `Script.json` from a story prompt.
The agent must accept `--prompt` and `--out` flags.

---

### `orchestrator explain` — inspect a run

```bash
orchestrator explain --run artifacts/<project_id>/<run_id>/
```

Prints each stage with its input and output files (paths + SHA-256 hashes) as
recorded in `RunIndex.json`.  Use this to understand exactly what each stage
consumed and produced.

---

### `orchestrator validate-run` — verify artifact integrity

```bash
orchestrator validate-run --run artifacts/<project_id>/<run_id>/
```

Re-hashes every artifact and checks `schema_id`/`schema_version` presence.
Fails if any file is missing, corrupt, or lacks schema metadata.  Run after any
manual edits to verify nothing was accidentally corrupted.

---

### `orchestrator diff` — compare two runs

```bash
orchestrator diff \
  --run     artifacts/<project_id>/<run_id_a>/ \
  --against artifacts/<project_id>/<run_id_b>/
```

Compares two runs field-by-field (SHA-256 + JSON diff per artifact).  Exits 0
if identical, 1 if any difference is found.  Use to confirm two runs are
deterministically equal.

---

### `orchestrator replay` — resume after a partial failure

```bash
orchestrator replay --run artifacts/<project_id>/<run_id>/
```

Re-hashes all outputs recorded in `RunIndex.json`.  Deletes any file whose hash
does not match, then re-runs only those stages.  Safe to run after partial
failures or accidental file edits.

---

### `orchestrator package` — bundle a completed run for distribution

```bash
orchestrator package \
  --run        artifacts/<project_id>/<run_id>/ \
  --episode-id ep-001 \
  --out        bundles/ \
  [--mode copy|hardlink]
```

Assembles all run artifacts into a portable `EpisodeBundle` directory and
writes `EpisodeBundle.json` with per-file hashes and a `bundle_hash`.

---

### `orchestrator validate-bundle` — verify a bundle after transfer

```bash
orchestrator validate-bundle --bundle bundles/ep-001/
```

Re-verifies every artifact file hash and the `bundle_hash` inside
`EpisodeBundle.json`.  Use after transferring a bundle to confirm nothing was
corrupted in transit.

---

### `orchestrator investigate-determinism` — check pipeline determinism

```bash
orchestrator investigate-determinism \
  --project examples/phase0/project.json \
  --out     /tmp/det-check/
```

Runs the pipeline **twice** with the same project config, then compares all
contract artifacts (ShotList, AssetManifest, RenderPlan, RenderOutput).  Writes
`DeterminismReport.json`.  Exits 1 if any semantic field differs between the two
runs.  Run this after changing any stage to confirm determinism is intact.

---

### `orchestrator verify-system` — end-to-end health check

```bash
orchestrator verify-system
```

Probes available external binaries (`world-engine`, `media`, `video` — skips
any not installed), then runs the full pipeline against the example project,
`validate-run`, and `diff` against itself.  Exits 0 only if every check passes.
Run after install or before a release.

---

## Running Tests

```bash
.venv/bin/pytest tests/ -v
# or via the menu:
./setup.sh   # → option 2
```

Option 2 of `setup.sh` also runs contract verification, a lint/syntax check,
from-stage workflow tests, and a live e2e determinism check on top of the pytest
suite.
