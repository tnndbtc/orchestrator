# Orchestrator — Phase 0 Workstream A

CLI + Artifact Registry per Master Plan v1.2 §19.0 (Phase 0 / Workstream A).

Implements a deterministic, schema-validated, resumable 5-stage pipeline that produces screenplay → shot-list → asset manifest → render plan → render output artifacts — all using local stub logic (no external AI, no network, no ffmpeg).

---

## Prerequisites

- Python 3.12+
- virtualenv already created at `~/.virtualenvs/orchestrator`

---

## Install

```bash
make install
```

This runs `pip install -e ".[dev]"` into `~/.virtualenvs/orchestrator`.

---

## Run the Example Pipeline

```bash
make run-example
# equivalent to:
orchestrator run --project examples/phase0/project.json
```

Artifacts are written to `./artifacts/<project_id>/<run_id>/`.

---

## Resume and Force Flags

Skip already-completed stages (default behaviour — artifacts already exist and are schema-valid):

```bash
orchestrator run --project examples/phase0/project.json
# All 5 stages skipped (↩) if artifacts exist and are valid
```

Re-run from a specific stage:

```bash
orchestrator run --project examples/phase0/project.json --from-stage 3
# Stages 1–2 skipped; stages 3–5 re-run
```

Force re-run of all stages:

```bash
orchestrator run --project examples/phase0/project.json --force
# All 5 stages executed (✓)
```

Use an explicit run ID (overrides the hash-derived default):

```bash
orchestrator run --project examples/phase0/project.json --run-id my-run-001
```

---

## Run Tests

```bash
make test
# equivalent to:
~/.virtualenvs/orchestrator/bin/pytest tests/ -v
```

---

## Artifact Structure

```
artifacts/
└── <project_id>/
    └── <run_id>/
        ├── Script.json
        ├── Script.meta.json
        ├── ShotList.json
        ├── ShotList.meta.json
        ├── AssetManifest.json
        ├── AssetManifest.meta.json
        ├── RenderPlan.json
        ├── RenderPlan.meta.json
        ├── RenderOutput.json
        ├── RenderOutput.meta.json
        └── run_summary.json
```

Each `.meta.json` file records the artifact hash, schema version, parent references, creation parameters, compute origin, and creation timestamp.

The `run_id` is derived deterministically from the SHA-256 hash of the project config (canonical JSON), ensuring the same project config always maps to the same run directory.

---

## Schema Versions

| Artifact       | Schema File              | Version |
|----------------|--------------------------|---------|
| Script         | schemas/Script.v1.json   | 1.0.0   |
| ShotList       | schemas/ShotList.v1.json | 1.0.0   |
| AssetManifest  | schemas/AssetManifest.v1.json | 1.0.0 |
| RenderPlan     | schemas/RenderPlan.v1.json | 1.0.0  |
| RenderOutput   | schemas/RenderOutput.v1.json | 1.0.0 |
| RenderPackage  | schemas/RenderPackage.v1.json | 1.0.0 |

All schemas use JSON Schema draft-07 with `"additionalProperties": true` for forward-compatibility (§30.2).

---

## Pipeline Stages

| # | Stage | Reads | Writes |
|---|-------|-------|--------|
| 1 | `stage1_generate_script` | — | Script |
| 2 | `stage2_script_to_shotlist` | Script | ShotList |
| 3 | `stage3_shotlist_to_assetmanifest` | ShotList, Script | AssetManifest |
| 4 | `stage4_build_renderplan` | AssetManifest, ShotList | RenderPlan |
| 5 | `stage5_render_preview` | RenderPlan, ShotList | RenderOutput |
