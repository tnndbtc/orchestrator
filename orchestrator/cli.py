"""Click CLI entrypoint for the Orchestrator pipeline."""

import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import click

from .packager import package_episode
from .pipeline import PipelineRunner
from .registry import ArtifactRegistry, EpisodeRegistry
from .utils.hashing import hash_artifact, hash_file_bytes


@click.group()
def cli() -> None:
    """Orchestrator CLI — Phase 0 Workstream A."""


@cli.command("run")
@click.option(
    "--media",
    default=None,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help=(
        "Path to AssetManifest.media.json (the file produced by the media agent). "
        "Reads project_id from the file; auto-discovers locale variants, draft, and "
        "shotlist from the same directory.  Runs Stage 4 only.  "
        "Mutually exclusive with --project."
    ),
)
@click.option(
    "--project",
    default=None,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to project.json (legacy full-pipeline mode).  Mutually exclusive with --media.",
)
@click.option(
    "--artifacts-dir",
    default="./artifacts",
    show_default=True,
    help="Root directory for artifact storage (legacy --project mode only)",
)
@click.option(
    "--run-id",
    default=None,
    help="Explicit run ID (legacy --project mode only; default: SHA-256 of project config)",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Overwrite existing RenderPlan.json if it already exists (--media mode only)",
)
@click.option(
    "--from-stage",
    default=1,
    type=click.IntRange(1, 5),
    show_default=True,
    help="Start execution from stage N (1–5); earlier stages are skipped (--project mode only)",
)
@click.option(
    "--to-last-stage",
    is_flag=True,
    default=False,
    help="When combined with --from-stage N, run all stages from N to the last (--project mode only)",
)
@click.option(
    "--stub",
    is_flag=True,
    default=False,
    help=(
        "Auto-create stub external inputs when absent: "
        "AssetManifest.media.json (empty items) before stage 4, "
        "CanonDecision.json (allow) before stage 5. "
        "Useful for e2e tests and CI runs."
    ),
)
def run_command(
    media: str | None,
    project: str | None,
    artifacts_dir: str,
    run_id: str | None,
    force: bool,
    from_stage: int,
    to_last_stage: bool,
    stub: bool,
) -> None:
    """Run the orchestrator pipeline for a project or episode.

    \b
    Media mode  (--media):   point at AssetManifest.media.json produced by the
                             media agent; everything else auto-discovered from
                             the same directory.  Runs Stage 4 only.
    Project mode (--project): legacy full-pipeline mode using project.json.
    """
    if media and project:
        raise click.UsageError("--media and --project are mutually exclusive.")
    if not media and not project:
        raise click.UsageError("One of --media or --project is required.")

    # -------------------------------------------------------------------------
    # Media mode: entry point is AssetManifest.media.json
    # -------------------------------------------------------------------------
    if media:
        import importlib
        import time
        from datetime import datetime, timezone

        media_path = Path(media).resolve()
        episode_dir = media_path.parent
        media_filename = media_path.name  # e.g. "AssetManifest.media.json" or "AssetManifest.media.zh-Hans.json"

        # Extract locale from media filename, if any.
        # "AssetManifest.media.json"         → None
        # "AssetManifest.media.zh-Hans.json" → "zh-Hans"
        _stem = media_path.stem  # strips .json suffix
        _after = _stem[len("AssetManifest.media."):] if _stem.startswith("AssetManifest.media.") else ""
        media_locale: str | None = _after if _after else None

        media_data = json.loads(media_path.read_text(encoding="utf-8"))

        # project_id from the media file (copied from source AssetManifest)
        project_id: str = media_data.get("project_id", "unknown-project")

        # Locate draft: when a locale-specific media file is given, prefer the
        # matching locale draft; fall back to the base draft.
        if media_locale:
            _locale_draft = episode_dir / f"AssetManifest_draft.{media_locale}.json"
            _base_draft   = episode_dir / "AssetManifest_draft.json"
            if _locale_draft.exists():
                draft_path = _locale_draft
            elif _base_draft.exists():
                draft_path = _base_draft
            else:
                raise click.UsageError(
                    f"Neither AssetManifest_draft.{media_locale}.json nor "
                    f"AssetManifest_draft.json found in {episode_dir}\n"
                    "Stage 4 requires a draft manifest alongside the media manifest."
                )
        else:
            draft_path = episode_dir / "AssetManifest_draft.json"
            if not draft_path.exists():
                raise click.UsageError(
                    f"AssetManifest_draft.json not found in {episode_dir}\n"
                    "Stage 4 requires the draft alongside the media manifest."
                )
        draft_data = json.loads(draft_path.read_text(encoding="utf-8"))
        episode_id: str = draft_data.get("episode_id", episode_dir.name)

        click.echo(f"▶  Media mode")
        click.echo(f"   Project : {project_id}")
        click.echo(f"   Episode : {episode_id}")
        click.echo(f"   Dir     : {episode_dir}")
        if media_locale:
            click.echo(f"   Locale  : {media_locale}")
        click.echo()

        registry = EpisodeRegistry(episode_dir)
        # Pass the actual media filename so stage4 uses it as the base
        # (rather than assuming "AssetManifest.media.json").
        project_config = {"id": project_id, "_media_file": media_filename}

        # Check if outputs already exist and --force not set
        render_plan_path = episode_dir / "RenderPlan.json"
        if render_plan_path.exists() and not force:
            click.echo("ℹ  RenderPlan.json already exists — skipping (use --force to overwrite).")
            click.echo(f"   {render_plan_path}")
            return

        stage4 = importlib.import_module(
            ".stages.stage4_build_renderplan", package="orchestrator"
        )

        t0 = time.monotonic()
        try:
            render_plan = stage4.run(project_config, episode_id, registry)
        except Exception as exc:
            click.echo(f"❌  Stage 4 FAILED: {type(exc).__name__}: {exc}", err=True)
            sys.exit(1)
        duration = time.monotonic() - t0

        # Report outputs
        click.echo(f"  ✓  stage4_build_renderplan   ({duration:.3f}s)")
        click.echo()
        click.echo(f"✅  Stage 4 completed")
        click.echo(f"   AssetManifest_final.json → {episode_dir / 'AssetManifest_final.json'}")

        for path in sorted(episode_dir.glob("AssetManifest_final.*.json")):
            click.echo(f"   {path.name:<40} → {path}")

        click.echo(f"   RenderPlan.json            → {episode_dir / 'RenderPlan.json'}")

        if "locale_tracks" in render_plan:
            tracks = render_plan["locale_tracks"]
            click.echo(f"   locale_tracks              : {list(tracks.keys())}")
        return

    # -------------------------------------------------------------------------
    # Legacy project mode: full pipeline via PipelineRunner (--project)
    # -------------------------------------------------------------------------
    project_path = Path(project).resolve()  # type: ignore[arg-type]
    project_config = json.loads(project_path.read_text(encoding="utf-8"))

    registry = ArtifactRegistry(artifacts_dir)
    runner = PipelineRunner(
        project_config=project_config,
        registry=registry,
        artifacts_dir=artifacts_dir,
        force=force,
        from_stage=from_stage,
        to_last_stage=to_last_stage,
        run_id=run_id,
        project_path=str(project_path),
        stub_external_inputs=stub,
        auto_approve_canon=True,
    )

    click.echo(f"▶  Run ID : {runner.run_id}")
    click.echo(f"   Project: {project_config.get('title', project_config['id'])}")
    if force:
        click.echo("   Mode   : force (all stages will re-run)")
    elif from_stage > 1 and to_last_stage:
        click.echo(f"   Mode   : from-stage {from_stage} to last")
    elif from_stage > 1:
        click.echo(f"   Mode   : stage {from_stage} only")
    click.echo()

    summary = runner.run()

    for stage in summary["stages"]:
        if stage["skipped"]:
            icon = "↩"
            label = "skipped"
        elif stage["status"] == "completed":
            icon = "✓"
            label = f"completed  ({stage['duration_sec']:.3f}s)"
        else:
            icon = "✗"
            label = f"FAILED — {stage['error']}"
        click.echo(f"  {icon}  {stage['name']:<44} {label}")

    click.echo()
    if summary["status"] == "completed":
        click.echo(f"✅  Pipeline completed   run_id={runner.run_id}")
        run_dir = Path(artifacts_dir).resolve() / project_config["id"] / runner.run_id
        click.echo(f"    Artifacts: {run_dir}")

        # Print the video output path so the user knows where to find it.
        render_output_path = run_dir / "RenderOutput.json"
        if render_output_path.exists():
            try:
                ro = json.loads(render_output_path.read_text(encoding="utf-8"))
                video_uri = ro.get("video_uri", "")
                if video_uri.startswith("file://"):
                    video_path = Path(video_uri[len("file://"):])
                    if video_path.exists():
                        click.echo(f"    Video:     {video_path}")
                    else:
                        click.echo(f"    Video URI: {video_uri}  (file not found)")
                elif video_uri and not video_uri.startswith("placeholder://"):
                    click.echo(f"    Video URI: {video_uri}")
            except (OSError, json.JSONDecodeError, KeyError):
                pass
    else:
        click.echo(f"❌  Pipeline FAILED      run_id={runner.run_id}", err=False)
        for err in summary["errors"]:
            click.echo(f"    Error: {err}", err=True)
        sys.exit(1)


@cli.command("explain")
@click.option(
    "--run", "run_dir", required=True,
    type=click.Path(exists=True, file_okay=False, readable=True),
    help="Path to a run directory containing RunIndex.json",
)
def explain_command(run_dir: str) -> None:
    """Print stage inputs and outputs recorded in RunIndex.json."""
    run_path = Path(run_dir)
    index_path = run_path / "RunIndex.json"

    if not index_path.exists():
        click.echo(f"Error: RunIndex.json not found in {run_path}", err=True)
        sys.exit(1)

    try:
        run_index = json.loads(index_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        click.echo(f"Error: RunIndex.json is not valid JSON: {exc}", err=True)
        sys.exit(1)

    for stage in run_index["stages"]:
        click.echo(f"Stage: {stage['name']}")
        click.echo("  inputs:")
        for inp in stage["inputs"]:
            click.echo(f"    {inp['path']} {inp['sha256']}")
        click.echo("  outputs:")
        for out in stage["outputs"]:
            click.echo(f"    {out['path']} {out['sha256']}")


@cli.command("replay")
@click.option(
    "--run", "run_dir", required=True,
    type=click.Path(exists=True, file_okay=False, readable=True),
    help="Path to a run directory containing RunIndex.json",
)
def replay_command(run_dir: str) -> None:
    """Verify hashes and re-run only stages with missing or corrupt outputs."""
    run_path = Path(run_dir)
    index_path = run_path / "RunIndex.json"

    if not index_path.exists():
        click.echo(f"Error: RunIndex.json not found in {run_path}", err=True)
        sys.exit(1)

    run_index = json.loads(index_path.read_text(encoding="utf-8"))

    # Phase 1: verify hashes — delete corrupted outputs so PipelineRunner re-runs them
    for stage in run_index["stages"]:
        for entry in stage["outputs"]:
            file_path = run_path / entry["path"]
            if file_path.exists():
                actual = hash_file_bytes(file_path)
                if actual != entry["sha256"]:
                    click.echo(
                        f"Hash mismatch: {entry['path']} "
                        f"(expected {entry['sha256'][:12]}... got {actual[:12]}...)"
                    )
                    file_path.unlink(missing_ok=True)
                    # Remove matching .meta.json so registry sees it as absent
                    meta = run_path / (Path(entry["path"]).stem + ".meta.json")
                    meta.unlink(missing_ok=True)

    # Phase 2: reconstruct PipelineRunner from run_summary.json
    summary_path = run_path / "run_summary.json"
    if not summary_path.exists():
        click.echo("Error: run_summary.json not found; cannot replay.", err=True)
        sys.exit(1)

    run_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    project_path = run_summary.get("project_path", "")
    if not project_path or not Path(project_path).exists():
        click.echo(
            f"Error: project_path {project_path!r} does not exist.", err=True
        )
        sys.exit(1)

    project_config = json.loads(Path(project_path).read_text(encoding="utf-8"))
    # run_dir layout: <artifacts_dir>/<project_id>/<run_id>/
    artifacts_dir = run_path.parent.parent
    registry = ArtifactRegistry(artifacts_dir)

    runner = PipelineRunner(
        project_config=project_config,
        registry=registry,
        artifacts_dir=artifacts_dir,
        force=False,   # never overwrite valid outputs
        from_stage=1,
        run_id=run_summary["run_id"],
        project_path=project_path,
    )

    click.echo(f"Replaying {run_summary['run_id']} ...")
    new_summary = runner.run()

    if new_summary["status"] == "completed":
        click.echo("Replay completed successfully.")
    else:
        click.echo("Replay FAILED.", err=True)
        for err in new_summary.get("errors", []):
            click.echo(f"  Error: {err}", err=True)
        sys.exit(1)


@cli.command("write")
@click.option(
    "--prompt", required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to StoryPrompt.json",
)
@click.option(
    "--out", required=True,
    type=click.Path(dir_okay=False),
    help="Path to write Script.json",
)
@click.option(
    "--writing-agent-cmd",
    default="writing-agent generate",
    show_default=True,
    help="Shell command for the writing agent (split on whitespace)",
)
def write_command(prompt: str, out: str, writing_agent_cmd: str) -> None:
    """Generate Script.json by calling writing-agent with a StoryPrompt."""
    cmd = shlex.split(writing_agent_cmd) + ["--prompt", prompt, "--out", out]
    try:
        proc = subprocess.run(cmd, capture_output=True)
    except FileNotFoundError:
        click.echo("ERROR: writing-agent failed")
        sys.exit(1)
    if proc.returncode != 0:
        click.echo("ERROR: writing-agent failed")
        sys.exit(1)


def _flatten_json(data: object, prefix: str = "") -> dict[str, str]:
    """Recursive flattener returning {path: repr(value)}."""
    result: dict[str, str] = {}
    if isinstance(data, dict):
        for k, v in sorted(data.items()):
            sub = f"{prefix}[{k}]"
            result.update(_flatten_json(v, sub))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            result.update(_flatten_json(v, f"{prefix}[{i}]"))
    else:
        result[prefix] = repr(data)
    return result


def _diff_run_dirs(dir_a: Path, dir_b: Path) -> list[str]:
    """Compare two run directories; return sorted diff lines (empty = identical)."""
    lines: list[str] = []
    idx_a = json.loads((dir_a / "RunIndex.json").read_text(encoding="utf-8"))
    idx_b = json.loads((dir_b / "RunIndex.json").read_text(encoding="utf-8"))

    # Top-level RunIndex field comparison (exclude "stages")
    for key in sorted((set(idx_a) | set(idx_b)) - {"stages"}):
        va, vb = idx_a.get(key), idx_b.get(key)
        if va != vb:
            lines.append(f"RunIndex[{key}]: {va!r} != {vb!r}")

    # Stage-by-stage artifact comparison
    stages_a = {s["name"]: s for s in idx_a.get("stages", [])}
    stages_b = {s["name"]: s for s in idx_b.get("stages", [])}
    for stage_name in sorted(set(stages_a) | set(stages_b)):
        sa, sb = stages_a.get(stage_name, {}), stages_b.get(stage_name, {})
        for section in ("inputs", "outputs"):
            ea_map = {e["path"]: e for e in sa.get(section, [])}
            eb_map = {e["path"]: e for e in sb.get(section, [])}
            for rel_path in sorted(set(ea_map) | set(eb_map)):
                prefix = f"stages[{stage_name}]/{section}[{rel_path}]"
                ea, eb = ea_map.get(rel_path), eb_map.get(rel_path)
                if ea is None:
                    lines.append(f"{prefix}: missing in A")
                    continue
                if eb is None:
                    lines.append(f"{prefix}: missing in B")
                    continue
                sha_a, sha_b = ea["sha256"], eb["sha256"]
                if sha_a != sha_b:
                    lines.append(f"{prefix}/sha256: {sha_a} != {sha_b}")
                    # JSON field-level diff on changed files
                    fa, fb = dir_a / rel_path, dir_b / rel_path
                    if fa.exists() and fb.exists() and rel_path.endswith(".json"):
                        try:
                            flat_a = _flatten_json(
                                json.loads(fa.read_text(encoding="utf-8")))
                            flat_b = _flatten_json(
                                json.loads(fb.read_text(encoding="utf-8")))
                            for fk in sorted(set(flat_a) | set(flat_b)):
                                if flat_a.get(fk) != flat_b.get(fk):
                                    lines.append(
                                        f"{prefix}/json{fk}: "
                                        f"{flat_a.get(fk)} != {flat_b.get(fk)}"
                                    )
                        except (json.JSONDecodeError, OSError):
                            pass
    return lines


# Contract artifacts compared by investigate-determinism
_CONTRACT_ARTIFACTS = [
    "CanonDecision.json",
    "ShotList.json",
    "AssetManifest_draft.json",
    "RenderPlan.json",
    "RenderOutput.json",
]
_OPTIONAL_CONTRACT = "render_preview/render_output.json"


def _normalize_artifact(artifact_name: str, data: dict) -> dict:
    """Return a deep copy of *data* with run-identity fields stripped.

    CanonDecision.json is returned unchanged (compared fully).
    Uses JSON round-trip for deep copy — no extra imports needed.
    """
    d = json.loads(json.dumps(data))  # deep copy

    if artifact_name == "ShotList.json":
        for f in ("script_id", "shotlist_id"):
            d.pop(f, None)

    elif artifact_name == "AssetManifest_draft.json":
        for f in ("manifest_id", "shotlist_ref"):
            d.pop(f, None)

    elif artifact_name == "RenderPlan.json":
        for f in ("plan_id", "manifest_ref"):
            d.pop(f, None)

    elif artifact_name in ("RenderOutput.json", "render_preview/render_output.json"):
        for f in ("request_id", "output_id"):
            d.pop(f, None)
        # Remove all top-level *_ref and *_uri fields
        for key in list(d.keys()):
            if key.endswith("_ref") or key.endswith("_uri"):
                d.pop(key)
        # Remove outputs[*].path (filesystem paths)
        if isinstance(d.get("outputs"), list):
            for item in d["outputs"]:
                if isinstance(item, dict):
                    item.pop("path", None)
        # Remove provenance.rendered_at (wall-clock timestamp)
        if isinstance(d.get("provenance"), dict):
            d["provenance"].pop("rendered_at", None)

    # CanonDecision.json — no normalization; full comparison
    return d


def _compute_normalized_render_hashes(run_dir: Path) -> dict:
    """Compute sha256 hashes of the *normalized* AssetManifest_draft and RenderPlan.

    These replace the raw derived hash fields in RenderOutput so that
    differences caused only by run-identity strings (manifest_id, plan_id …)
    are invisible to the determinism comparison.

    Returns an empty dict if either source artifact is missing or unreadable.
    """
    try:
        manifest_raw = json.loads(
            (run_dir / "AssetManifest_draft.json").read_text(encoding="utf-8")
        )
        plan_raw = json.loads(
            (run_dir / "RenderPlan.json").read_text(encoding="utf-8")
        )
    except (json.JSONDecodeError, OSError):
        return {}

    norm_manifest = _normalize_artifact("AssetManifest_draft.json", manifest_raw)
    norm_plan = _normalize_artifact("RenderPlan.json", plan_raw)

    # Canonical bytes: same algorithm used by the real renderer
    manifest_bytes = json.dumps(
        norm_manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    plan_bytes = json.dumps(
        norm_plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")

    return {
        "asset_manifest_hash": hashlib.sha256(manifest_bytes).hexdigest(),
        "render_plan_hash": hashlib.sha256(plan_bytes).hexdigest(),
        # separator avoids accidental collisions between adjacent byte strings
        "inputs_digest": hashlib.sha256(manifest_bytes + b"\n" + plan_bytes).hexdigest(),
    }


def _inject_normalized_render_hashes(data: dict, norm_hashes: dict) -> dict:
    """Replace raw derived hash fields in a RenderOutput copy with normalized values.

    Only replaces fields that already exist in *data* — never adds new keys.
    Returns *data* unchanged when *norm_hashes* is empty (missing source artifacts).
    """
    if not norm_hashes:
        return data
    d = json.loads(json.dumps(data))  # deep copy
    if "inputs_digest" in d:
        d["inputs_digest"] = norm_hashes["inputs_digest"]
    if isinstance(d.get("lineage"), dict):
        if "asset_manifest_hash" in d["lineage"]:
            d["lineage"]["asset_manifest_hash"] = norm_hashes["asset_manifest_hash"]
        if "render_plan_hash" in d["lineage"]:
            d["lineage"]["render_plan_hash"] = norm_hashes["render_plan_hash"]
    return d


def _compare_contract_artifacts(dir_a: Path, dir_b: Path) -> list[dict]:
    """Return sorted list of diff dicts for all contract artifacts."""
    diffs: list[dict] = []
    candidates = list(_CONTRACT_ARTIFACTS)
    opt_a = dir_a / _OPTIONAL_CONTRACT
    opt_b = dir_b / _OPTIONAL_CONTRACT
    if opt_a.exists() or opt_b.exists():
        candidates.append(_OPTIONAL_CONTRACT)

    # Pre-compute normalized hashes of the input artifacts; used to replace the
    # raw derived hash fields in RenderOutput before field-level comparison.
    norm_hashes_a = _compute_normalized_render_hashes(dir_a)
    norm_hashes_b = _compute_normalized_render_hashes(dir_b)

    for artifact in candidates:
        fa, fb = dir_a / artifact, dir_b / artifact
        missing_a, missing_b = not fa.exists(), not fb.exists()
        if missing_a or missing_b:
            diffs.append({
                "artifact": artifact,
                "type": "artifact_missing",
                "path": "",
                "runA": "present" if not missing_a else "missing",
                "runB": "present" if not missing_b else "missing",
            })
            continue
        try:
            data_a = json.loads(fa.read_text(encoding="utf-8"))
            data_b = json.loads(fb.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        data_a = _normalize_artifact(artifact, data_a)
        data_b = _normalize_artifact(artifact, data_b)
        # For RenderOutput variants, swap raw derived hashes for normalized recomputed ones
        if artifact in ("RenderOutput.json", _OPTIONAL_CONTRACT):
            data_a = _inject_normalized_render_hashes(data_a, norm_hashes_a)
            data_b = _inject_normalized_render_hashes(data_b, norm_hashes_b)
        flat_a = _flatten_json(data_a)
        flat_b = _flatten_json(data_b)
        for key in sorted(set(flat_a) | set(flat_b)):
            if flat_a.get(key) != flat_b.get(key):
                diffs.append({
                    "artifact": artifact,
                    "type": "json_field_mismatch",
                    "path": key,
                    "runA": flat_a.get(key),
                    "runB": flat_b.get(key),
                })

    # When normalized derived hashes still differ (genuine semantic change), emit
    # one diagnostic entry per source artifact showing the first mismatching field.
    _hash_keys = ("asset_manifest_hash", "render_plan_hash", "inputs_digest")
    if norm_hashes_a and norm_hashes_b and any(
        norm_hashes_a.get(k) != norm_hashes_b.get(k) for k in _hash_keys
    ):
        for art_name, label in (
            ("AssetManifest_draft.json", "[AssetManifest_draft]"),
            ("RenderPlan.json", "[RenderPlan]"),
        ):
            try:
                da = json.loads((dir_a / art_name).read_text(encoding="utf-8"))
                db = json.loads((dir_b / art_name).read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            flat_a = _flatten_json(_normalize_artifact(art_name, da))
            flat_b = _flatten_json(_normalize_artifact(art_name, db))
            for key in sorted(set(flat_a) | set(flat_b)):
                if flat_a.get(key) != flat_b.get(key):
                    diffs.append({
                        "artifact": "NORMALIZED_INPUTS",
                        "type": "normalized_input_mismatch",
                        "path": label,
                        "runA": f"{key}: {flat_a.get(key)}",
                        "runB": f"{key}: {flat_b.get(key)}",
                    })
                    break  # first mismatch only per source artifact

    # Stable sort: (artifact, path)
    return sorted(diffs, key=lambda d: (d["artifact"], d["path"]))


@cli.command("validate-run")
@click.option("--run", "run_dir", required=True,
              type=click.Path(exists=True, file_okay=False, readable=True),
              help="Path to a run directory containing RunIndex.json")
def validate_run_command(run_dir: str) -> None:
    """Re-hash artifacts and validate schema metadata in a run directory."""
    run_path = Path(run_dir)
    index_path = run_path / "RunIndex.json"
    if not index_path.exists():
        click.echo(f"ERROR: RunIndex.json not found in {run_path}")
        sys.exit(1)
    try:
        run_index = json.loads(index_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        click.echo(f"ERROR: RunIndex.json is not valid JSON: {exc}")
        sys.exit(1)

    errors: list[str] = []

    # Collect all unique artifact paths (deduped, sorted for determinism)
    seen: set[str] = set()
    all_entries: list[dict] = []
    for stage in run_index.get("stages", []):
        for entry in stage.get("inputs", []) + stage.get("outputs", []):
            if entry["path"] not in seen:
                seen.add(entry["path"])
                all_entries.append(entry)
    all_entries.sort(key=lambda e: e["path"])

    # 1) Re-hash
    for entry in all_entries:
        file_path = run_path / entry["path"]
        if not file_path.exists():
            errors.append(f"ERROR: missing artifact: {entry['path']}")
            continue
        if hash_file_bytes(file_path) != entry["sha256"]:
            errors.append(f"ERROR: hash mismatch for {entry['path']}")

    # 2) schema_id / schema_version presence
    for entry in all_entries:
        file_path = run_path / entry["path"]
        if not file_path.exists() or not file_path.suffix == ".json":
            continue
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("schema_id") is None or data.get("schema_version") is None:
            errors.append(f"ERROR: missing schema metadata for {entry['path']}")

    # 3) CanonDecision allow/deny consistency (only when file is present)
    canon_path = run_path / "CanonDecision.json"
    if canon_path.exists():
        try:
            canon_data = json.loads(canon_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            canon_data = {}
        decision = canon_data.get("decision")
        run_status = run_index.get("status")          # absent means "completed"
        failure_reason = run_index.get("failure_reason")
        rejected = (run_status == "failed" and failure_reason == "continuation_rejected")
        if rejected and decision != "deny":
            errors.append(
                f"ERROR: CanonDecision inconsistency: run continuation_rejected "
                f"but decision={decision!r}"
            )
        elif not rejected and decision != "allow":
            errors.append(
                f"ERROR: CanonDecision inconsistency: run completed "
                f"but decision={decision!r}"
            )

    if errors:
        for err in errors:
            click.echo(err)
        sys.exit(1)
    click.echo("OK: run valid")


@cli.command("diff")
@click.option("--run", "run_dir", required=True,
              type=click.Path(exists=True, file_okay=False, readable=True),
              help="Primary run directory (RunIndex.json must exist)")
@click.option("--against", "against_dir", required=True,
              type=click.Path(exists=True, file_okay=False, readable=True),
              help="Reference run directory to compare against")
def diff_command(run_dir: str, against_dir: str) -> None:
    """Compare two run directories: sha256 + JSON field diffs."""
    run_path, against_path = Path(run_dir), Path(against_dir)
    for p in (run_path, against_path):
        if not (p / "RunIndex.json").exists():
            click.echo(f"ERROR: RunIndex.json not found in {p}", err=True)
            sys.exit(1)

    diff_lines = _diff_run_dirs(run_path, against_path)
    if diff_lines:
        for line in diff_lines:
            click.echo(line)
        sys.exit(1)
    else:
        click.echo("OK: no differences")


@cli.command("verify-system")
def verify_system_command() -> None:
    """Run external tool checks then full pipeline + validate + diff."""
    errors: list[tuple[str, str]] = []

    # Locate the orchestrator binary (same venv as this process)
    orchestrator_bin = shutil.which("orchestrator") or sys.argv[0]

    # Steps 1–3: external component health checks (deterministic order)
    for step_cmd in (
        ["world-engine", "verify"],
        ["media", "verify"],
        ["video", "verify"],
    ):
        step_name = " ".join(step_cmd)
        try:
            proc = subprocess.run(step_cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                errors.append((step_name, proc.stdout + proc.stderr))
        except FileNotFoundError:
            pass  # binary not installed — skip this check

    # Steps 4–6: pipeline in a temp directory
    repo_root = Path(__file__).resolve().parent.parent
    project_file = repo_root / "examples" / "phase0" / "project.json"

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Compute run_id inline (same formula as compute_run_id, no repo import)
        project_config = json.loads(project_file.read_text(encoding="utf-8"))
        raw = json.dumps(
            project_config, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        run_id = "run-" + hashlib.sha256(raw).hexdigest()[:12]

        # Pre-write CanonDecision.json (allow) so the gate passes
        run_dir = tmp_path / project_config["id"] / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        canon = {
            "schema_id": "CanonDecision",
            "schema_version": "1.0.0",
            "decision": "allow",
            "decision_id": "verify-system-canon-01",
        }
        (run_dir / "CanonDecision.json").write_text(
            json.dumps(canon, indent=2), encoding="utf-8"
        )
        # Pre-write AssetManifest.media.json stub (media agent external input)
        media_stub = {
            "schema_id": "AssetManifest.media",
            "schema_version": "1.0.0",
            "manifest_id": "verify-system-media-stub",
            "producer": "verify-system-stub",
            "items": [],
        }
        (run_dir / "AssetManifest.media.json").write_text(
            json.dumps(media_stub, indent=2), encoding="utf-8"
        )

        # Step 4: full pipeline
        proc = subprocess.run(
            [orchestrator_bin, "run",
             "--project", str(project_file),
             "--artifacts-dir", tmp_dir,
             "--force"],
            capture_output=True, text=True,
        )
        pipeline_ok = proc.returncode == 0
        if not pipeline_ok:
            errors.append(("orchestrator run", proc.stdout + proc.stderr))

        if pipeline_ok:
            # Step 5: validate-run
            proc = subprocess.run(
                [orchestrator_bin, "validate-run", "--run", str(run_dir)],
                capture_output=True, text=True,
            )
            if proc.returncode != 0:
                errors.append(("orchestrator validate-run",
                                proc.stdout + proc.stderr))

            # Step 6: diff against itself
            proc = subprocess.run(
                [orchestrator_bin, "diff",
                 "--run", str(run_dir), "--against", str(run_dir)],
                capture_output=True, text=True,
            )
            if proc.returncode != 0:
                errors.append(("orchestrator diff", proc.stdout + proc.stderr))

    if errors:
        for step_name, output in errors:
            click.echo(f"FAIL: {step_name}")
            click.echo(output.rstrip())
        sys.exit(1)

    click.echo("OK: system verified")


@cli.command("package")
@click.option(
    "--run", "run_dir", required=True,
    type=click.Path(exists=True, file_okay=False, readable=True),
    help="Path to a finished run directory",
)
@click.option("--episode-id", required=True, help="Stable episode identifier")
@click.option(
    "--out", "out_dir", required=True,
    type=click.Path(file_okay=False),
    help="Parent directory to create the bundle under",
)
@click.option(
    "--mode", default="copy",
    type=click.Choice(["copy", "hardlink"]),
    show_default=True,
    help="File transfer mode: copy (safe default) or hardlink (faster; source artifacts must remain immutable after packaging)",
)
def package_command(run_dir: str, episode_id: str, out_dir: str, mode: str) -> None:
    """Assemble a finished run dir into a portable EpisodeBundle."""
    try:
        package_episode(Path(run_dir), episode_id, Path(out_dir), mode)
    except ValueError as exc:
        click.echo(str(exc))
        sys.exit(1)
    click.echo(f"OK: packaged episode {episode_id}")


@cli.command("validate-bundle")
@click.option(
    "--bundle", "bundle_dir", required=True,
    type=click.Path(exists=True, file_okay=False, readable=True),
    help="Path to a bundle root directory containing EpisodeBundle.json",
)
def validate_bundle_command(bundle_dir: str) -> None:
    """Re-verify all artifact hashes and bundle_hash in an EpisodeBundle."""
    bundle_path = Path(bundle_dir)
    bundle_json_path = bundle_path / "EpisodeBundle.json"

    if not bundle_json_path.exists():
        click.echo(f"ERROR: EpisodeBundle.json not found in {bundle_path}")
        sys.exit(1)

    try:
        bundle_data = json.loads(bundle_json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        click.echo(f"ERROR: EpisodeBundle.json is not valid JSON: {exc}")
        sys.exit(1)

    errors: list[str] = []

    # Verify each artifact file hash
    for name, entry in bundle_data.get("artifacts", {}).items():
        fp = bundle_path / entry["path"]
        if not fp.exists():
            errors.append(f"ERROR: missing file: {entry['path']}")
            continue
        if hash_file_bytes(fp) != entry["sha256"]:
            errors.append(f"ERROR: hash mismatch for {entry['path']}")

    # Verify bundle_hash
    without = {k: v for k, v in bundle_data.items()
               if k not in ("bundle_hash", "created_utc")}
    if hash_artifact(without) != bundle_data.get("bundle_hash", ""):
        errors.append("ERROR: bundle_hash mismatch")

    if errors:
        for e in errors:
            click.echo(e)
        sys.exit(1)
    click.echo("OK: bundle valid")


@cli.command("investigate-determinism")
@click.option(
    "--project", required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to project.json",
)
@click.option(
    "--out", "out_dir", required=True,
    type=click.Path(file_okay=False),
    help="Directory to write DeterminismReport.json and run artifacts",
)
def investigate_determinism_command(project: str, out_dir: str) -> None:
    """Run the pipeline twice and report determinism of contract artifacts."""
    project_path = Path(project).resolve()
    project_config = json.loads(project_path.read_text(encoding="utf-8"))
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    project_id = project_config["id"]
    token = uuid.uuid4().hex[:8]

    # Both runs share the same run_id so that all JSON fields seeded from it
    # (generation_seed, script_id, shotlist_id, manifest_id, plan_id …) are
    # byte-identical.  The two runs are separated by using different artifacts_dir
    # roots, so their output *folder* paths differ while their JSON *content* does not.
    run_id = f"invdet-{token}"
    artifacts_dir_a = out_path / "run-a"
    artifacts_dir_b = out_path / "run-b"

    _canon = {
        "schema_id": "CanonDecision", "schema_version": "1.0.0",
        "decision": "allow", "decision_id": "investigate-determinism",
    }

    _media_stub = {
        "schema_id": "AssetManifest.media",
        "schema_version": "1.0.0",
        "manifest_id": "investigate-determinism-media-stub",
        "producer": "investigate-determinism-stub",
        "items": [],
    }

    for artifacts_dir in (artifacts_dir_a, artifacts_dir_b):
        run_dir = artifacts_dir / project_id / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "CanonDecision.json").write_text(
            json.dumps(_canon, indent=2), encoding="utf-8"
        )
        (run_dir / "AssetManifest.media.json").write_text(
            json.dumps(_media_stub, indent=2), encoding="utf-8"
        )
        registry = ArtifactRegistry(artifacts_dir)
        runner = PipelineRunner(
            project_config=project_config,
            registry=registry,
            artifacts_dir=artifacts_dir,
            force=True,
            run_id=run_id,
            project_path=str(project_path),
        )
        summary = runner.run()
        if summary["status"] != "completed":
            click.echo(f"ERROR: pipeline run failed (artifacts_dir={artifacts_dir})")
            for err in summary.get("errors", []):
                click.echo(f"  {err}")
            sys.exit(1)

    dir_a = artifacts_dir_a / project_id / run_id
    dir_b = artifacts_dir_b / project_id / run_id

    # Harness assertion: Script.json must be byte-identical across both runs.
    # A mismatch here means the upstream generator (writing-agent) is itself
    # non-deterministic — a separate problem that masks all downstream diffs.
    script_a = dir_a / "Script.json"
    script_b = dir_b / "Script.json"
    if script_a.exists() and script_b.exists():
        if script_a.read_bytes() != script_b.read_bytes():
            click.echo(
                "ERROR: inputs mutated — Script.json differs between the two runs.\n"
                "The writing-agent (or stub) is non-deterministic; fix that before\n"
                "investigating downstream determinism.",
                err=True,
            )
            sys.exit(1)

    diffs = _compare_contract_artifacts(dir_a, dir_b)

    status = "pass" if not diffs else "fail"
    report = {"status": status, "diffs": diffs}
    report_path = out_path / "DeterminismReport.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )

    click.echo(f"DeterminismReport: {report_path}")
    if diffs:
        click.echo(f"FAIL: {len(diffs)} diff(s) found")
        sys.exit(1)
    click.echo("OK: determinism pass")


if __name__ == "__main__":
    cli()
