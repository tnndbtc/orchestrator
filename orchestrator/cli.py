"""Click CLI entrypoint for the Orchestrator pipeline."""

import json
import sys
from pathlib import Path

import click

from .pipeline import PipelineRunner
from .registry import ArtifactRegistry
from .utils.hashing import hash_file_bytes


@click.group()
def cli() -> None:
    """Orchestrator CLI — Phase 0 Workstream A."""


@cli.command("run")
@click.option(
    "--project",
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to project.json",
)
@click.option(
    "--artifacts-dir",
    default="./artifacts",
    show_default=True,
    help="Root directory for artifact storage",
)
@click.option(
    "--run-id",
    default=None,
    help="Explicit run ID (default: SHA-256 hash of project config)",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-run all eligible stages even if artifacts already exist",
)
@click.option(
    "--from-stage",
    default=1,
    type=click.IntRange(1, 5),
    show_default=True,
    help="Start execution from stage N (1–5); earlier stages are skipped",
)
def run_command(
    project: str,
    artifacts_dir: str,
    run_id: str | None,
    force: bool,
    from_stage: int,
) -> None:
    """Run the orchestrator pipeline for a project."""
    project_path = Path(project).resolve()
    project_config: dict = json.loads(project_path.read_text(encoding="utf-8"))

    registry = ArtifactRegistry(artifacts_dir)
    runner = PipelineRunner(
        project_config=project_config,
        registry=registry,
        artifacts_dir=artifacts_dir,
        force=force,
        from_stage=from_stage,
        run_id=run_id,
        project_path=str(project_path),
    )

    click.echo(f"▶  Run ID : {runner.run_id}")
    click.echo(f"   Project: {project_config.get('title', project_config['id'])}")
    if force:
        click.echo("   Mode   : force (all stages will re-run)")
    elif from_stage > 1:
        click.echo(f"   Mode   : from-stage {from_stage}")
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
        click.echo(
            f"    Artifacts: {Path(artifacts_dir).resolve() / project_config['id'] / runner.run_id}"
        )
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
