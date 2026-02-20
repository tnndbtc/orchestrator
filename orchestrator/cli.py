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
