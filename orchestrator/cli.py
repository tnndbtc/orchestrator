"""Click CLI entrypoint for the Orchestrator pipeline."""

import json
import sys
from pathlib import Path

import click

from .pipeline import PipelineRunner, compute_run_id
from .registry import ArtifactRegistry


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
