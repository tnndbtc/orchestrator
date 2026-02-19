"""Stage 5: Stub render — produces a RenderOutput with placeholder paths."""

from ..registry import ArtifactRegistry
from ..utils.hashing import hash_artifact


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Produce a placeholder RenderOutput.

    Reads:  RenderPlan.json, ShotList.json (to sum shot durations)
    Writes: RenderOutput.json

    video_path   = placeholder://video/<project_id>-<run_id>.mp4
    captions_path = placeholder://captions/<project_id>-<run_id>.srt
    content_hash  = hash_artifact({"captions_path": ..., "video_path": ...})
    duration_sec  = sum of all shot duration_sec from ShotList

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    render_plan = registry.read_artifact(project_id, run_id, "RenderPlan")
    shotlist = registry.read_artifact(project_id, run_id, "ShotList")

    total_duration_sec: float = sum(
        shot.get("duration_sec", 0.0) for shot in shotlist.get("shots", [])
    )

    video_path = f"placeholder://video/{project_id}-{run_id}.mp4"
    captions_path = f"placeholder://captions/{project_id}-{run_id}.srt"
    content_hash = hash_artifact(
        {"captions_path": captions_path, "video_path": video_path}
    )

    render_output: dict = {
        "schema_version": "1.0.0",
        "output_id": f"output-{project_id}-{run_id[:8]}",
        "project_id": project_id,
        "plan_ref": render_plan["plan_id"],
        "video_path": video_path,
        "captions_path": captions_path,
        "content_hash": content_hash,
        "duration_sec": total_duration_sec,
    }

    registry.write_artifact(
        project_id,
        run_id,
        "RenderOutput",
        render_output,
        parent_refs=[render_plan["plan_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage5_render_preview",
        },
    )
    return render_output
