"""Stage 1: Generate a deterministic stub Script from project config."""

from ..registry import ArtifactRegistry


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Generate a 2-scene stub script from project_config fields.

    Reads:  nothing
    Writes: Script.json

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    title = project_config.get("title", "Untitled")
    genre = project_config.get("genre", "drama")

    script: dict = {
        "schema_version": "1.0.0",
        "script_id": f"script-{project_id}-{run_id[:8]}",
        "project_id": project_id,
        "title": title,
        "genre": genre,
        "scenes": [
            {
                "scene_id": "scene-001",
                "location": "INT. COMMAND CENTER",
                "time_of_day": "NIGHT",
                "actions": [
                    {
                        "type": "action",
                        "text": "The room hums with the glow of monitors.",
                    },
                    {
                        "type": "dialogue",
                        "character": "COMMANDER",
                        "text": "We have lost contact with the probe.",
                    },
                    {
                        "type": "dialogue",
                        "character": "ANALYST",
                        "text": "The signal disappeared twelve minutes ago.",
                    },
                ],
            },
            {
                "scene_id": "scene-002",
                "location": "EXT. LAUNCH PAD",
                "time_of_day": "DAWN",
                "actions": [
                    {
                        "type": "action",
                        "text": "A lone figure walks toward the rocket.",
                    },
                    {
                        "type": "dialogue",
                        "character": "COMMANDER",
                        "text": "Prepare for immediate launch.",
                    },
                ],
            },
        ],
    }

    registry.write_artifact(
        project_id,
        run_id,
        "Script",
        script,
        parent_refs=[],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage1_generate_script",
        },
    )
    return script
