"""Stage 2: Convert Script to ShotList (2 shots per scene, duration from word count)."""

from ..registry import ArtifactRegistry
from ..utils.hashing import hash_artifact


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Derive ShotList from Script.

    Reads:  Script.json
    Writes: ShotList.json

    Shot rules:
    - 2 shots per scene (wide + medium_close_up)
    - duration_sec = max(3.0, total_dialogue_words_in_scene * 0.4)
    - timing_lock_hash = hash_artifact({"shots": [{shot_id, duration_sec}, ...]})

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    script = registry.read_artifact(project_id, run_id, "Script")

    shots: list[dict] = []
    for scene in script["scenes"]:
        scene_id = scene["scene_id"]
        # Sum word counts of all dialogue actions in this scene
        dialogue_word_count = sum(
            len(action["text"].split())
            for action in scene.get("actions", [])
            if action.get("type") == "dialogue"
        )
        duration_sec = max(3.0, dialogue_word_count * 0.4)

        shots.append(
            {
                "shot_id": f"{scene_id}-shot-001",
                "scene_id": scene_id,
                "duration_sec": duration_sec,
                "camera_framing": "wide",
            }
        )
        shots.append(
            {
                "shot_id": f"{scene_id}-shot-002",
                "scene_id": scene_id,
                "duration_sec": duration_sec,
                "camera_framing": "medium_close_up",
            }
        )

    timing_lock_hash = hash_artifact(
        {
            "shots": [
                {"shot_id": s["shot_id"], "duration_sec": s["duration_sec"]}
                for s in shots
            ]
        }
    )

    shotlist: dict = {
        "schema_version": "1.0.0",
        "shotlist_id": f"shotlist-{project_id}-{run_id[:8]}",
        "project_id": project_id,
        "script_ref": script["script_id"],
        "timing_lock_hash": timing_lock_hash,
        "shots": shots,
    }

    registry.write_artifact(
        project_id,
        run_id,
        "ShotList",
        shotlist,
        parent_refs=[script["script_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage2_script_to_shotlist",
        },
    )
    return shotlist
