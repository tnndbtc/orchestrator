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
    - camera_movement = "STATIC" for all shots
    - audio_intent populated from first dialogue action in scene
    - timing_lock_hash = hash_artifact({"shots": [{shot_id, duration_sec}, ...]})
    - total_duration_sec = sum of all shot duration_sec values

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    script = registry.read_artifact(project_id, run_id, "Script")

    shots: list[dict] = []
    for scene in script["scenes"]:
        scene_id = scene["scene_id"]
        actions = scene.get("actions", [])

        # Sum word counts of all dialogue actions in this scene
        dialogue_word_count = sum(
            len(a["text"].split()) for a in actions if a.get("type") == "dialogue"
        )
        duration_sec = max(3.0, dialogue_word_count * 0.4)

        # First dialogue action in scene (for audio / character stub)
        first_dialogue = next((a for a in actions if a.get("type") == "dialogue"), None)
        speaker_id = (
            first_dialogue["character"].lower().replace(" ", "_")
            if first_dialogue else None
        )
        audio_intent = {
            "vo_speaker_id": speaker_id,
            "vo_text": first_dialogue.get("text") if first_dialogue else None,
            "sfx_tags": [],
            "music_mood": None,
        }
        characters = (
            [{"character_id": speaker_id, "expression": None, "pose": None}]
            if speaker_id else []
        )

        for framing in ("wide", "medium_close_up"):
            idx = "001" if framing == "wide" else "002"
            shots.append(
                {
                    "shot_id": f"{scene_id}-shot-{idx}",
                    "scene_id": scene_id,
                    "duration_sec": duration_sec,
                    "camera_framing": framing,
                    "camera_movement": "STATIC",
                    "audio_intent": audio_intent,
                    "characters": characters,
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
    total_duration_sec = sum(s["duration_sec"] for s in shots)

    shotlist: dict = {
        "schema_id": "ShotList",
        "schema_version": "1.0.0",
        "shotlist_id": f"shotlist-{project_id}-{run_id[:8]}",
        "script_id": script["script_id"],
        "created_at": "1970-01-01T00:00:00Z",
        "timing_lock_hash": timing_lock_hash,
        "total_duration_sec": total_duration_sec,
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
