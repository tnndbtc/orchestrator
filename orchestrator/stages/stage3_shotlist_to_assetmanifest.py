"""Stage 3: Derive AssetManifest from ShotList + Script."""

from ..registry import ArtifactRegistry


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Build AssetManifest from ShotList and Script.

    Reads:  ShotList.json, Script.json
    Writes: AssetManifest.json

    Rules:
    - Unique characters from script dialogue → character_packs (is_placeholder=True)
    - One background per unique scene_id (from ShotList shot order)
    - One vo_item per dialogue action (license_type="generated_local")

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    shotlist = registry.read_artifact(project_id, run_id, "ShotList")
    script = registry.read_artifact(project_id, run_id, "Script")

    # Build scene_id → location lookup from script
    scene_locations: dict[str, str] = {
        scene["scene_id"]: scene.get("location", "UNKNOWN")
        for scene in script["scenes"]
    }

    # Collect unique characters and VO items from script
    characters: set[str] = set()
    vo_items: list[dict] = []
    for scene in script["scenes"]:
        scene_id = scene["scene_id"]
        for action in scene.get("actions", []):
            if action.get("type") == "dialogue":
                char = action.get("character", "UNKNOWN")
                characters.add(char)
                speaker_id = char.lower().replace(" ", "_")
                vo_items.append(
                    {
                        "item_id": f"vo-{scene_id}-{speaker_id}-{len(vo_items):03d}",
                        "speaker_id": speaker_id,
                        "text": action.get("text", ""),
                        "license_type": "generated_local",
                    }
                )

    character_packs: list[dict] = [
        {
            "pack_id": f"char-{char.lower().replace(' ', '_')}",
            "character_id": char.lower().replace(" ", "_"),
            "display_name": char,
            "is_placeholder": True,
        }
        for char in sorted(characters)
    ]

    # One background per unique scene (preserving first-seen order from shots)
    seen_scenes: set[str] = set()
    backgrounds: list[dict] = []
    for shot in shotlist["shots"]:
        scene_id = shot["scene_id"]
        if scene_id not in seen_scenes:
            seen_scenes.add(scene_id)
            backgrounds.append(
                {
                    "bg_id": f"bg-{scene_id}",
                    "scene_id": scene_id,
                    "description": scene_locations.get(scene_id, "UNKNOWN"),
                    "is_placeholder": True,
                }
            )

    manifest: dict = {
        "schema_version": "1.0.0",
        "manifest_id": f"manifest-{project_id}-{run_id[:8]}",
        "project_id": project_id,
        "shotlist_ref": shotlist["shotlist_id"],
        "character_packs": character_packs,
        "backgrounds": backgrounds,
        "vo_items": vo_items,
    }

    registry.write_artifact(
        project_id,
        run_id,
        "AssetManifest",
        manifest,
        parent_refs=[shotlist["shotlist_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage3_shotlist_to_assetmanifest",
        },
    )
    return manifest
