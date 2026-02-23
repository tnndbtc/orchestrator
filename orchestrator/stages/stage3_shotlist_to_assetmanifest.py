"""Stage 3: Derive AssetManifest_draft from ShotList."""

import json

from ..registry import ArtifactRegistry


def _to_slug(s: str) -> str:
    """Normalise an identifier to lowercase-hyphen form.

    Mirrors the normalisation applied by the media-agent's ``_normalize_id()``
    so that asset IDs are byte-identical in every artifact produced within a run
    (AssetManifest.json, AssetManifest.media.json, RenderPlan.json).
    """
    return s.strip().lower().replace(" ", "-").replace("_", "-")


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Build AssetManifest_draft from ShotList alone.

    Reads:  ShotList.json
    Writes: AssetManifest_draft.json

    All asset requirements are derived from the ShotList shots:
    - characters[].character_id  → character_packs (unique, sorted)
    - scene_id (first-seen order) + environment_notes → backgrounds
    - audio_intent.vo_speaker_id + vo_text → vo_items (only when both present)

    Script.json is NOT read.  This lets the orchestrator accept a ShotList
    produced directly by world-engine (--from-stage 3) without also requiring
    a Script artifact in the run directory.

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    shotlist = registry.read_artifact(project_id, run_id, "ShotList")

    seen_character_ids: dict[str, None] = {}   # ordered set (insertion order)
    seen_scenes: dict[str, str] = {}            # scene_id → description (first-seen)
    vo_items: list[dict] = []

    for shot in shotlist.get("shots", []):
        scene_id = shot["scene_id"]

        # Background — one entry per unique scene, preserve first-seen shot order
        if scene_id not in seen_scenes:
            seen_scenes[scene_id] = shot.get("environment_notes", "")

        # Characters — from per-shot character list
        for char in shot.get("characters", []):
            cid = char["character_id"]
            if cid not in seen_character_ids:
                seen_character_ids[cid] = None

        # VO item — only when both speaker and text are present
        intent = shot.get("audio_intent", {})
        speaker_id = intent.get("vo_speaker_id")
        vo_text = intent.get("vo_text")
        if speaker_id and vo_text:
            vo_items.append(
                {
                    "item_id": f"vo-{_to_slug(scene_id)}-{_to_slug(speaker_id)}-{len(vo_items):03d}",
                    "speaker_id": _to_slug(speaker_id),
                    "text": vo_text,
                    "license_type": "generated_local",
                }
            )

    character_packs: list[dict] = [
        {
            "asset_id": f"char-{_to_slug(cid)}",
            "pack_id": f"char-{_to_slug(cid)}",
            "character_id": _to_slug(cid),
            "display_name": cid,
            "license_type": "proprietary_cleared",
            "is_placeholder": True,
        }
        for cid in sorted(seen_character_ids)
    ]

    backgrounds: list[dict] = [
        {
            "asset_id": f"bg-{_to_slug(scene_id)}",
            "bg_id": f"bg-{_to_slug(scene_id)}",
            "scene_id": scene_id,
            "description": description,
            "license_type": "proprietary_cleared",
            "is_placeholder": True,
        }
        for scene_id, description in seen_scenes.items()
    ]

    manifest: dict = {
        "schema_id": "AssetManifest_draft",
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
        "AssetManifest_draft",
        manifest,
        parent_refs=[shotlist["shotlist_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage3_shotlist_to_assetmanifest",
        },
    )

    # Write AssetManifest.json as a bridge file for the media agent.
    # media verify reads $RUN_DIR/AssetManifest.json (not AssetManifest_draft.json).
    # The content is identical; only the schema_id is updated to match the filename stem.
    run_dir = registry.run_dir(project_id, run_id)
    bridge = {**manifest, "schema_id": "AssetManifest"}
    (run_dir / "AssetManifest.json").write_text(
        json.dumps(bridge, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    return manifest
