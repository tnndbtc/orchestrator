"""Stage 4: Build RenderPlan from AssetManifest + ShotList timing."""

from ..registry import ArtifactRegistry


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Resolve all assets to placeholder URIs and build RenderPlan.

    Reads:  AssetManifest.json, ShotList.json (for timing_lock_hash)
    Writes: RenderPlan.json

    Profile: preview_local, 1280x720, 24fps.
    All asset URIs use placeholder:// scheme (is_placeholder=True).

    Returns the artifact dict.
    """
    project_id = project_config["id"]
    manifest = registry.read_artifact(project_id, run_id, "AssetManifest")
    shotlist = registry.read_artifact(project_id, run_id, "ShotList")
    timing_lock_hash: str = shotlist["timing_lock_hash"]

    resolved_assets: list[dict] = []

    # Character pack assets
    for pack in manifest.get("character_packs", []):
        resolved_assets.append(
            {
                "asset_id": pack["pack_id"],
                "asset_type": "character_pack",
                "uri": f"placeholder://character/{pack['character_id']}",
                "license_type": "generated_local",
                "is_placeholder": True,
            }
        )

    # Background assets
    for bg in manifest.get("backgrounds", []):
        resolved_assets.append(
            {
                "asset_id": bg["bg_id"],
                "asset_type": "background",
                "uri": f"placeholder://background/{bg['scene_id']}",
                "license_type": "generated_local",
                "is_placeholder": True,
            }
        )

    # VO assets
    for vo in manifest.get("vo_items", []):
        resolved_assets.append(
            {
                "asset_id": vo["item_id"],
                "asset_type": "vo",
                "uri": f"placeholder://vo/{vo['item_id']}",
                "license_type": vo.get("license_type", "generated_local"),
                "is_placeholder": True,
            }
        )

    render_plan: dict = {
        "schema_version": "1.0.0",
        "plan_id": f"plan-{project_id}-{run_id[:8]}",
        "project_id": project_id,
        "manifest_ref": manifest["manifest_id"],
        "timing_lock_hash": timing_lock_hash,
        "profile": "preview_local",
        "resolution": "1280x720",
        "aspect_ratio": "16:9",
        "fps": 24,
        "resolved_assets": resolved_assets,
    }

    registry.write_artifact(
        project_id,
        run_id,
        "RenderPlan",
        render_plan,
        parent_refs=[manifest["manifest_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage4_build_renderplan",
        },
    )
    return render_plan
