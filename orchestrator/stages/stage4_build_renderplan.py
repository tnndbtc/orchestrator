"""Stage 4: Merge AssetManifest_draft + AssetManifest.media → AssetManifest_final, build RenderPlan."""

import json
import os

from ..registry import ArtifactRegistry
from ..utils.agent_bin import call_agent
from ..validator import validate_artifact


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Merge draft + media manifests and build RenderPlan.

    Reads:  AssetManifest_draft.json  (envelope metadata: project_id, shotlist_ref, manifest_id)
            AssetManifest.media.json  (required — resolved asset list; fails with clear error if absent)
            ShotList.json             (for timing_lock_hash)
    Writes: AssetManifest_final.json  (merge of draft envelope + media items, side output)
            RenderPlan.json           (primary output)

    Returns the RenderPlan artifact dict.
    """
    project_id = project_config["id"]
    run_dir = registry.run_dir(project_id, run_id)

    # 1. Read AssetManifest_draft.json for envelope metadata
    draft = registry.read_artifact(project_id, run_id, "AssetManifest_draft")

    # 2. Read AssetManifest.media.json — auto-call media agent if absent
    media_path = run_dir / "AssetManifest.media.json"
    if not media_path.exists():
        try:
            result = call_agent(
                "media",
                ["verify"],
                capture_output=True,
                text=True,
                env={**os.environ, "RUN_DIR": str(run_dir)},
            )
            # media verify --strict fails for placeholder assets (expected in dev/test);
            # we only hard-fail if the output file was never written at all.
            if result.returncode != 0 and not media_path.exists():
                raise RuntimeError(
                    f"media agent failed (exit {result.returncode}) "
                    f"and produced no output.\n"
                    f"stdout: {result.stdout.strip()}\n"
                    f"stderr: {result.stderr.strip()}"
                )
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                "media agent not found. Install it in the same environment:\n"
                "  pip install -e /path/to/media-agent\n"
                f"Original error: {exc}"
            ) from exc

    if not media_path.exists():
        raise FileNotFoundError(
            "ERROR: AssetManifest.media.json not found in run directory.\n"
            "  Install and run the media agent, or place the file manually:\n"
            f"  {run_dir}/AssetManifest.media.json\n"
            "  Then resume: orchestrator run --project <p> --from-stage 4"
        )
    media = json.loads(media_path.read_text(encoding="utf-8"))

    # 3. Validate AssetManifest.media against its schema
    validate_artifact(media, "AssetManifest.media")

    # 4. Read ShotList for timing_lock_hash
    shotlist = registry.read_artifact(project_id, run_id, "ShotList")
    timing_lock_hash: str = shotlist["timing_lock_hash"]

    # 5. Build AssetManifest_final: draft envelope + media items
    asset_manifest_final: dict = {
        "schema_id": "AssetManifest_final",
        "schema_version": "1.0.0",
        "manifest_id": draft["manifest_id"],
        "project_id": draft["project_id"],
        "shotlist_ref": draft["shotlist_ref"],
        "items": media.get("items", []),
    }

    # 6. Write AssetManifest_final.json via registry (validates + writes meta)
    registry.write_artifact(
        project_id,
        run_id,
        "AssetManifest_final",
        asset_manifest_final,
        parent_refs=[draft["manifest_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage4_build_renderplan",
        },
    )

    # 7. Build RenderPlan.resolved_assets by mapping each item in AssetManifest_final
    resolved_assets: list[dict] = []
    for item in asset_manifest_final.get("items", []):
        resolved_assets.append(
            {
                "asset_id": item["asset_id"],
                "asset_type": item["asset_type"],
                "uri": item["uri"],
                "license_type": item["metadata"]["license_type"],
                "is_placeholder": item["is_placeholder"],
            }
        )

    render_plan: dict = {
        "schema_id": "RenderPlan",
        "schema_version": "1.0.0",
        "plan_id": f"plan-{project_id}-{run_id[:8]}",
        "project_id": project_id,
        "manifest_ref": asset_manifest_final["manifest_id"],
        "timing_lock_hash": timing_lock_hash,
        "profile": "preview_local",
        "resolution": "1280x720",
        "aspect_ratio": "16:9",
        "fps": 24,
        "resolved_assets": resolved_assets,
    }

    # 8. Write RenderPlan.json (primary output)
    registry.write_artifact(
        project_id,
        run_id,
        "RenderPlan",
        render_plan,
        parent_refs=[asset_manifest_final["manifest_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage4_build_renderplan",
        },
    )
    return render_plan
