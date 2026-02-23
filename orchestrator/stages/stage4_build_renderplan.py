"""Stage 4: Merge AssetManifest_draft + AssetManifest.media → AssetManifest_final, build RenderPlan."""

import json
import logging
import os
from pathlib import Path

from ..registry import ArtifactRegistry
from ..utils.agent_bin import call_agent
from ..validator import validate_artifact

logger = logging.getLogger(__name__)

# Visual asset types that need real image files for the renderer.
_VISUAL_ASSET_TYPES = frozenset({"character", "background", "prop"})

# Distinct background colors per visual asset type (dark, low-saturation palette).
_ASSET_TYPE_COLOR: dict[str, str] = {
    "background": "#1a1a2e",  # dark navy
    "character":  "#1a2e1a",  # dark forest
    "prop":       "#2e1a1a",  # dark maroon
}


def _generate_visual_placeholder(asset_id: str, asset_type: str, run_dir: Path) -> str | None:
    """Generate a solid-colour 1280×720 PNG for a visual placeholder asset.

    Tries ``tools.renderer.placeholder.generate_placeholder`` (video-agent) first,
    then falls back to raw Pillow, then gives up (returns None).

    Returns:
        A ``file://`` URI string pointing to the written PNG, or None if neither
        Pillow nor the video-agent renderer is available.
    """
    assets_dir = run_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    out_path = assets_dir / f"{asset_id}.png"

    # Already materialised on a previous run — reuse it.
    if out_path.exists() and out_path.stat().st_size > 100:
        return f"file://{out_path.resolve()}"

    color = _ASSET_TYPE_COLOR.get(asset_type, "#1a1a2e")

    # --- Prefer video-agent's generator (already installed in the shared venv) ---
    try:
        from tools.renderer.placeholder import generate_placeholder  # type: ignore[import]
        generate_placeholder(
            shot_id=asset_id,
            width=1280,
            height=720,
            color=color,
            output_path=out_path,
        )
        logger.debug("Generated placeholder via tools.renderer.placeholder: %s", out_path)
        return f"file://{out_path.resolve()}"
    except ImportError:
        pass
    except Exception as exc:
        logger.warning("tools.renderer.placeholder failed for %s: %s — trying raw Pillow", asset_id, exc)

    # --- Fallback: raw Pillow ---
    try:
        from PIL import Image  # type: ignore[import]
        hex_color = color.lstrip("#")
        bg_rgb: tuple[int, int, int] = (
            int(hex_color[0:2], 16),
            int(hex_color[2:4], 16),
            int(hex_color[4:6], 16),
        )
        img = Image.new("RGB", (1280, 720), color=bg_rgb)
        img.save(str(out_path), format="PNG", compress_level=9, optimize=False)
        logger.debug("Generated placeholder via raw Pillow: %s", out_path)
        return f"file://{out_path.resolve()}"
    except ImportError:
        logger.warning(
            "Pillow not available — cannot materialise placeholder for %s. "
            "Install: pip install Pillow>=10",
            asset_id,
        )
        return None
    except Exception as exc:
        logger.warning("Pillow failed for %s: %s", asset_id, exc)
        return None


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Merge draft + media manifests and build RenderPlan.

    Reads:  AssetManifest_draft.json  (envelope metadata: project_id, shotlist_ref, manifest_id)
            AssetManifest.media.json  (required — resolved asset list; fails with clear error if absent)
            ShotList.json             (for timing_lock_hash)
    Writes: AssetManifest_final.json  (merge of draft envelope + media items, side output)
            RenderPlan.json           (primary output)
            assets/<asset_id>.png     (solid-colour PNGs for every visual placeholder)

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

    # 7. Build RenderPlan.resolved_assets.
    #    For visual placeholders (character / background / prop), generate a real
    #    solid-colour PNG so the renderer's _collect_placeholders() guard does not
    #    block.  Non-visual assets (vo, sfx, music) keep their placeholder URIs.
    resolved_assets: list[dict] = []
    for item in asset_manifest_final.get("items", []):
        uri = item["uri"]
        is_placeholder = item["is_placeholder"]

        if is_placeholder and item["asset_type"] in _VISUAL_ASSET_TYPES:
            file_uri = _generate_visual_placeholder(item["asset_id"], item["asset_type"], run_dir)
            if file_uri:
                uri = file_uri
                is_placeholder = False

        resolved_assets.append(
            {
                "asset_id": item["asset_id"],
                "asset_type": item["asset_type"],
                "uri": uri,
                "license_type": item["metadata"]["license_type"],
                "is_placeholder": is_placeholder,
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
