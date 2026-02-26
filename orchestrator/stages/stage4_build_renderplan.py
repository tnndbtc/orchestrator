"""Stage 4: Merge AssetManifest_draft + AssetManifest.media → AssetManifest_final, build RenderPlan.

Locale-aware mode
-----------------
When ``AssetManifest_draft.{locale}.json`` siblings are found in the run directory
(e.g. ``AssetManifest_draft.zh-Hans.json``), Stage 4 switches to multi-locale mode:

* **Shared assets** (character, background, prop, sfx, music) are read from the
  base ``AssetManifest.media.json`` and written to ``resolved_assets`` in RenderPlan.
* **VO assets** are locale-specific: each locale's VO is read from
  ``AssetManifest.media.{locale}.json`` and placed in ``RenderPlan.locale_tracks``.
* A separate ``AssetManifest_final.{locale}.json`` is written for every locale
  (shared non-VO items + that locale's VO items).  The base ``AssetManifest_final.json``
  captures the base media in full (for traceability).

Single-locale mode (backward-compatible)
-----------------------------------------
When no locale variants exist, all items land in ``resolved_assets`` exactly as
before and no ``locale_tracks`` key is emitted.
"""

import json
import logging
from pathlib import Path

from ..registry import ArtifactRegistry
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

# Mirrors render_from_orchestrator._MIN_REAL_ASSET_BYTES.  Files at or below this
# size are considered stubs (empty placeholders) and replaced with Pillow-generated
# solid-colour PNGs so the renderer's own stub guard never fires.
_MIN_REAL_ASSET_BYTES = 100


def _is_stub_uri(uri: str) -> bool:
    """Return True when *uri* is a ``file://`` that points to a stub (≤ _MIN_REAL_ASSET_BYTES).

    Mirrors ``render_from_orchestrator._is_stub_file``.  Media-agent's local
    resolver can return minimal 1×1 test-fixture PNGs (≈ 69 bytes) that are
    technically ``is_placeholder=False`` yet too small for the renderer to accept.
    Stage 4 replaces them pre-emptively so the renderer never encounters a stub.
    Does not raise — returns False on any OS error.
    """
    if not uri or not uri.startswith("file://"):
        return False
    try:
        return Path(uri[len("file://"):]).stat().st_size <= _MIN_REAL_ASSET_BYTES
    except OSError:
        return False


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


# ---------------------------------------------------------------------------
# Locale helpers
# ---------------------------------------------------------------------------

def _discover_locales(run_dir: Path) -> list[str]:
    """Scan *run_dir* for ``AssetManifest_draft.{locale}.json`` siblings.

    Returns a sorted list of locale strings, e.g. ``["zh-Hans"]``.
    The base file (no locale suffix) is never included.
    """
    locales: list[str] = []
    prefix = "AssetManifest_draft."
    for path in sorted(run_dir.glob("AssetManifest_draft.*.json")):
        stem = path.stem  # e.g. "AssetManifest_draft.zh-Hans"
        if stem.startswith(prefix):
            locale = stem[len(prefix):]
            if locale:
                locales.append(locale)
    return locales


def _read_locale_draft(run_dir: Path, locale: str | None) -> dict:
    """Read the draft manifest for *locale* (or base when locale is None)."""
    path = (
        run_dir / f"AssetManifest_draft.{locale}.json"
        if locale
        else run_dir / "AssetManifest_draft.json"
    )
    return json.loads(path.read_text(encoding="utf-8"))


def _read_locale_media(run_dir: Path, locale: str | None) -> dict | None:
    """Read and validate the media manifest for *locale* (or base when locale is None).

    Returns None when the file is absent (caller decides how to handle).
    """
    path = (
        run_dir / f"AssetManifest.media.{locale}.json"
        if locale
        else run_dir / "AssetManifest.media.json"
    )
    if not path.exists():
        return None
    media = json.loads(path.read_text(encoding="utf-8"))
    validate_artifact(media, "AssetManifest.media")
    return media


def _process_items(media: dict, run_dir: Path) -> list[dict]:
    """Replace visual stub/placeholder URIs with Pillow-generated solid-colour PNGs.

    Non-visual assets (vo, sfx, music) pass through unchanged.
    Returns a new list; original media dict is not mutated.
    """
    final_items: list[dict] = []
    for item in media.get("items", []):
        uri = item["uri"]
        is_placeholder = item["is_placeholder"]

        if (is_placeholder or _is_stub_uri(uri)) and item["asset_type"] in _VISUAL_ASSET_TYPES:
            file_uri = _generate_visual_placeholder(item["asset_id"], item["asset_type"], run_dir)
            if file_uri:
                item = {**item, "uri": file_uri, "is_placeholder": False}

        final_items.append(item)
    return final_items


def _build_asset_manifest_final(draft: dict, items: list[dict], locale: str | None = None) -> dict:
    """Construct an AssetManifest_final dict from draft envelope + processed items.

    When *locale* is provided, ``.{locale}`` is appended to ``manifest_id`` so
    each locale variant has a unique, traceable identifier.
    """
    manifest_id = draft["manifest_id"]
    if locale:
        manifest_id = f"{manifest_id}.{locale}"
    return {
        "schema_id": "AssetManifest_final",
        "schema_version": "1.0.0",
        "manifest_id": manifest_id,
        "project_id": draft["project_id"],
        "shotlist_ref": draft["shotlist_ref"],
        "items": items,
    }


def _project_asset(item: dict) -> dict:
    """Project a full resolved-asset item to the compact RenderPlan asset shape."""
    return {
        "asset_id":     item["asset_id"],
        "asset_type":   item["asset_type"],
        "uri":          item["uri"],
        "license_type": item["metadata"]["license_type"],
        "is_placeholder": item["is_placeholder"],
    }


# ---------------------------------------------------------------------------
# Main stage entry point
# ---------------------------------------------------------------------------

def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Merge draft + media manifests and build RenderPlan.

    Single-locale mode (no ``AssetManifest_draft.*.json`` siblings):
        Reads:  AssetManifest_draft.json
                AssetManifest.media.json
                ShotList.json
        Writes: AssetManifest_final.json
                RenderPlan.json  (``resolved_assets`` contains all items)

    Multi-locale mode (locale siblings detected):
        Reads:  AssetManifest_draft.json  + AssetManifest_draft.{locale}.json …
                AssetManifest.media.json  + AssetManifest.media.{locale}.json …
                ShotList.json
        Writes: AssetManifest_final.json               (base — full traceability)
                AssetManifest_final.{locale}.json …    (shared non-VO + locale VO)
                RenderPlan.json  (``resolved_assets`` = shared non-VO;
                                  ``locale_tracks``   = VO per locale)

    Returns the RenderPlan artifact dict.
    """
    project_id = project_config["id"]
    run_dir = registry.run_dir(project_id, run_id)

    # Determine which media file is the "base" for this run.
    # When called from `orchestrator run --media`, the CLI passes the actual
    # filename so we use it directly instead of assuming the neutral base name.
    # Legacy / PipelineRunner callers omit this key → default to base name.
    base_media_name: str = project_config.get("_media_file", "AssetManifest.media.json")

    # Extract locale embedded in the base media filename, if any.
    # "AssetManifest.media.json"         → None   (neutral base — discover locale variants)
    # "AssetManifest.media.zh-Hans.json" → "zh-Hans" (single-locale run, no variants)
    _bm_stem = Path(base_media_name).stem  # strips .json
    _bm_after = (
        _bm_stem[len("AssetManifest.media."):]
        if _bm_stem.startswith("AssetManifest.media.")
        else ""
    )
    base_media_locale: str | None = _bm_after if _bm_after else None

    # 1. Read base AssetManifest_draft for envelope metadata.
    #    When the base media is locale-specific, prefer the matching locale draft.
    if base_media_locale:
        locale_draft_path = run_dir / f"AssetManifest_draft.{base_media_locale}.json"
        fallback_draft_path = run_dir / "AssetManifest_draft.json"
        _draft_path = locale_draft_path if locale_draft_path.exists() else fallback_draft_path
        draft = json.loads(_draft_path.read_text(encoding="utf-8"))
    else:
        draft = registry.read_artifact(project_id, run_id, "AssetManifest_draft")

    # 2. Discover locale variants — only when the base media is the neutral base.
    #    When the base IS locale-specific (e.g. zh-Hans only), there are no variants.
    locales = [] if base_media_locale else _discover_locales(run_dir)

    # 3. Read ShotList for timing_lock_hash
    shotlist = registry.read_artifact(project_id, run_id, "ShotList")
    timing_lock_hash: str = shotlist["timing_lock_hash"]

    # 4. Read base AssetManifest.media.json — must already exist.
    #    The media agent's job is done before this stage runs; stage4 never
    #    calls the media agent itself.
    media_path = run_dir / base_media_name
    if not media_path.exists():
        raise FileNotFoundError(
            f"ERROR: {base_media_name} not found in run directory.\n"
            f"  Expected: {media_path}\n"
            "  Run the media agent first, then:\n"
            "    orchestrator run --media path/to/AssetManifest.media[.locale].json"
        )

    base_media = _read_locale_media(run_dir, base_media_locale)
    assert base_media is not None  # guaranteed by the existence check above

    # 5. Process base media items (stub/placeholder replacement for visual assets)
    base_final_items = _process_items(base_media, run_dir)

    # 6. Write AssetManifest_final.json (base — full item list for traceability)
    base_final = _build_asset_manifest_final(draft, base_final_items, locale=None)
    registry.write_artifact(
        project_id,
        run_id,
        "AssetManifest_final",
        base_final,
        parent_refs=[draft["manifest_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage4_build_renderplan",
        },
    )

    # -----------------------------------------------------------------------
    # Multi-locale path
    # -----------------------------------------------------------------------
    if locales:
        # Shared assets: non-VO items from base media (same across all locales)
        non_vo_items = [item for item in base_final_items if item["asset_type"] != "vo"]
        resolved_assets = [_project_asset(item) for item in non_vo_items]

        # locale_tracks: VO per locale (including base as "default")
        locale_tracks: dict[str, dict] = {}

        base_vo = [item for item in base_final_items if item["asset_type"] == "vo"]
        if base_vo:
            locale_tracks["default"] = {
                "vo_assets": [_project_asset(item) for item in base_vo],
            }

        for locale in locales:
            locale_draft = _read_locale_draft(run_dir, locale)
            locale_media = _read_locale_media(run_dir, locale)
            if locale_media is None:
                logger.warning(
                    "AssetManifest.media.%s.json not found — skipping locale %s", locale, locale
                )
                continue

            locale_final_items = _process_items(locale_media, run_dir)

            # Write AssetManifest_final.{locale}.json directly (not via registry,
            # since registry enforces stem == schema_id and locale suffix breaks that)
            locale_final = _build_asset_manifest_final(locale_draft, locale_final_items, locale=locale)
            validate_artifact(locale_final, "AssetManifest_final")
            locale_final_path = run_dir / f"AssetManifest_final.{locale}.json"
            locale_final_path.write_text(
                json.dumps(locale_final, indent=2, sort_keys=True), encoding="utf-8"
            )
            logger.info("Wrote %s", locale_final_path.name)

            locale_vo = [item for item in locale_final_items if item["asset_type"] == "vo"]
            locale_tracks[locale] = {
                "vo_assets": [_project_asset(item) for item in locale_vo],
            }

        render_plan: dict = {
            "schema_id": "RenderPlan",
            "schema_version": "1.0.0",
            "plan_id": f"plan-{project_id}-{run_id[:8]}",
            "project_id": project_id,
            "manifest_ref": base_final["manifest_id"],
            "timing_lock_hash": timing_lock_hash,
            "profile": "preview_local",
            "resolution": "1280x720",
            "aspect_ratio": "16:9",
            "fps": 24,
            "resolved_assets": resolved_assets,
            "locale_tracks": locale_tracks,
        }

    # -----------------------------------------------------------------------
    # Single-locale path (backward-compatible)
    # -----------------------------------------------------------------------
    else:
        resolved_assets = [_project_asset(item) for item in base_final_items]
        render_plan = {
            "schema_id": "RenderPlan",
            "schema_version": "1.0.0",
            "plan_id": f"plan-{project_id}-{run_id[:8]}",
            "project_id": project_id,
            "manifest_ref": base_final["manifest_id"],
            "timing_lock_hash": timing_lock_hash,
            "profile": "preview_local",
            "resolution": "1280x720",
            "aspect_ratio": "16:9",
            "fps": 24,
            "resolved_assets": resolved_assets,
        }

    # 9. Write RenderPlan.json (primary output)
    registry.write_artifact(
        project_id,
        run_id,
        "RenderPlan",
        render_plan,
        parent_refs=[base_final["manifest_id"]],
        creation_params={
            "project_id": project_id,
            "run_id": run_id,
            "stage": "stage4_build_renderplan",
        },
    )
    return render_plan
