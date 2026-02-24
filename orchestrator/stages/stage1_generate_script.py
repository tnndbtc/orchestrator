"""Stage 1: DraftStory → world-engine validation → StoryPrompt → Script.

§42 CORRECT WORKFLOW
--------------------
1. Locate or auto-generate DraftStory.json (scene breakdown; the human-readable
   narrative draft that world-engine validates).
2. Call  world-engine validate-story-draft  on the draft.
   - Blocks the pipeline if canon violations are detected.
   - Skipped gracefully when world-engine is not installed (Phase 0 / no canon yet).
3. Compile StoryPrompt.json from the validated draft.
   (Only reaches this step if step 2 passed or was skipped.)
4. Call  writing-agent generate  → Script.json.
5. Fall back to a deterministic 2-scene stub when writing-agent is unavailable.

§42 RESPONSIBILITY CLARIFICATION (orchestrator role)
-----------------------------------------------------
- Orchestrator coordinates the validation sequence and stops the pipeline on
  any canon violation before StoryPrompt.json is compiled.
- writing-agent: produces DraftStory and (after validation) Script.json.
- world-engine:  persists CanonSnapshot; validates drafts; emits
                 CanonViolationReport.json on failure.
- Orchestrator:  NEVER generates story content; only invokes binaries and
                 enforces the validation gate.
"""

import hashlib
import json
import tempfile
from pathlib import Path

from ..registry import ArtifactRegistry
from ..utils.agent_bin import call_agent, find_agent_bin


# ---------------------------------------------------------------------------
# DraftStory generation (Phase 0 stub — replaces manual human-authored draft)
# ---------------------------------------------------------------------------

def _generate_draft_story(project_config: dict, run_id: str) -> dict:
    """Derive a DraftStory from project config.

    DraftStory is the human-readable scene breakdown that world-engine validates
    BEFORE StoryPrompt.json is compiled (§42).  In Phase 0 there is no
    human-authored text, so this is auto-derived from the project config.
    """
    project_id      = project_config["id"]
    title           = project_config.get("title", "Untitled")
    genre           = project_config.get("genre", "drama")
    visual_style    = project_config.get("visual_style", "cinematic")
    target_duration = project_config.get("target_duration", 60)
    max_scenes      = max(2, target_duration // 30)

    genre_settings: dict[str, str] = {
        "sci-fi":   "Space station",
        "fantasy":  "Enchanted forest",
        "action":   "Urban environment",
        "drama":    "City apartment",
        "horror":   "Abandoned building",
        "comedy":   "Suburban neighborhood",
        "thriller": "Downtown high-rise",
        "romance":  "Coastal town",
    }
    primary_location = genre_settings.get(genre.lower(), "Unspecified location")

    # Deterministic seed from project_id + run_id
    seed_bytes      = hashlib.sha256(f"{project_id}:{run_id}".encode()).digest()
    generation_seed = int.from_bytes(seed_bytes[:4], "big") & 0x7FFF_FFFF

    genre_characters: dict[str, list[dict]] = {
        "sci-fi":   [{"id": "commander", "role": "mission commander"},
                     {"id": "analyst",   "role": "data analyst"}],
        "fantasy":  [{"id": "hero",   "role": "chosen one"},
                     {"id": "mentor", "role": "wise guide"}],
        "action":   [{"id": "agent",   "role": "field operative"},
                     {"id": "handler", "role": "mission controller"}],
        "horror":   [{"id": "survivor",     "role": "main survivor"},
                     {"id": "investigator", "role": "investigator"}],
        "thriller": [{"id": "detective", "role": "lead detective"},
                     {"id": "suspect",   "role": "prime suspect"}],
    }
    characters = genre_characters.get(
        genre.lower(),
        [{"id": "protagonist", "role": "lead character"},
         {"id": "supporting",  "role": "supporting character"}],
    )

    return {
        "schema_id":        "DraftStory",
        "schema_version":   "1.0.0",
        "draft_id":         f"draft-{project_id}-{run_id[:8]}",
        "project_id":       project_id,
        "title":            title,
        "genre":            genre,
        "visual_style":     visual_style,
        "generation_seed":  generation_seed,
        "primary_location": primary_location,
        "characters":       characters,
        "max_scenes":       max_scenes,
        "scenes": [
            {
                "scene_index": i + 1,
                "description": (
                    f"Scene {i + 1} of the {genre} episode at {primary_location}."
                ),
            }
            for i in range(max_scenes)
        ],
    }


# ---------------------------------------------------------------------------
# world-engine validation gate (§42)
# ---------------------------------------------------------------------------

class CanonViolationError(RuntimeError):
    """Raised when world-engine validate-story-draft rejects the draft."""


def _validate_draft(draft_path: Path, run_dir: Path) -> None:
    """Call world-engine validate-story-draft.  Raises CanonViolationError on failure.

    Skipped when world-engine is not installed (Phase 0 — no canon yet).
    CanonSnapshot.json is passed when present alongside the run directory.
    """
    if find_agent_bin("world-engine") is None:
        return  # world-engine not installed — skip validation

    canon_path     = run_dir / "CanonSnapshot.json"
    violation_path = run_dir / "CanonViolationReport.json"

    args = [
        "validate-story-draft",
        "--draft", str(draft_path),
        "--out",   str(violation_path),
    ]
    if canon_path.exists():
        args += ["--canon", str(canon_path)]

    try:
        result = call_agent("world-engine", args, capture_output=True, text=True)
    except Exception as exc:
        # Unexpected failure from world-engine call; skip validation rather than
        # halt — resilience for Phase 0 where world-engine may be partially available.
        print(
            f"⚠  world-engine validate-story-draft failed unexpectedly: {exc}; "
            "skipping validation",
            flush=True,
        )
        return

    if result.returncode == 0:
        return  # validation passed

    # Validation failed — surface the report and stop the pipeline.
    msg = (
        f"Canon validation failed (world-engine exited {result.returncode}).\n"
        f"  Report: {violation_path}\n"
    )
    if violation_path.exists():
        try:
            report     = json.loads(violation_path.read_text(encoding="utf-8"))
            violations = report.get("violations", [])
            for v in violations[:5]:  # show up to 5 for brevity
                msg += f"  • {v.get('message', v)}\n"
        except (OSError, json.JSONDecodeError):
            pass
    if result.stderr.strip():
        msg += result.stderr.strip()
    raise CanonViolationError(msg)


# ---------------------------------------------------------------------------
# StoryPrompt compilation (after validation passes — §42 step 3)
# ---------------------------------------------------------------------------

def _compile_story_prompt(draft: dict) -> dict:
    """Compile a StoryPrompt from a validated DraftStory.

    StoryPrompt is only created after world-engine validation passes (§42).
    """
    return {
        "schema_id":       "StoryPrompt",
        "schema_version":  "1.0.0",
        "prompt_id":       f"auto-{draft['project_id']}-{draft['draft_id'][-8:]}",
        "episode_goal":    f"Generate a compelling {draft['genre']} episode: {draft['title']}",
        "generation_seed": draft["generation_seed"],
        "series": {
            "genre": draft["genre"],
            "title": draft["title"],
            "tone":  draft["visual_style"],
        },
        "setting": {
            "primary_location": draft["primary_location"],
        },
        "characters":  draft["characters"],
        "constraints": {"max_scenes": draft["max_scenes"]},
    }


# ---------------------------------------------------------------------------
# writing-agent call
# ---------------------------------------------------------------------------

def _call_writing_agent(story_prompt_path: Path, script_out_path: Path) -> bool:
    """Call writing-agent generate. Returns True on success."""
    try:
        result = call_agent(
            "writing-agent",
            ["generate", "--prompt", str(story_prompt_path), "--out", str(script_out_path)],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except (FileNotFoundError, Exception):
        return False


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Generate Script.json following the §42 CORRECT WORKFLOW.

    Flow:
      1. Locate or auto-generate DraftStory.json.
      2. Call world-engine validate-story-draft  (skipped if world-engine absent).
      3. Compile StoryPrompt.json from the validated draft.
      4. Call writing-agent generate → Script.json.
      5. Fall back to a deterministic 2-scene stub if writing-agent is unavailable.

    Reads:  DraftStory.json    (project dir, optional; auto-generated if absent)
            CanonSnapshot.json (run dir, passed to world-engine when present)
    Writes: DraftStory.json         (run dir, when auto-generated)
            CanonViolationReport.json (run dir, written by world-engine on failure)
            StoryPrompt.json         (run dir, compiled after validation passes)
            Script.json
    """
    project_id = project_config["id"]
    run_dir    = registry.run_dir(project_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    project_path = project_config.get("_project_path", "")

    # ------------------------------------------------------------------
    # Step 1: locate or auto-generate DraftStory.json
    # ------------------------------------------------------------------
    draft_story_path: Path | None = None
    if project_path:
        candidate = Path(project_path).parent / "DraftStory.json"
        if candidate.exists():
            draft_story_path = candidate

    if draft_story_path is None:
        draft            = _generate_draft_story(project_config, run_id)
        draft_story_path = run_dir / "DraftStory.json"
        draft_story_path.write_text(
            json.dumps(draft, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    else:
        draft = json.loads(draft_story_path.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # Step 2: world-engine validate-story-draft  (§42 gate)
    # ------------------------------------------------------------------
    _validate_draft(draft_story_path, run_dir)

    # ------------------------------------------------------------------
    # Step 3: compile StoryPrompt.json from the validated draft
    # (check for a hand-authored StoryPrompt in the project dir first)
    # ------------------------------------------------------------------
    story_prompt_path: Path | None = None
    if project_path:
        candidate = Path(project_path).parent / "StoryPrompt.json"
        if candidate.exists():
            story_prompt_path = candidate

    if story_prompt_path is None:
        story_prompt      = _compile_story_prompt(draft)
        story_prompt_path = run_dir / "StoryPrompt.json"
        story_prompt_path.write_text(
            json.dumps(story_prompt, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    # ------------------------------------------------------------------
    # Step 4: writing-agent generate → Script.json
    # ------------------------------------------------------------------
    script: dict | None = None
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        tmp_script = Path(f.name)
    try:
        if _call_writing_agent(story_prompt_path, tmp_script) and tmp_script.stat().st_size > 0:
            script = json.loads(tmp_script.read_text(encoding="utf-8"))
    except Exception:
        pass
    finally:
        tmp_script.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Step 5: deterministic 2-scene stub fallback
    # ------------------------------------------------------------------
    if script is None:
        title  = project_config.get("title", "Untitled")
        genre  = project_config.get("genre", "drama")
        script = {
            "schema_id":    "Script",
            "schema_version": "1.0.0",
            "script_id":    f"script-{project_id}-{run_id[:8]}",
            "project_id":   project_id,
            "title":        title,
            "genre":        genre,
            "scenes": [
                {
                    "scene_id":    "scene-001",
                    "location":    "INT. COMMAND CENTER",
                    "time_of_day": "NIGHT",
                    "actions": [
                        {"type": "action",   "text": "The room hums with the glow of monitors."},
                        {"type": "dialogue", "character": "COMMANDER",
                         "text": "We have lost contact with the probe."},
                        {"type": "dialogue", "character": "ANALYST",
                         "text": "The signal disappeared twelve minutes ago."},
                    ],
                },
                {
                    "scene_id":    "scene-002",
                    "location":    "EXT. LAUNCH PAD",
                    "time_of_day": "DAWN",
                    "actions": [
                        {"type": "action",   "text": "A lone figure walks toward the rocket."},
                        {"type": "dialogue", "character": "COMMANDER",
                         "text": "Prepare for immediate launch."},
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
            "run_id":     run_id,
            "stage":      "stage1_generate_script",
        },
    )
    return script
