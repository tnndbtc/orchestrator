"""Stage 1: Generate StoryPrompt from project config, then Script via writing-agent."""

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

from ..registry import ArtifactRegistry
from ..utils.agent_bin import call_agent


def _generate_story_prompt(project_config: dict, run_id: str) -> dict:
    """Derive a valid StoryPrompt from project.json fields.

    All required schema fields are filled deterministically from project config
    so the pipeline never needs a manually authored StoryPrompt.json.
    """
    project_id = project_config["id"]
    title = project_config.get("title", "Untitled")
    genre = project_config.get("genre", "drama")
    visual_style = project_config.get("visual_style", "cinematic")
    target_duration = project_config.get("target_duration", 60)

    max_scenes = max(2, target_duration // 30)

    genre_settings: dict[str, str] = {
        "sci-fi": "Space station",
        "fantasy": "Enchanted forest",
        "action": "Urban environment",
        "drama": "City apartment",
        "horror": "Abandoned building",
        "comedy": "Suburban neighborhood",
        "thriller": "Downtown high-rise",
        "romance": "Coastal town",
    }
    primary_location = genre_settings.get(genre.lower(), "Unspecified location")

    # Deterministic seed from project_id + run_id
    seed_bytes = hashlib.sha256(f"{project_id}:{run_id}".encode()).digest()
    generation_seed = int.from_bytes(seed_bytes[:4], "big") & 0x7FFF_FFFF

    genre_characters: dict[str, list[dict]] = {
        "sci-fi": [
            {"id": "commander", "role": "mission commander"},
            {"id": "analyst", "role": "data analyst"},
        ],
        "fantasy": [
            {"id": "hero", "role": "chosen one"},
            {"id": "mentor", "role": "wise guide"},
        ],
        "action": [
            {"id": "agent", "role": "field operative"},
            {"id": "handler", "role": "mission controller"},
        ],
        "horror": [
            {"id": "survivor", "role": "main survivor"},
            {"id": "investigator", "role": "investigator"},
        ],
        "thriller": [
            {"id": "detective", "role": "lead detective"},
            {"id": "suspect", "role": "prime suspect"},
        ],
    }
    characters = genre_characters.get(
        genre.lower(),
        [
            {"id": "protagonist", "role": "lead character"},
            {"id": "supporting", "role": "supporting character"},
        ],
    )

    return {
        "schema_id": "StoryPrompt",
        "schema_version": "1.0.0",
        "prompt_id": f"auto-{project_id}-{run_id[:8]}",
        "episode_goal": f"Generate a compelling {genre} episode: {title}",
        "generation_seed": generation_seed,
        "series": {
            "genre": genre,
            "title": title,
            "tone": visual_style,
        },
        "setting": {
            "primary_location": primary_location,
        },
        "characters": characters,
        "constraints": {
            "max_scenes": max_scenes,
        },
    }


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


def run(project_config: dict, run_id: str, registry: ArtifactRegistry) -> dict:
    """Generate Script.json via StoryPrompt + writing-agent, or fall back to stub.

    Flow:
      1. Check for an existing StoryPrompt.json in the project directory.
      2. If absent, auto-generate one from project config and write it to the run dir.
      3. Call writing-agent generate → Script.json.
      4. If writing-agent is unavailable, fall back to a deterministic 2-scene stub.

    Reads:  StoryPrompt.json (project dir, optional; auto-generated if missing)
    Writes: StoryPrompt.json (run dir, when auto-generated)
            Script.json
    """
    project_id = project_config["id"]
    run_dir = registry.run_dir(project_id, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    # --- Locate or generate StoryPrompt.json ---
    project_path = project_config.get("_project_path", "")
    story_prompt_path: Path | None = None

    if project_path:
        candidate = Path(project_path).parent / "StoryPrompt.json"
        if candidate.exists():
            story_prompt_path = candidate

    if story_prompt_path is None:
        # Auto-generate from project config and persist in the run dir
        story_prompt = _generate_story_prompt(project_config, run_id)
        generated_path = run_dir / "StoryPrompt.json"
        generated_path.write_text(
            json.dumps(story_prompt, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        story_prompt_path = generated_path

    # --- Call writing-agent ---
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

    # --- Fallback: deterministic 2-scene stub ---
    if script is None:
        title = project_config.get("title", "Untitled")
        genre = project_config.get("genre", "drama")
        script = {
            "schema_id": "Script",
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
