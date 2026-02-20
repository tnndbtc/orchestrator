#!/usr/bin/env python3
"""Automated integration verification for the Phase 0 orchestrator → video pipeline."""

import json, sys, tempfile
from pathlib import Path

repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(repo_root))

from orchestrator.pipeline import PipelineRunner, compute_run_id
from orchestrator.registry import ArtifactRegistry

PROJECT_JSON = repo_root / "examples" / "phase0" / "project.json"

def main() -> None:
    project_config = json.loads(PROJECT_JSON.read_text())
    project_id = project_config["id"]

    with tempfile.TemporaryDirectory(prefix="orch-verify-") as tmp:
        artifacts_dir = Path(tmp)
        registry = ArtifactRegistry(artifacts_dir)
        run_id = compute_run_id(project_config)

        print(f"▶  Running pipeline for '{project_id}' (run_id={run_id})…")
        summary = PipelineRunner(
            project_config=project_config,
            registry=registry,
            artifacts_dir=artifacts_dir,
            force=True,
            run_id=run_id,
        ).run()

        if summary["status"] != "completed":
            print("FAIL  pipeline did not complete:")
            for s in summary["stages"]:
                if s.get("error"):
                    print(f"  ✗ {s['name']}: {s['error']}")
            sys.exit(1)

        shotlist = registry.read_artifact(project_id, run_id, "ShotList")
        ro       = registry.read_artifact(project_id, run_id, "RenderOutput")

        errors: list[str] = []
        video_uri    = ro.get("video_uri", "")
        captions_uri = ro.get("captions_uri", "")
        hashes       = ro.get("hashes", {})
        prov         = ro.get("provenance", {})

        if not str(video_uri).startswith("file://"):
            errors.append(f"video_uri not a file:// URI: {video_uri!r}")
        if not str(captions_uri).startswith("file://"):
            errors.append(f"captions_uri not a file:// URI: {captions_uri!r}")
        if not hashes.get("video_sha256"):
            errors.append("hashes.video_sha256 is null or missing")
        if not hashes.get("captions_sha256"):
            errors.append("hashes.captions_sha256 is null or missing")

        sl_hash = shotlist.get("timing_lock_hash", "")
        ro_hash = prov.get("timing_lock_hash", "")
        if sl_hash != ro_hash:
            errors.append(
                f"timing_lock_hash mismatch\n"
                f"  ShotList:          {sl_hash}\n"
                f"  RenderOutput.prov: {ro_hash}"
            )

        if errors:
            print("\nFAIL  integration verification failed:")
            for e in errors:
                print(f"  ✗ {e}")
            sys.exit(1)

        print("\nPASS  all integration checks passed:")
        print(f"  ✓ video_uri        = {video_uri}")
        print(f"  ✓ captions_uri     = {captions_uri}")
        print(f"  ✓ video_sha256     = {hashes['video_sha256'][:16]}…")
        print(f"  ✓ captions_sha256  = {hashes['captions_sha256'][:16]}…")
        print(f"  ✓ timing_lock_hash = {ro_hash[:16]}… (matches ShotList)")

if __name__ == "__main__":
    main()
