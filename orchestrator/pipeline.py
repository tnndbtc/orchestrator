"""PipelineRunner: executes the 5-stage orchestrator pipeline with resume/skip/force logic."""

import hashlib
import importlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .registry import ArtifactRegistry
from .utils.hashing import hash_artifact

# Ordered list of (stage_number, module_name, artifact_type)
STAGES: list[tuple[int, str, str]] = [
    (1, "stage1_generate_script",           "Script"),
    (2, "stage2_script_to_shotlist",        "ShotList"),
    (3, "stage3_shotlist_to_assetmanifest", "AssetManifest"),
    (4, "stage4_build_renderplan",          "RenderPlan"),
    (5, "stage5_render_preview",            "RenderOutput"),
]


def compute_run_id(project_config: dict) -> str:
    """Derive a stable run ID from the canonical SHA-256 of the project config.

    Returns a string of the form "run-<12 hex chars>".
    The same project config always maps to the same run ID.
    """
    content = json.dumps(
        project_config, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "run-" + hashlib.sha256(content).hexdigest()[:12]


class PipelineRunner:
    """Executes all pipeline stages with configurable skip / force / from-stage logic.

    Skip logic per stage
    --------------------
    A stage is RUN if:
        stage_num >= from_stage
        AND (force is True OR artifact does not exist / is schema-invalid)

    A stage is SKIPPED if:
        stage_num < from_stage
        OR (not force AND artifact exists_and_valid)
    """

    def __init__(
        self,
        project_config: dict,
        registry: ArtifactRegistry,
        artifacts_dir: str | Path,
        force: bool = False,
        from_stage: int = 1,
        run_id: Optional[str] = None,
        project_path: str = "",
    ) -> None:
        self.project_config = project_config
        self.registry = registry
        self.artifacts_dir = Path(artifacts_dir)
        self.force = force
        self.from_stage = from_stage
        self.run_id = run_id or compute_run_id(project_config)
        self.project_id: str = project_config["id"]
        self.project_path = project_path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_run(self, stage_num: int, artifact_type: str) -> bool:
        """Return True if the stage should execute (not be skipped)."""
        if stage_num < self.from_stage:
            return False
        if self.force:
            return True
        return not self.registry.exists_and_valid(
            self.project_id, self.run_id, artifact_type
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """Execute all pipeline stages and return the run summary dict.

        Writes run_summary.json regardless of success or failure.
        If any stage raises an exception, the pipeline stops at that stage
        and the summary status is set to "failed".
        """
        started_at = datetime.now(timezone.utc).isoformat()
        stage_results: list[dict] = []
        errors: list[str] = []
        overall_status = "completed"

        for stage_num, stage_name, artifact_type in STAGES:
            should_run = self._should_run(stage_num, artifact_type)

            if not should_run:
                stage_results.append(
                    {
                        "name": stage_name,
                        "stage_num": stage_num,
                        "artifact_type": artifact_type,
                        "status": "skipped",
                        "skipped": True,
                        "duration_sec": 0.0,
                        "artifact_path": str(
                            self.registry.artifact_path(
                                self.project_id, self.run_id, artifact_type
                            )
                        ),
                        "artifact_hash": None,
                        "error": None,
                    }
                )
                continue

            stage_start = time.monotonic()
            try:
                module = importlib.import_module(
                    f".stages.{stage_name}", package="orchestrator"
                )
                artifact = module.run(
                    self.project_config, self.run_id, self.registry
                )
                duration = time.monotonic() - stage_start
                stage_results.append(
                    {
                        "name": stage_name,
                        "stage_num": stage_num,
                        "artifact_type": artifact_type,
                        "status": "completed",
                        "skipped": False,
                        "duration_sec": round(duration, 6),
                        "artifact_path": str(
                            self.registry.artifact_path(
                                self.project_id, self.run_id, artifact_type
                            )
                        ),
                        "artifact_hash": hash_artifact(artifact),
                        "error": None,
                    }
                )
            except Exception as exc:
                duration = time.monotonic() - stage_start
                error_msg = f"{type(exc).__name__}: {exc}"
                errors.append(error_msg)
                overall_status = "failed"
                stage_results.append(
                    {
                        "name": stage_name,
                        "stage_num": stage_num,
                        "artifact_type": artifact_type,
                        "status": "failed",
                        "skipped": False,
                        "duration_sec": round(duration, 6),
                        "artifact_path": str(
                            self.registry.artifact_path(
                                self.project_id, self.run_id, artifact_type
                            )
                        ),
                        "artifact_hash": None,
                        "error": error_msg,
                    }
                )
                break  # stop on first failure

        completed_at = datetime.now(timezone.utc).isoformat()
        summary: dict = {
            "run_id": self.run_id,
            "project_id": self.project_id,
            "project_path": self.project_path,
            "started_at": started_at,
            "completed_at": completed_at,
            "status": overall_status,
            "stages": stage_results,
            "errors": errors,
        }
        self.registry.write_run_summary(self.project_id, self.run_id, summary)
        return summary
