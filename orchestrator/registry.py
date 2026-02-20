"""ArtifactRegistry: filesystem-backed artifact store with schema validation."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .utils.hashing import hash_artifact
from .validator import validate_artifact

# Maps artifact type → the field name that holds the artifact's own ID
_ARTIFACT_ID_FIELD: dict[str, str] = {
    "Script": "script_id",
    "ShotList": "shotlist_id",
    "AssetManifest": "manifest_id",
    "RenderPlan": "plan_id",
    "RenderOutput": "output_id",
    "RenderPackage": "request_id",
}


class ArtifactRegistry:
    """Manages artifact storage under base_dir/<project_id>/<run_id>/."""

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def artifact_path(self, project_id: str, run_id: str, artifact_type: str) -> Path:
        """Return the path to <artifact_type>.json for this run."""
        return self.base_dir / project_id / run_id / f"{artifact_type}.json"

    def meta_path(self, project_id: str, run_id: str, artifact_type: str) -> Path:
        """Return the path to <artifact_type>.meta.json for this run."""
        return self.base_dir / project_id / run_id / f"{artifact_type}.meta.json"

    # ------------------------------------------------------------------
    # Existence / validity check
    # ------------------------------------------------------------------

    def exists_and_valid(
        self, project_id: str, run_id: str, artifact_type: str
    ) -> bool:
        """Return True iff the artifact file exists AND passes schema validation.

        Meta is treated as strictly optional: only fail on a *confirmed* hash
        mismatch (meta parsed successfully AND ``"hash"`` key present AND value
        differs from the current artifact content).  Any other meta problem
        (file unreadable, malformed JSON, missing ``"hash"`` key) is treated as
        if no meta file exists, and the artifact is still considered valid.
        """
        path = self.artifact_path(project_id, run_id, artifact_type)
        if not path.exists():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            validate_artifact(data, artifact_type)
            meta_p = self.meta_path(project_id, run_id, artifact_type)
            if meta_p.exists():
                try:
                    meta = json.loads(meta_p.read_text(encoding="utf-8"))
                    stored_hash = meta.get("hash")
                    if stored_hash is not None and stored_hash != hash_artifact(data):
                        return False
                except Exception:
                    pass  # malformed / unreadable meta → treat as absent
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write_artifact(
        self,
        project_id: str,
        run_id: str,
        artifact_type: str,
        data: dict,
        parent_refs: Optional[list] = None,
        creation_params: Optional[dict] = None,
    ) -> None:
        """Validate *data*, write <artifact_type>.json and .meta.json.

        Raises:
            jsonschema.ValidationError: If data is schema-invalid.
        """
        validate_artifact(data, artifact_type)

        artifact_file = self.artifact_path(project_id, run_id, artifact_type)
        artifact_file.parent.mkdir(parents=True, exist_ok=True)
        artifact_file.write_text(
            json.dumps(data, indent=2, sort_keys=True), encoding="utf-8"
        )

        id_field = _ARTIFACT_ID_FIELD.get(artifact_type, "id")
        artifact_id = data.get(id_field, "")

        meta: dict = {
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "schema_version": data.get("schema_version", "1.0.0"),
            "hash": hash_artifact(data),
            "parent_refs": parent_refs if parent_refs is not None else [],
            "creation_params": creation_params if creation_params is not None else {},
            "compute_origin": "local",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        meta_file = self.meta_path(project_id, run_id, artifact_type)
        meta_file.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read_artifact(
        self, project_id: str, run_id: str, artifact_type: str
    ) -> dict:
        """Read and return the artifact dict.

        Raises:
            FileNotFoundError: If the artifact file does not exist.
        """
        path = self.artifact_path(project_id, run_id, artifact_type)
        return json.loads(path.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # Run summary
    # ------------------------------------------------------------------

    def write_run_summary(
        self, project_id: str, run_id: str, summary: dict
    ) -> None:
        """Write run_summary.json for this run."""
        path = self.base_dir / project_id / run_id / "run_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
