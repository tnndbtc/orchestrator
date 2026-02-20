"""Artifact schema validation against JSON Schema draft-07 definitions."""

import json
from pathlib import Path

import jsonschema

ARTIFACT_SCHEMAS: dict[str, str] = {
    "EpisodeBundle": "EpisodeBundle.v1.json",
    "Script": "Script.v1.json",
    "ShotList": "ShotList.v1.json",
    "AssetManifest": "AssetManifest.v1.json",
    "RenderPlan": "RenderPlan.v1.json",
    "RenderOutput": "RenderOutput.v1.json",
    "RenderPackage": "RenderPackage.v1.json",
}

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


def validate_artifact(data: dict, artifact_type: str) -> None:
    """Load schema from disk and validate data against it.

    Args:
        data: The artifact dict to validate.
        artifact_type: One of the keys in ARTIFACT_SCHEMAS.

    Raises:
        KeyError: If artifact_type is not recognised.
        jsonschema.ValidationError: If data does not conform to the schema.
        jsonschema.SchemaError: If the schema file itself is malformed.
    """
    schema_filename = ARTIFACT_SCHEMAS[artifact_type]
    schema_file = SCHEMAS_DIR / schema_filename
    schema = json.loads(schema_file.read_text(encoding="utf-8"))
    jsonschema.validate(instance=data, schema=schema)
