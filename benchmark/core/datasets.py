"""Dataset manifest resolution.

Run configs may reference centrally registered datasets instead of repeating
input paths and metadata:

    studies:
      - dataset: suppression_study

Entries live in ``benchmark/config/datasets.yaml`` (override with a top-level
``datasets_manifest:`` config key). Fields written directly on the study entry
override the manifest values.
"""
from __future__ import annotations

from pathlib import Path

import yaml

DEFAULT_MANIFEST_PATH = Path("benchmark/config/datasets.yaml")

# Manifest fields copied onto the resolved study dict (study-level overrides win).
_MERGED_FIELDS = (
    "input",
    "sample_freq",
    "description",
    "channels",
    "duration_hours",
    "approx_download_mib",
    "remote_only",
    "remote_query",
)


def load_dataset_manifest(path: str | Path | None = None) -> dict[str, dict]:
    """Load the dataset manifest and return the ``datasets`` mapping."""
    manifest_path = Path(path) if path else DEFAULT_MANIFEST_PATH
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Dataset manifest '{manifest_path}' does not exist. Studies that use "
            "'dataset:' references need the manifest file."
        )
    with open(manifest_path, encoding="utf-8") as f:
        content = yaml.safe_load(f) or {}
    datasets = content.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError(f"Dataset manifest '{manifest_path}' must define a 'datasets' mapping.")
    return datasets


def _resolve_study(study_cfg: dict, manifest: dict[str, dict]) -> dict:
    dataset_key = study_cfg.get("dataset")
    if not dataset_key:
        return dict(study_cfg)

    entry = manifest.get(dataset_key)
    if entry is None:
        known = ", ".join(sorted(manifest)) or "(none)"
        raise ValueError(
            f"Study references unknown dataset '{dataset_key}'. "
            f"Known manifest entries: {known}."
        )

    resolved = {"name": study_cfg.get("name", dataset_key), "dataset": dataset_key}
    for field in _MERGED_FIELDS:
        if field in study_cfg:
            resolved[field] = study_cfg[field]
        elif field in entry:
            resolved[field] = entry[field]
    if entry.get("azure"):
        resolved["azure"] = entry["azure"]
    return resolved


def _check_azure_consistency(cfg: dict, resolved_studies: list[dict]) -> None:
    """All studies in a run must share the single configured Azure account."""
    global_azure = cfg.get("azure") or {}
    for study in resolved_studies:
        study_azure = study.pop("azure", None)
        if not study_azure:
            continue
        if not global_azure:
            cfg["azure"] = dict(study_azure)
            global_azure = cfg["azure"]
            continue
        for key in ("storage_account", "container"):
            if key in study_azure and study_azure[key] != global_azure.get(key):
                raise ValueError(
                    f"Study '{study.get('name')}' uses Azure {key} "
                    f"'{study_azure[key]}' but the run is configured for "
                    f"'{global_azure.get(key)}'. All studies in one run must share "
                    "a single storage account and container."
                )


def resolve_studies(cfg: dict) -> list[dict]:
    """Resolve ``dataset:`` references in ``cfg['studies']`` via the manifest.

    Returns the resolved study list; also adopts the manifest's Azure settings
    when the run config does not define its own ``azure:`` block.
    """
    studies = cfg.get("studies") or []
    needs_manifest = any(isinstance(s, dict) and s.get("dataset") for s in studies)
    manifest: dict[str, dict] = {}
    if needs_manifest:
        manifest = load_dataset_manifest(cfg.get("datasets_manifest"))

    resolved = [_resolve_study(study, manifest) for study in studies]
    _check_azure_consistency(cfg, resolved)
    return resolved


__all__ = [
    "DEFAULT_MANIFEST_PATH",
    "load_dataset_manifest",
    "resolve_studies",
]
