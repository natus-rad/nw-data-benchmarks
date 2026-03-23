from __future__ import annotations

from pathlib import Path


def list_parquet_files(path: Path) -> list[Path]:
    """Return Parquet files for either a single file path or a dataset directory."""
    path = Path(path)
    if path.is_file():
        return [path] if path.suffix.lower() == ".parquet" else []
    return sorted(path.glob("*.parquet")) if path.exists() else []