from __future__ import annotations

from pathlib import Path


def list_parquet_files(path: Path) -> list[Path]:
    """Return Parquet files for either a single file path or a dataset directory."""
    path = Path(path)
    if path.is_file():
        return [path] if path.suffix.lower() == ".parquet" else []
    return sorted(path.glob("*.parquet")) if path.exists() else []


def parquet_total_size_bytes(path: Path) -> int:
    """Return the total size of a Parquet artifact stored as a file or dataset dir."""
    return sum(file_path.stat().st_size for file_path in list_parquet_files(path))