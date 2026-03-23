from __future__ import annotations

DEFAULT_PARQUET_COMPRESSION_VARIANTS = [
    {"codec": "none"},
    {"codec": "snappy"},
    {"codec": "zstd", "level": 3},
    {"codec": "zstd", "level": 9},
    {"codec": "lz4"},
]

DEFAULT_TUNED_BLOCK_MINUTES = [5, 10, 20, 30, 60, 120]
DEFAULT_TUNED_PARQUET_CODECS = ["snappy", "lz4"]
DEFAULT_TUNED_HDF5_COMPRESSION = "lz4"
DEFAULT_TUNED_CHUNK_SEC = 300


def get_parquet_investigations(cfg: dict) -> dict:
    value = cfg.get("parquet_investigations", {})
    return value if isinstance(value, dict) else {}


def get_investigation_cfg(cfg: dict, name: str) -> dict:
    value = get_parquet_investigations(cfg).get(name, {})
    return value if isinstance(value, dict) else {}


def is_investigation_enabled(cfg: dict, name: str, default: bool = True) -> bool:
    section = get_investigation_cfg(cfg, name)
    if not section:
        return default
    return bool(section.get("enabled", default))


def get_parquet_compression_variants(cfg: dict) -> list[dict]:
    section = get_investigation_cfg(cfg, "compression")
    variants = section.get("variants")
    if variants is None:
        return list(DEFAULT_PARQUET_COMPRESSION_VARIANTS)
    return list(variants)


def get_remote_query_cfg(cfg: dict) -> dict:
    return get_investigation_cfg(cfg, "remote_query")


def get_tuned_comparison_cfg(cfg: dict) -> dict:
    value = cfg.get("tuned_comparison", {})
    return value if isinstance(value, dict) else {}


def get_tuned_block_sizes_minutes(cfg: dict) -> list:
    section = get_tuned_comparison_cfg(cfg)
    minutes = section.get("block_sizes_minutes")
    if minutes is None:
        return list(DEFAULT_TUNED_BLOCK_MINUTES)
    return list(minutes)


def get_tuned_parquet_codecs(cfg: dict) -> list[str]:
    section = get_tuned_comparison_cfg(cfg)
    codecs = section.get("parquet_codecs")
    if codecs is None:
        return list(DEFAULT_TUNED_PARQUET_CODECS)
    return list(codecs)


def get_tuned_hdf5_compression(cfg: dict) -> str:
    section = get_tuned_comparison_cfg(cfg)
    compression = section.get("hdf5_compression", DEFAULT_TUNED_HDF5_COMPRESSION)
    if compression != "lz4":
        raise ValueError("tuned_comparison.hdf5_compression currently supports only lz4")
    return compression


def get_tuned_chunk_sec(cfg: dict) -> int:
    section = get_tuned_comparison_cfg(cfg)
    return int(section.get("chunk_sec", DEFAULT_TUNED_CHUNK_SEC))


def tuned_parquet_key(codec: str, label: str) -> str:
    return f"tuned_pq_{label}" if codec == "snappy" else f"tuned_pq_{codec}_{label}"