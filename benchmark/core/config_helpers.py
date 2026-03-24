from __future__ import annotations

from copy import deepcopy

from .constants import CATEGORY_ORDER, Category

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
DEFAULT_CANONICAL_PARQUET = {
    "id": "canonical",
    "compression": "snappy",
    "row_group_minutes": 30,
    "chunk_writer_max_rowgroups": 1,
    "chunk_reader_max_rowgroups": 1,
}

REMOVED_TOP_LEVEL_BENCHMARK_FIELDS = {
    "repetitions": "benchmarks.common.repetitions",
    "default_window": "benchmarks.common.default_window",
    "read_positions": "benchmarks.core.random_access.read_positions",
    "channel_subsets": "benchmarks.core.channel_subset.channel_subsets",
    "window_sizes": "benchmarks.core.window_scaling.window_sizes",
    "parquet_investigations": "benchmarks.parquet_investigations",
    "tuned_comparison": "benchmarks.other.tuned_comparison",
    "baseline_comparison": "benchmarks.other.baseline_comparison",
}


def _list_or_empty(value) -> list:
    return list(value) if isinstance(value, list) else []


def _dict_or_empty(value) -> dict:
    return value if isinstance(value, dict) else {}


def _require_mapping(value, path: str) -> dict:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    raise ValueError(f"Config field '{path}' must be a mapping/object.")


def _normalize_core_leaf(leaf: dict, selector, extras: dict | None = None) -> dict:
    normalized = {
        "enabled": bool(leaf.get("enabled", False)),
        "variants": leaf.get("variants", selector),
        "include_canonical": bool(leaf.get("include_canonical", False)),
    }
    if extras:
        for key, value in extras.items():
            normalized[key] = leaf.get(key, value)
    return normalized


def normalize_config(cfg: dict | None) -> dict:
    cfg = deepcopy(cfg or {})
    if isinstance(cfg.get("benchmarks"), list):
        raise ValueError(
            "Config field 'benchmarks' must be a mapping/object; list-style benchmark selection is no longer supported. "
            "Use benchmarks.common/core/parquet_investigations/other with explicit enabled flags."
        )
    for field, replacement in REMOVED_TOP_LEVEL_BENCHMARK_FIELDS.items():
        if field in cfg:
            raise ValueError(f"Config field '{field}' is no longer supported; use '{replacement}'.")

    raw_benchmarks = _require_mapping(cfg.get("benchmarks"), "benchmarks")
    raw_common = _require_mapping(raw_benchmarks.get("common"), "benchmarks.common")
    raw_core = _require_mapping(raw_benchmarks.get("core"), "benchmarks.core")
    raw_parquet = _require_mapping(raw_benchmarks.get("parquet_investigations"), "benchmarks.parquet_investigations")
    raw_other = _require_mapping(raw_benchmarks.get("other"), "benchmarks.other")
    raw_random_access = _require_mapping(raw_core.get(Category.RANDOM_ACCESS), f"benchmarks.core.{Category.RANDOM_ACCESS}")
    raw_channel_subset = _require_mapping(raw_core.get(Category.CHANNEL_SUBSET), f"benchmarks.core.{Category.CHANNEL_SUBSET}")
    raw_remontage = _require_mapping(raw_core.get(Category.REMONTAGE), f"benchmarks.core.{Category.REMONTAGE}")
    raw_filter_pipeline = _require_mapping(raw_core.get(Category.FILTER_PIPELINE), f"benchmarks.core.{Category.FILTER_PIPELINE}")
    raw_window_scaling = _require_mapping(raw_core.get(Category.WINDOW_SCALING), f"benchmarks.core.{Category.WINDOW_SCALING}")
    raw_tuned = _require_mapping(raw_other.get(Category.TUNED_COMPARISON), f"benchmarks.other.{Category.TUNED_COMPARISON}")
    raw_baseline = _require_mapping(raw_other.get(Category.BASELINE_COMPARISON), f"benchmarks.other.{Category.BASELINE_COMPARISON}")

    common = {
        "repetitions": int(raw_common.get("repetitions", 3)),
        "default_window": int(raw_common.get("default_window", 60)),
    }
    core = {
        Category.RANDOM_ACCESS: _normalize_core_leaf(
            raw_random_access,
            "all",
            {"read_positions": _list_or_empty(raw_random_access.get("read_positions", [0.0, 0.5, 0.75, 0.95]))}
        ),
        Category.CHANNEL_SUBSET: _normalize_core_leaf(
            raw_channel_subset,
            "all",
            {"channel_subsets": _list_or_empty(raw_channel_subset.get("channel_subsets", [4, 10]))}
        ),
        Category.REMONTAGE: _normalize_core_leaf(
            raw_remontage,
            "all",
        ),
        Category.FILTER_PIPELINE: _normalize_core_leaf(
            raw_filter_pipeline,
            "all",
        ),
        Category.WINDOW_SCALING: _normalize_core_leaf(
            raw_window_scaling,
            "all",
            {"window_sizes": _list_or_empty(raw_window_scaling.get("window_sizes", [10, 30, 60, 300, 900, 1800, 3600]))}
        ),
    }
    parquet_investigations = {
        Category.COMPRESSION: _require_mapping(raw_parquet.get(Category.COMPRESSION), f"benchmarks.parquet_investigations.{Category.COMPRESSION}"),
        Category.PRECISION_LOSS: _require_mapping(raw_parquet.get(Category.PRECISION_LOSS), f"benchmarks.parquet_investigations.{Category.PRECISION_LOSS}"),
        Category.INT32_STORAGE: _require_mapping(raw_parquet.get(Category.INT32_STORAGE), f"benchmarks.parquet_investigations.{Category.INT32_STORAGE}"),
        Category.REMOTE_QUERY: _require_mapping(raw_parquet.get(Category.REMOTE_QUERY), f"benchmarks.parquet_investigations.{Category.REMOTE_QUERY}"),
    }
    for category in (Category.COMPRESSION, Category.PRECISION_LOSS, Category.INT32_STORAGE, Category.REMOTE_QUERY):
        parquet_investigations[category]["enabled"] = bool(parquet_investigations[category].get("enabled", False))

    tuned = dict(raw_tuned)
    tuned["enabled"] = bool(tuned.get("enabled", False))
    baseline = dict(raw_baseline)
    baseline["enabled"] = bool(baseline.get("enabled", False))

    cfg["canonical_parquet"] = get_canonical_parquet_cfg(cfg)
    cfg["benchmarks"] = {
        "common": common,
        "core": core,
        "parquet_investigations": parquet_investigations,
        "other": {
            Category.TUNED_COMPARISON: tuned,
            Category.BASELINE_COMPARISON: baseline,
        },
    }
    return cfg


def validate_config(cfg: dict) -> None:
    canonical_cfg = get_canonical_parquet_cfg(cfg)
    canonical_id = canonical_cfg.get("id")
    if not isinstance(canonical_id, str) or not canonical_id.strip():
        raise ValueError("canonical_parquet.id must define a non-empty string")
    for field in ("row_group_minutes", "chunk_writer_max_rowgroups", "chunk_reader_max_rowgroups"):
        value = canonical_cfg.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"canonical_parquet.{field} must be a positive integer")

    variants = cfg.get("variants", []) or []
    ids: list[str] = []
    for i, spec in enumerate(variants):
        variant_id = spec.get("id")
        if not isinstance(variant_id, str) or not variant_id.strip():
            raise ValueError(f"variants[{i}] must define a non-empty string 'id'")
        if variant_id in ids:
            raise ValueError(f"Duplicate variant id: {variant_id}")
        if variant_id == canonical_id:
            raise ValueError(f"canonical_parquet.id collides with root variant id: {variant_id}")
        ids.append(variant_id)

    variant_ids = set(ids)
    for category in (
        Category.RANDOM_ACCESS,
        Category.CHANNEL_SUBSET,
        Category.REMONTAGE,
        Category.FILTER_PIPELINE,
        Category.WINDOW_SCALING,
    ):
        selector = get_core_variants_selector(cfg, category)
        if selector == "all" or selector == []:
            continue
        if not isinstance(selector, list):
            raise ValueError(
                f"benchmarks.core.{category}.variants must be 'all', [], or a list of variant ids"
            )
        if any(not isinstance(variant_id, str) or not variant_id.strip() for variant_id in selector):
            raise ValueError(
                f"benchmarks.core.{category}.variants must contain only non-empty string variant ids"
            )
        if not variant_ids:
            raise ValueError(
                f"benchmarks.core.{category}.variants cannot list explicit ids when root variants is empty"
            )
        unknown = [variant_id for variant_id in selector if variant_id not in variant_ids]
        if unknown:
            raise ValueError(
                f"benchmarks.core.{category}.variants references unknown variant ids: {unknown}"
            )


def get_benchmarks_cfg(cfg: dict) -> dict:
    return _dict_or_empty(cfg.get("benchmarks"))


def get_common_benchmark_cfg(cfg: dict) -> dict:
    return _dict_or_empty(get_benchmarks_cfg(cfg).get("common"))


def get_core_benchmarks_cfg(cfg: dict) -> dict:
    return _dict_or_empty(get_benchmarks_cfg(cfg).get("core"))


def get_core_category_cfg(cfg: dict, category: str) -> dict:
    return _dict_or_empty(get_core_benchmarks_cfg(cfg).get(category))


def is_core_category_enabled(cfg: dict, category: str) -> bool:
    return bool(get_core_category_cfg(cfg, category).get("enabled", False))


def get_core_variants_selector(cfg: dict, category: str):
    return get_core_category_cfg(cfg, category).get("variants", "all")


def get_core_include_canonical(cfg: dict, category: str) -> bool:
    return bool(get_core_category_cfg(cfg, category).get("include_canonical", False))


def selected_categories(cfg: dict) -> list[str]:
    benchmarks = get_benchmarks_cfg(cfg)
    categories: list[str] = []
    for category in CATEGORY_ORDER:
        if category in (
            Category.RANDOM_ACCESS,
            Category.CHANNEL_SUBSET,
            Category.REMONTAGE,
            Category.FILTER_PIPELINE,
            Category.WINDOW_SCALING,
        ):
            if is_core_category_enabled(cfg, category):
                categories.append(category)
        elif category in (
            Category.COMPRESSION,
            Category.PRECISION_LOSS,
            Category.INT32_STORAGE,
            Category.REMOTE_QUERY,
        ):
            section = _dict_or_empty(benchmarks.get("parquet_investigations", {})).get(category, {})
            if bool(_dict_or_empty(section).get("enabled", False)):
                categories.append(category)
        else:
            section = _dict_or_empty(_dict_or_empty(benchmarks.get("other", {})).get(category, {}))
            if bool(section.get("enabled", False)):
                categories.append(category)
    return categories


def get_canonical_parquet_cfg(cfg: dict) -> dict:
    canonical_cfg = _dict_or_empty(cfg.get("canonical_parquet"))
    return {
        key: canonical_cfg.get(key, default)
        for key, default in DEFAULT_CANONICAL_PARQUET.items()
    }


def get_repetitions(cfg: dict) -> int:
    return int(get_common_benchmark_cfg(cfg).get("repetitions", 3))


def get_default_window(cfg: dict) -> int:
    return int(get_common_benchmark_cfg(cfg).get("default_window", 60))


def get_read_positions(cfg: dict) -> list[float]:
    return list(get_core_category_cfg(cfg, Category.RANDOM_ACCESS).get("read_positions", [0.0, 0.5, 0.75, 0.95]))


def get_channel_subsets(cfg: dict) -> list[int]:
    return list(get_core_category_cfg(cfg, Category.CHANNEL_SUBSET).get("channel_subsets", [4, 10]))


def get_window_sizes(cfg: dict) -> list[int]:
    return list(get_core_category_cfg(cfg, Category.WINDOW_SCALING).get("window_sizes", [10, 30, 60, 300, 900, 1800, 3600]))


def get_parquet_investigations(cfg: dict) -> dict:
    value = get_benchmarks_cfg(cfg).get("parquet_investigations", {})
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
    value = _dict_or_empty(_dict_or_empty(get_benchmarks_cfg(cfg).get("other", {})).get(Category.TUNED_COMPARISON, {}))
    return value if isinstance(value, dict) else {}


def get_baseline_comparison_cfg(cfg: dict) -> dict:
    value = _dict_or_empty(_dict_or_empty(get_benchmarks_cfg(cfg).get("other", {})).get(Category.BASELINE_COMPARISON, {}))
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


def get_baseline_chunk_sec(cfg: dict) -> int:
    section = get_baseline_comparison_cfg(cfg)
    return int(section.get("chunk_sec", get_tuned_chunk_sec(cfg)))


def tuned_parquet_key(codec: str, label: str) -> str:
    return f"tuned_pq_{label}" if codec == "snappy" else f"tuned_pq_{codec}_{label}"
