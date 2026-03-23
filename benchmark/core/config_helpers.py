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
    "chunk_reader_max_rows": 65_536,
}


def _list_or_empty(value) -> list:
    return list(value) if isinstance(value, list) else []


def _dict_or_empty(value) -> dict:
    return value if isinstance(value, dict) else {}


def _enabled_from_legacy(categories: list[str], category: str) -> bool:
    return category in categories


def _normalize_core_leaf(raw_leaf, enabled: bool, selector, extras: dict | None = None) -> dict:
    leaf = _dict_or_empty(raw_leaf)
    normalized = {
        "enabled": bool(leaf.get("enabled", enabled)),
        "variants": leaf.get("variants", selector),
        "include_canonical": bool(leaf.get("include_canonical", False)),
    }
    if extras:
        for key, value in extras.items():
            normalized[key] = leaf.get(key, value)
    return normalized


def normalize_config(cfg: dict | None) -> dict:
    cfg = deepcopy(cfg or {})
    legacy_mode = isinstance(cfg.get("benchmarks"), list)
    legacy_categories = cfg.get("benchmarks") if isinstance(cfg.get("benchmarks"), list) else []
    raw_benchmarks = _dict_or_empty(cfg.get("benchmarks"))
    raw_common = _dict_or_empty(raw_benchmarks.get("common"))
    raw_core = _dict_or_empty(raw_benchmarks.get("core"))
    raw_parquet = _dict_or_empty(raw_benchmarks.get("parquet_investigations"))
    raw_other = _dict_or_empty(raw_benchmarks.get("other"))
    raw_random_access = _dict_or_empty(raw_core.get(Category.RANDOM_ACCESS))
    raw_channel_subset = _dict_or_empty(raw_core.get(Category.CHANNEL_SUBSET))
    raw_window_scaling = _dict_or_empty(raw_core.get(Category.WINDOW_SCALING))
    legacy_parquet = _dict_or_empty(cfg.get("parquet_investigations"))
    legacy_tuned = _dict_or_empty(cfg.get("tuned_comparison"))
    legacy_baseline = _dict_or_empty(cfg.get("baseline_comparison"))

    common = {
        "repetitions": int(raw_common.get("repetitions", cfg.get("repetitions", 3))),
        "default_window": int(raw_common.get("default_window", cfg.get("default_window", 60))),
    }
    core = {
        Category.RANDOM_ACCESS: _normalize_core_leaf(
            raw_random_access,
            _enabled_from_legacy(legacy_categories, Category.RANDOM_ACCESS),
            "all",
            {"read_positions": _list_or_empty(raw_random_access.get("read_positions", cfg.get("read_positions", [0.0, 0.5, 0.75, 0.95])))}
        ),
        Category.CHANNEL_SUBSET: _normalize_core_leaf(
            raw_channel_subset,
            _enabled_from_legacy(legacy_categories, Category.CHANNEL_SUBSET),
            "all",
            {"channel_subsets": _list_or_empty(raw_channel_subset.get("channel_subsets", cfg.get("channel_subsets", [4, 10])))}
        ),
        Category.REMONTAGE: _normalize_core_leaf(
            raw_core.get(Category.REMONTAGE),
            _enabled_from_legacy(legacy_categories, Category.REMONTAGE),
            "all",
        ),
        Category.FILTER_PIPELINE: _normalize_core_leaf(
            raw_core.get(Category.FILTER_PIPELINE),
            _enabled_from_legacy(legacy_categories, Category.FILTER_PIPELINE),
            "all",
        ),
        Category.WINDOW_SCALING: _normalize_core_leaf(
            raw_window_scaling,
            _enabled_from_legacy(legacy_categories, Category.WINDOW_SCALING),
            "all",
            {"window_sizes": _list_or_empty(raw_window_scaling.get("window_sizes", cfg.get("window_sizes", [10, 30, 60, 300, 900, 1800, 3600])))}
        ),
    }
    parquet_investigations = {
        Category.COMPRESSION: {
            **_dict_or_empty(legacy_parquet.get(Category.COMPRESSION)),
            **_dict_or_empty(raw_parquet.get(Category.COMPRESSION)),
        },
        Category.PRECISION_LOSS: {
            **_dict_or_empty(legacy_parquet.get(Category.PRECISION_LOSS)),
            **_dict_or_empty(raw_parquet.get(Category.PRECISION_LOSS)),
        },
        Category.INT32_STORAGE: {
            **_dict_or_empty(legacy_parquet.get(Category.INT32_STORAGE)),
            **_dict_or_empty(raw_parquet.get(Category.INT32_STORAGE)),
        },
        Category.REMOTE_QUERY: {
            **_dict_or_empty(legacy_parquet.get(Category.REMOTE_QUERY)),
            **_dict_or_empty(raw_parquet.get(Category.REMOTE_QUERY)),
        },
    }
    for category in (Category.COMPRESSION, Category.PRECISION_LOSS, Category.INT32_STORAGE, Category.REMOTE_QUERY):
        parquet_investigations[category]["enabled"] = bool(
            parquet_investigations[category].get("enabled", _enabled_from_legacy(legacy_categories, category))
        )

    tuned = {**legacy_tuned, **_dict_or_empty(raw_other.get(Category.TUNED_COMPARISON))}
    tuned["enabled"] = bool(tuned.get("enabled", _enabled_from_legacy(legacy_categories, Category.TUNED_COMPARISON)))
    baseline = {**legacy_baseline, **_dict_or_empty(raw_other.get(Category.BASELINE_COMPARISON))}
    baseline["enabled"] = bool(baseline.get("enabled", _enabled_from_legacy(legacy_categories, Category.BASELINE_COMPARISON)))

    if legacy_mode:
        for category in core:
            core[category]["enabled"] = _enabled_from_legacy(legacy_categories, category)
        for category in parquet_investigations:
            parquet_investigations[category]["enabled"] = _enabled_from_legacy(legacy_categories, category)
        tuned["enabled"] = _enabled_from_legacy(legacy_categories, Category.TUNED_COMPARISON)
        baseline["enabled"] = _enabled_from_legacy(legacy_categories, Category.BASELINE_COMPARISON)

    canonical_cfg = _dict_or_empty(cfg.get("canonical_parquet"))
    cfg["canonical_parquet"] = {
        **DEFAULT_CANONICAL_PARQUET,
        **canonical_cfg,
    }
    cfg["benchmarks"] = {
        "common": common,
        "core": core,
        "parquet_investigations": parquet_investigations,
        "other": {
            Category.TUNED_COMPARISON: tuned,
            Category.BASELINE_COMPARISON: baseline,
        },
    }

    # Transitional mirrors for call sites that still read legacy top-level keys.
    cfg["repetitions"] = common["repetitions"]
    cfg["default_window"] = common["default_window"]
    cfg["read_positions"] = list(core[Category.RANDOM_ACCESS].get("read_positions", []))
    cfg["channel_subsets"] = list(core[Category.CHANNEL_SUBSET].get("channel_subsets", []))
    cfg["window_sizes"] = list(core[Category.WINDOW_SCALING].get("window_sizes", []))
    cfg["parquet_investigations"] = parquet_investigations
    cfg["tuned_comparison"] = tuned
    cfg["baseline_comparison"] = baseline
    return cfg


def validate_config(cfg: dict) -> None:
    raw_canonical_cfg = _dict_or_empty(cfg.get("canonical_parquet"))
    if "write_row_groups_per_chunk" in raw_canonical_cfg:
        raise ValueError(
            "canonical_parquet.write_row_groups_per_chunk is no longer supported; "
            "use canonical_parquet.chunk_writer_max_rowgroups"
        )
    if "variant_read_batch_rows" in raw_canonical_cfg:
        raise ValueError(
            "canonical_parquet.variant_read_batch_rows is no longer supported; "
            "use canonical_parquet.chunk_reader_max_rows"
        )

    canonical_cfg = get_canonical_parquet_cfg(cfg)
    canonical_id = canonical_cfg.get("id")
    if not isinstance(canonical_id, str) or not canonical_id.strip():
        raise ValueError("canonical_parquet.id must define a non-empty string")
    for field in ("row_group_minutes", "chunk_writer_max_rowgroups", "chunk_reader_max_rows"):
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
    return {**DEFAULT_CANONICAL_PARQUET, **_dict_or_empty(cfg.get("canonical_parquet"))}


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