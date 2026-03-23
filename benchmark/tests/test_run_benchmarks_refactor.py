import argparse
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from benchmark.core import azure_storage, bench_utils, benchmarks, readers, remote, setup, signal, study_info
from benchmark.core.config_helpers import (
    get_canonical_parquet_cfg,
    get_parquet_compression_variants,
    get_tuned_block_sizes_minutes,
    get_tuned_chunk_sec,
    get_tuned_hdf5_compression,
    get_tuned_parquet_codecs,
    normalize_config,
    validate_config,
)
from benchmark.core.ingest import _canonical_file, ingest
from benchmark.core.study_info import StudyInfo
from benchmark.core.variants import generate_variants
from benchmark.core.remote import bench_remote_query
import benchmark.scripts.run_benchmarks as run_benchmarks


class BenchmarkRefactorTests(unittest.TestCase):
    def test_core_modules_import(self):
        self.assertIsNotNone(azure_storage)
        self.assertIsNotNone(study_info)
        self.assertIsNotNone(signal)
        self.assertIsNotNone(bench_utils)
        self.assertIsNotNone(setup)
        self.assertIsNotNone(readers)
        self.assertIsNotNone(remote)
        self.assertIsNotNone(benchmarks)

    def test_registry_contains_expected_categories(self):
        expected = {
            "random_access",
            "channel_subset",
            "remontage",
            "filter_pipeline",
            "window_scaling",
            "compression",
            "precision_loss",
            "int32_storage",
            "remote_query",
            "tuned_comparison",
            "baseline_comparison",
        }
        self.assertEqual(expected, set(benchmarks.BENCHMARKS))

    def test_remote_category_uses_remote_function(self):
        _, fn = benchmarks.BENCHMARKS["remote_query"]
        self.assertIs(fn, bench_remote_query)

    def test_runner_exposes_orchestrator(self):
        self.assertTrue(callable(run_benchmarks.run_benchmarks))
        self.assertTrue(callable(run_benchmarks.main))

    def test_runner_dry_run_accepts_input_study(self):
        cfg = {
            "studies": [{"name": "demo", "input": "demo.edf", "sample_freq": 256}],
            "benchmarks": ["random_access"],
        }
        args = argparse.Namespace(
            config="benchmark/config/default.yaml",
            categories=None,
            output=None,
            dry_run=True,
            sas_token=None,
        )

        with redirect_stdout(io.StringIO()) as out:
            run_benchmarks.run_benchmarks(cfg, args)

        self.assertIn("(input: demo.edf)", out.getvalue())

    def test_runner_dry_run_lists_planned_artifacts_and_cache_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_file = tmp_path / "demo.edf"
            input_file.write_bytes(b"demo")
            cache_dir = tmp_path / "cache"
            output_base = cache_dir / "demo_variants"
            output_base.mkdir(parents=True, exist_ok=True)

            cfg = {
                "cache_dir": str(cache_dir),
                "studies": [{"name": "demo", "input": str(input_file), "sample_freq": 256}],
                "variants": [
                    {"id": "pq_fast", "format": "parquet", "compression": "lz4", "row_group_minutes": 30},
                    {"id": "h5_col", "format": "hdf5", "layout": "columnar", "chunk_minutes": 5, "dtype": "float32", "compression": "lz4"},
                ],
                "benchmarks": {
                    "core": {
                        "random_access": {"enabled": True, "variants": "all"},
                    },
                    "parquet_investigations": {
                        "compression": {"enabled": True, "variants": [{"codec": "snappy"}, {"codec": "lz4"}]},
                        "precision_loss": {"enabled": True},
                        "int32_storage": {"enabled": True},
                    },
                    "other": {
                        "tuned_comparison": {
                            "enabled": True,
                            "block_sizes_minutes": [5],
                            "parquet_codecs": ["snappy", "lz4"],
                            "hdf5_compression": "lz4",
                        },
                        "baseline_comparison": {"enabled": True},
                    },
                },
            }
            normalized = normalize_config(cfg)

            canonical = _canonical_file(
                cache_dir,
                input_file,
                "edf",
                256.0,
                get_canonical_parquet_cfg(normalized),
                study_name="demo",
            )
            canonical.write_bytes(b"cached canonical")

            root_variant = run_benchmarks._root_variant_output_path(output_base, normalized["variants"][0])
            root_variant.write_bytes(b"cached variant")
            (output_base / "parquet_lz4.parquet").write_bytes(b"cached compression variant")
            (output_base / "tuned_h5_5m.h5").write_bytes(b"cached tuned h5")

            args = argparse.Namespace(
                config="benchmark/config/default.yaml",
                categories=[
                    "random_access",
                    "compression",
                    "precision_loss",
                    "int32_storage",
                    "tuned_comparison",
                    "baseline_comparison",
                ],
                output=None,
                dry_run=True,
                no_report=True,
                sas_token=None,
            )

            with redirect_stdout(io.StringIO()) as out:
                run_benchmarks.run_benchmarks(cfg, args)

            dry_run = out.getvalue()
            self.assertIn("[cached] canonical: canonical ->", dry_run)
            self.assertIn(str(canonical), dry_run)
            self.assertIn("[cached] root_variant: pq_fast ->", dry_run)
            self.assertIn("[would-create] root_variant: h5_col ->", dry_run)
            self.assertIn("[reuses-canonical] compression: parquet_snappy ->", dry_run)
            self.assertIn("[cached] compression: parquet_lz4 ->", dry_run)
            self.assertIn("[would-create] int32_storage: parquet_int32_calibrated_zstd ->", dry_run)
            self.assertIn("[would-create] tuned_parquet: tuned_pq_5m ->", dry_run)
            self.assertIn("[would-create] tuned_parquet: tuned_pq_lz4_5m ->", dry_run)
            self.assertIn("[cached] tuned_hdf5: tuned_h5_5m ->", dry_run)
            self.assertIn("[info] precision_loss reuses the default Parquet artifact; no extra cache artifacts", dry_run)
            self.assertIn("[info] baseline_comparison reuses the resolved study input artifact; no extra cache artifacts", dry_run)
            self.assertIn("summary: cached=4 would-create=9 reuses-canonical=1 unknown=0", dry_run)

    def test_runner_rejects_legacy_source_study_configs(self):
        cfg = {
            "studies": [{
                "name": "legacy",
                "source": "parquet",
                "remote_parquet_url": "parquet/legacy/",
                "sample_freq": 256,
            }],
            "benchmarks": ["random_access"],
        }
        args = argparse.Namespace(
            config="benchmark/config/default.yaml",
            categories=None,
            output=None,
            dry_run=True,
            sas_token=None,
        )

        with self.assertRaisesRegex(ValueError, "Legacy study configs are no longer supported"):
            run_benchmarks.run_benchmarks(cfg, args)

    def test_selected_benchmarks_come_only_from_benchmarks_list(self):
        cfg = {
            "benchmarks": ["random_access"],
            "parquet_investigations": {
                "compression": {"enabled": True},
                "remote_query": {"enabled": True},
            },
            "tuned_comparison": {"block_sizes_minutes": [5, 10]},
        }
        args = argparse.Namespace(categories=None)

        selected = run_benchmarks._selected_benchmarks(cfg, args)

        self.assertEqual([cat_id for cat_id, _, _ in selected], ["random_access"])

    def test_validate_config_requires_variant_ids(self):
        cfg = normalize_config({
            "variants": [{"format": "parquet", "compression": "lz4", "row_group_minutes": 30}],
        })

        with self.assertRaisesRegex(ValueError, "must define a non-empty string 'id'"):
            validate_config(cfg)

    def test_normalize_config_defaults_canonical_id_and_include_flag(self):
        cfg = normalize_config({
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True, "variants": "all"},
                }
            }
        })

        self.assertEqual(cfg["canonical_parquet"]["id"], "canonical")
        self.assertFalse(cfg["benchmarks"]["core"]["random_access"]["include_canonical"])

    def test_validate_config_rejects_canonical_id_collision(self):
        cfg = normalize_config({
            "canonical_parquet": {"id": "pq_main"},
            "variants": [{"id": "pq_main", "format": "parquet", "compression": "lz4", "row_group_minutes": 30}],
        })

        with self.assertRaisesRegex(ValueError, "collides with root variant id"):
            validate_config(cfg)

    def test_ingest_materializes_parquet_input_to_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            parquet_file = tmp_path / "input.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1, 2], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2, 0.3], type=pa.float32()),
                }),
                parquet_file,
                compression="snappy",
            )

            canonical_file, detected_fmt, sample_freq = ingest(
                parquet_file,
                tmp_path / "cache",
                sample_freq=256,
                canonical_cfg={"compression": "lz4", "row_group_minutes": 30},
                study_name="suppression_study",
            )

            self.assertEqual(detected_fmt, "parquet")
            self.assertEqual(sample_freq, 256.0)
            self.assertNotEqual(canonical_file, parquet_file)
            self.assertEqual(canonical_file.parent, tmp_path / "cache")
            self.assertEqual(canonical_file.suffix, ".parquet")
            self.assertRegex(canonical_file.name, r"^suppression_study_canonical_[0-9a-f]{10}\.parquet$")
            self.assertTrue(canonical_file.exists())

    def test_runner_rejects_no_variant_direct_source_erd_core_runs(self):
        cfg = {
            "studies": [{"name": "demo", "input": "demo.erd"}],
            "benchmarks": ["random_access"],
            "variants": [],
        }
        args = argparse.Namespace(
            config="benchmark/config/default.yaml",
            categories=None,
            output=None,
            dry_run=False,
            no_report=True,
            sas_token=None,
        )
        fake_info = SimpleNamespace(
            sample_freq=256.0,
            total_rows=1024,
            n_segments=1,
            segment_plans=[],
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
        )

        with patch.object(run_benchmarks, "resolve_input_path", return_value=Path("demo.erd")), \
             patch.object(run_benchmarks, "ingest", return_value=(Path("demo_canonical"), "erd", 256.0)), \
             patch.object(run_benchmarks.StudyInfo, "from_parquet", return_value=fake_info), \
             patch.object(run_benchmarks, "generate_variants", return_value={"parquet": Path("demo_canonical"), "__root_variants__": []}):
            with self.assertRaisesRegex(ValueError, "not yet supported for ERD"):
                run_benchmarks.run_benchmarks(cfg, args)

    def test_runner_allows_erd_canonical_only_core_run(self):
        cfg = {
            "studies": [{"name": "demo", "input": "demo.erd"}],
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True, "variants": [], "include_canonical": True},
                }
            },
            "variants": [],
        }
        args = argparse.Namespace(
            config="benchmark/config/default.yaml",
            categories=None,
            output=None,
            dry_run=False,
            no_report=True,
            sas_token=None,
        )
        fake_info = SimpleNamespace(
            sample_freq=256.0,
            total_rows=1024,
            n_segments=1,
            segment_plans=[],
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            start_stamp=0,
            end_stamp=1023,
        )
        captured = {}

        def _fake_bench(_info, paths, cfg_norm):
            captured["targets"] = benchmarks._core_targets(paths, cfg_norm, "random_access")
            return []

        with patch.object(run_benchmarks, "_selected_benchmarks", return_value=[("random_access", "A", _fake_bench)]), \
             patch.object(run_benchmarks, "resolve_input_path", return_value=Path("demo.erd")), \
             patch.object(run_benchmarks, "ingest", return_value=(Path("demo_canonical"), "erd", 256.0)), \
             patch.object(run_benchmarks.StudyInfo, "from_parquet", return_value=fake_info), \
             patch.object(run_benchmarks, "generate_variants", return_value={"parquet": Path("demo_canonical"), "__root_variants__": []}), \
             patch.object(run_benchmarks, "_save_results"):
            run_benchmarks.run_benchmarks(cfg, args)

        self.assertEqual([target["artifact_id"] for target in captured["targets"]], ["canonical"])

    def test_runner_wires_baseline_input_path_separately_from_canonical_parquet(self):
        cfg = {
            "studies": [{"name": "demo", "input": "demo-source.parquet", "sample_freq": 256}],
            "benchmarks": ["baseline_comparison"],
            "variants": [],
        }
        args = argparse.Namespace(
            config="benchmark/config/default.yaml",
            categories=None,
            output=None,
            dry_run=False,
            no_report=True,
            sas_token=None,
        )
        canonical = Path("demo_canonical.parquet")
        captured = {}

        def _fake_bench(_info, paths, _cfg):
            captured.update(paths)
            return []

        selected = [("baseline_comparison", "K", _fake_bench)]
        fake_info = SimpleNamespace(
            sample_freq=256.0,
            channel_labels=["Fp1"],
            start_stamp=0,
            end_stamp=1,
            total_rows=2,
            n_segments=1,
            segment_plans=[object()],
        )

        with patch.object(run_benchmarks, "_selected_benchmarks", return_value=selected), \
             patch.object(run_benchmarks, "resolve_input_path", return_value=Path("demo-source.parquet")), \
             patch.object(run_benchmarks, "ingest", return_value=(canonical, "parquet", 256.0)), \
             patch.object(run_benchmarks.StudyInfo, "from_parquet", return_value=fake_info), \
             patch.object(run_benchmarks, "generate_variants", return_value={"parquet": canonical}), \
             patch.object(run_benchmarks, "_save_results"):
            run_benchmarks.run_benchmarks(cfg, args)

        self.assertEqual(captured["parquet"], canonical)
        self.assertEqual(captured["baseline_parquet"], Path("demo-source.parquet"))

    def test_nested_config_helpers_read_new_schema(self):
        cfg = {
            "parquet_investigations": {
                "compression": {
                    "enabled": False,
                    "variants": [{"codec": "snappy"}],
                },
            },
            "tuned_comparison": {
                "block_sizes_minutes": [7, 15],
                "parquet_codecs": ["lz4"],
                "hdf5_compression": "lz4",
                "chunk_sec": 123,
            },
        }

        self.assertEqual(get_parquet_compression_variants(cfg), [{"codec": "snappy"}])
        self.assertEqual(get_tuned_block_sizes_minutes(cfg), [7, 15])
        self.assertEqual(get_tuned_parquet_codecs(cfg), ["lz4"])
        self.assertEqual(get_tuned_hdf5_compression(cfg), "lz4")
        self.assertEqual(get_tuned_chunk_sec(cfg), 123)

    def test_core_targets_can_append_canonical_for_root_variants(self):
        cfg = normalize_config({
            "canonical_parquet": {"id": "canonical_pq"},
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True, "variants": "all", "include_canonical": True},
                }
            },
        })
        paths = {
            "__root_variants__": [
                {"artifact_id": "pq_main", "variant_id": "pq_main", "artifact_kind": "variant", "format_family": "parquet", "reader_kind": "parquet", "path": Path("pq_main.parquet"), "display_label": "pq_main", "sort_index": 0},
                {"artifact_id": "h5_main", "variant_id": "h5_main", "artifact_kind": "variant", "format_family": "hdf5", "reader_kind": "hdf5_columnar", "path": Path("h5_main.h5"), "display_label": "h5_main", "sort_index": 1},
            ],
            "__canonical_target__": {"artifact_id": "canonical_pq", "variant_id": "canonical_pq", "artifact_kind": "canonical", "format_family": "parquet", "reader_kind": "parquet", "path": Path("canonical.parquet"), "display_label": "canonical_pq", "sort_index": 0},
        }

        targets = benchmarks._core_targets(paths, cfg, "random_access")

        self.assertEqual([target["artifact_id"] for target in targets], ["pq_main", "h5_main", "canonical_pq"])

    def test_core_targets_can_run_source_plus_canonical_when_requested(self):
        cfg = normalize_config({
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True, "variants": "all", "include_canonical": True},
                }
            },
            "variants": [],
        })
        paths = {
            "__root_variants__": [],
            "__source_target__": {"artifact_id": "source_parquet", "variant_id": None, "artifact_kind": "source", "format_family": "parquet", "reader_kind": "parquet", "path": Path("source.parquet"), "display_label": "source_parquet", "sort_index": 0},
            "__canonical_target__": {"artifact_id": "canonical", "variant_id": "canonical", "artifact_kind": "canonical", "format_family": "parquet", "reader_kind": "parquet", "path": Path("canonical.parquet"), "display_label": "canonical", "sort_index": 0},
        }

        targets = benchmarks._core_targets(paths, cfg, "random_access")

        self.assertEqual([target["artifact_id"] for target in targets], ["source_parquet", "canonical"])

    def test_core_targets_can_run_canonical_only(self):
        cfg = normalize_config({
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True, "variants": [], "include_canonical": True},
                }
            },
            "variants": [],
        })
        paths = {
            "__root_variants__": [],
            "__source_target__": {"artifact_id": "source_parquet", "variant_id": None, "artifact_kind": "source", "format_family": "parquet", "reader_kind": "parquet", "path": Path("source.parquet"), "display_label": "source_parquet", "sort_index": 0},
            "__canonical_target__": {"artifact_id": "canonical", "variant_id": "canonical", "artifact_kind": "canonical", "format_family": "parquet", "reader_kind": "parquet", "path": Path("canonical.parquet"), "display_label": "canonical", "sort_index": 0},
        }

        targets = benchmarks._core_targets(paths, cfg, "random_access")

        self.assertEqual([target["artifact_id"] for target in targets], ["canonical"])

    def test_resolve_input_path_downloads_remote_prefix(self):
        class FakeDownload:
            def __init__(self, payload: bytes):
                self.payload = payload

            def readinto(self, fh):
                fh.write(self.payload)
                return len(self.payload)

        class FakeContainerClient:
            def __init__(self, blobs: dict[str, bytes]):
                self.blobs = blobs

            def list_blobs(self, name_starts_with=None):
                prefix = name_starts_with or ""
                return [
                    SimpleNamespace(name=name, size=len(data))
                    for name, data in self.blobs.items()
                    if name.startswith(prefix)
                ]

            def download_blob(self, blob_name):
                return FakeDownload(self.blobs[blob_name])

        class FakeBlobServiceClient:
            def __init__(self, container_client):
                self.container_client = container_client

            def get_container_client(self, _name):
                return self.container_client

        with tempfile.TemporaryDirectory() as tmp:
            blobs = {
                "parquet/demo-study": b"",
                "parquet/demo-study/_metadata_json": b"",
                "parquet/demo-study/_metadata_json/waveform_meta.json": b"{}",
                "parquet/demo-study/part_00000.parquet": b"abc",
                "parquet/demo-study/part_00001.parquet": b"def",
            }
            cfg = {
                "cache_dir": tmp,
                "azure": {
                    "storage_account": "demo",
                    "container": "waveforms",
                    "anonymous": True,
                },
            }
            study = {"name": "demo", "input": "parquet/demo-study/"}
            args = argparse.Namespace(sas_token=None)

            with patch.object(
                azure_storage,
                "_get_blob_service_client",
                return_value=FakeBlobServiceClient(FakeContainerClient(blobs)),
            ):
                resolved = azure_storage.resolve_input_path(cfg, study, args)

            self.assertTrue(resolved.is_dir())
            self.assertEqual(resolved.name, "demo-study")
            self.assertEqual((resolved / "part_00000.parquet").read_bytes(), b"abc")
            self.assertEqual((resolved / "part_00001.parquet").read_bytes(), b"def")
            self.assertEqual((resolved / "_metadata_json" / "waveform_meta.json").read_bytes(), b"{}")
            self.assertFalse((resolved / "_metadata_json").is_file())
            self.assertTrue((resolved / ".download_complete").exists())

    def test_generate_variants_skips_empty_output_dir_when_no_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )
            info = StudyInfo.from_parquet(canonical, sample_freq=256)
            output_base = tmp_path / "demo_study_variants"

            paths = generate_variants(canonical, info, [], output_base)

            self.assertEqual(paths["parquet"], canonical)
            self.assertFalse(output_base.exists())

    def test_generate_variants_rejects_unsupported_hdf5_dtype_and_compression(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )
            info = StudyInfo.from_parquet(canonical, sample_freq=256)
            output_base = tmp_path / "demo_study_variants"

            with self.assertRaisesRegex(ValueError, "dtype=float32"):
                generate_variants(canonical, info, [{
                    "id": "h5_bad_dtype",
                    "format": "hdf5",
                    "layout": "columnar",
                    "chunk_minutes": 5,
                    "dtype": "float64",
                }], output_base)

            with self.assertRaisesRegex(ValueError, "compression=lz4"):
                generate_variants(canonical, info, [{
                    "id": "h5_bad_codec",
                    "format": "hdf5",
                    "layout": "columnar",
                    "chunk_minutes": 5,
                    "compression": "gzip",
                }], output_base)

    def test_generate_variants_reuses_single_file_parquet_variant_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )
            info = StudyInfo.from_parquet(canonical, sample_freq=256)
            output_base = tmp_path / "demo_study_variants"
            spec = [{"id": "pq_30m_lz4", "format": "parquet", "row_group_minutes": 30, "compression": "lz4"}]

            first_paths = generate_variants(canonical, info, spec, output_base)
            out_file = first_paths["variant__pq_30m_lz4"]
            self.assertTrue(out_file.exists())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                paths = generate_variants(canonical, info, spec, output_base)

            self.assertIn("[cached] variant__pq_30m_lz4", stdout.getvalue())
            self.assertEqual(paths["parquet"], canonical)
            self.assertEqual(paths["variant__pq_30m_lz4"], out_file)

    def test_study_info_from_parquet_accepts_single_file_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            pq_file = tmp_path / "single.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([10, 11], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                pq_file,
                compression="snappy",
            )

            info = StudyInfo.from_parquet(pq_file, sample_freq=256)

            self.assertEqual(info.channel_labels, ["Fp1"])
            self.assertEqual(info.total_rows, 2)
            self.assertEqual(info.stamp_at_row(1), 11)

    def test_setup_parquet_compression_variants_accepts_single_file_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src_file = tmp_path / "single.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                src_file,
                compression="snappy",
            )
            paths = {"parquet": src_file}
            cfg = {
                "parquet_investigations": {
                    "compression": {
                        "enabled": True,
                        "variants": [{"codec": "none"}],
                    }
                }
            }

            setup._setup_parquet_compression_variants(paths, src_file, tmp_path / "variants", "demo", cfg)

            out_file = tmp_path / "variants" / "parquet_none.parquet"
            self.assertTrue(out_file.exists())
            self.assertEqual(paths["parquet_none"], out_file)

    def test_setup_int32_variants_accepts_single_file_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src_file = tmp_path / "single.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                src_file,
                compression="snappy",
            )
            paths = {"parquet": src_file}

            setup._setup_int32_variants(paths, tmp_path / "variants", "demo")

            self.assertTrue((tmp_path / "variants" / "parquet_int32_calibrated_zstd.parquet").exists())
            self.assertTrue((tmp_path / "variants" / "parquet_int32_nanovolt_snappy.parquet").exists())

    def test_setup_tuned_variants_respects_configured_codecs(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            src_file = tmp_path / "single.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                src_file,
                compression="snappy",
            )
            info = StudyInfo.from_parquet(src_file, sample_freq=256)
            paths = {"parquet": src_file}
            cfg = {
                "tuned_comparison": {
                    "block_sizes_minutes": [5],
                    "parquet_codecs": ["lz4"],
                    "hdf5_compression": "lz4",
                }
            }

            setup._setup_tuned_variants(paths, tmp_path / "variants", info, cfg)

            self.assertTrue((tmp_path / "variants" / "tuned_pq_lz4_5m.parquet").exists())
            self.assertFalse((tmp_path / "variants" / "tuned_pq_5m.parquet").exists())
            self.assertTrue((tmp_path / "variants" / "tuned_h5_5m.h5").exists())
            self.assertIn("tuned_pq_lz4_5m", paths)
            self.assertNotIn("tuned_pq_5m", paths)

    def test_bench_tuned_comparison_uses_configured_chunk_sec(self):
        info = SimpleNamespace(
            sample_freq=2.0,
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            total_rows=20,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=19,
        )
        cfg = {
            "repetitions": 1,
            "default_window": 1,
            "window_sizes": [1],
            "tuned_comparison": {
                "block_sizes_minutes": [5],
                "parquet_codecs": ["lz4"],
                "hdf5_compression": "lz4",
                "chunk_sec": 7,
            },
        }
        paths = {
            "tuned_pq_lz4_5m": Path("dummy.parquet"),
            "tuned_h5_5m": Path("dummy.h5"),
        }

        with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((1, 2), dtype=np.float32))), \
             patch.object(benchmarks, "_read_tuned_pq", return_value=np.zeros((1, 2), dtype=np.float32)), \
             patch.object(benchmarks, "_read_h5_columnar_window", return_value=np.zeros((1, 2), dtype=np.float32)), \
             patch.object(benchmarks, "_chunk_ranges", return_value=[(0, 13)]) as mock_chunk_ranges:
            benchmarks.bench_tuned_comparison(info, paths, cfg)

        self.assertEqual(mock_chunk_ranges.call_count, 2)
        for call in mock_chunk_ranges.call_args_list:
            self.assertEqual(call.args, (0, 19, 14))

    def test_bench_baseline_comparison_uses_baseline_input_path_and_chunk_sec(self):
        info = SimpleNamespace(
            sample_freq=2.0,
            channel_labels=["Fp1", "Fp2", "C3", "C4", "O1"],
            channel_columns=["ch_Fp1", "ch_Fp2", "ch_C3", "ch_C4", "ch_O1"],
            total_rows=20,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=19,
        )
        cfg = {
            "repetitions": 1,
            "default_window": 1,
            "window_sizes": [1],
            "tuned_comparison": {"chunk_sec": 7},
        }
        paths = {"baseline_parquet": Path("baseline-input.parquet")}

        with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((5, 2), dtype=np.float32))), \
             patch.object(benchmarks, "_read_parquet_window", return_value=np.zeros((5, 2), dtype=np.float32)) as mock_read, \
             patch.object(benchmarks, "_chunk_ranges", return_value=[(0, 13)]) as mock_chunk_ranges:
            results = benchmarks.bench_baseline_comparison(info, paths, cfg)

        self.assertEqual(
            {row["category"] for row in results},
            {"baseline_random_access", "baseline_channel_subset", "baseline_window_scaling", "baseline_full_study"},
        )
        self.assertTrue(all(row["artifact"] == "Baseline input" for row in results))
        self.assertTrue(all(row["format"] == "baseline_parquet" for row in results))
        self.assertEqual(mock_chunk_ranges.call_count, 1)
        mock_chunk_ranges.assert_called_once_with(0, 19, 14)
        self.assertEqual(mock_read.call_args_list[0].args[0], Path("baseline-input.parquet"))

    def test_bench_compression_uses_nested_variants_and_enabled_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            pq_dir = Path(tmp) / "parquet_none"
            pq_dir.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                pq_dir / "part_00000.parquet",
                compression="snappy",
            )
            info = SimpleNamespace(
                sample_freq=1.0,
                channel_labels=["Fp1"],
                channel_columns=["ch_Fp1"],
                total_rows=2,
                stamp_at_row=lambda row: row,
            )
            paths = {"parquet": pq_dir, "parquet_none": pq_dir}
            cfg = {
                "repetitions": 1,
                "default_window": 1,
                "parquet_investigations": {
                    "compression": {
                        "enabled": True,
                        "variants": [{"codec": "none"}],
                    }
                },
            }

            with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((1, 2), dtype=np.float32))):
                results = benchmarks.bench_compression(info, paths, cfg)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["codec"], "none")

            cfg["parquet_investigations"]["compression"]["enabled"] = False
            self.assertEqual(benchmarks.bench_compression(info, paths, cfg), [])

    def test_bench_compression_reports_size_for_single_file_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            pq_file = tmp_path / "single.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                pq_file,
                compression="snappy",
            )
            info = SimpleNamespace(
                sample_freq=1.0,
                channel_labels=["Fp1"],
                channel_columns=["ch_Fp1"],
                total_rows=2,
                stamp_at_row=lambda row: row,
            )
            paths = {"parquet": pq_file, "parquet_none": pq_file}
            cfg = {
                "repetitions": 1,
                "default_window": 1,
                "parquet_investigations": {
                    "compression": {
                        "enabled": True,
                        "variants": [{"codec": "none"}],
                    }
                },
            }

            with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((1, 2), dtype=np.float32))):
                results = benchmarks.bench_compression(info, paths, cfg)

            self.assertGreater(results[0]["file_size_bytes"], 0)
            self.assertGreater(results[0]["file_size_mib"], 0)

    def test_bench_remote_query_uses_nested_remote_query_config(self):
        class FakeCon:
            def close(self):
                return None

        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            total_rows=100,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=99,
        )
        cfg = {
            "azure": {"storage_account": "acct", "container": "waveforms"},
            "parquet_investigations": {
                "remote_query": {
                    "enabled": True,
                    "n_random_points": 1,
                    "window_sec": 10,
                    "remote_float32_path": "parquet/demo/",
                }
            },
        }

        with patch.object(remote, "_make_duckdb_connection", return_value=FakeCon()), \
             patch.object(remote, "_duckdb_remote_read", return_value=(0.1, 10)):
            results = remote.bench_remote_query(info, {"edf": Path("missing.edf")}, cfg)

        self.assertTrue(any(r["category"] == "remote_query" for r in results))

    def test_save_results_retries_after_transient_permission_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "results.json"
            attempts = {"count": 0}
            real_replace = Path.replace

            def flaky_replace(self, target):
                attempts["count"] += 1
                if attempts["count"] < 3:
                    raise PermissionError("transient lock")
                return real_replace(self, target)

            with patch.object(Path, "replace", new=flaky_replace), \
                 patch.object(run_benchmarks.time, "sleep") as mock_sleep:
                run_benchmarks._save_results({"ok": True}, out_path)

            self.assertEqual(attempts["count"], 3)
            self.assertEqual(json.loads(out_path.read_text(encoding="utf-8")), {"ok": True})
            self.assertEqual(mock_sleep.call_count, 2)

    def test_runner_generates_report_by_default(self):
        def fake_bench(_info, _paths, _cfg):
            return [{
                "category": "random_access",
                "format": "parquet",
                "position": "0%",
                "wall_clock_seconds": 0.5,
                "mib_per_sec": 10.0,
            }]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            results_path = tmp_path / "results.json"
            cfg = {
                "cache_dir": str(tmp_path / "cache"),
                "studies": [{"name": "demo", "input": "demo.edf", "sample_freq": 256}],
                "benchmarks": ["random_access"],
            }
            args = argparse.Namespace(
                config="benchmark/config/default.yaml",
                categories=None,
                output=str(results_path),
                dry_run=False,
                no_report=False,
                sas_token=None,
            )
            info = SimpleNamespace(
                sample_freq=256.0,
                channel_labels=["Fp1"],
                start_stamp=0,
                end_stamp=1,
                total_rows=2,
                n_segments=1,
                segment_plans=[SimpleNamespace(last_stamp=1)],
            )

            with patch.object(run_benchmarks, "_selected_benchmarks", return_value=[("random_access", "Random Access", fake_bench)]), \
                 patch.object(run_benchmarks, "resolve_input_path", return_value=Path("demo.edf")), \
                 patch.object(run_benchmarks, "ingest", return_value=(Path("demo_canonical"), "edf", 256.0)), \
                 patch.object(run_benchmarks.StudyInfo, "from_parquet", return_value=info), \
                 patch.object(run_benchmarks, "generate_variants", return_value={"parquet": Path("demo_canonical")}), \
                 patch.object(run_benchmarks, "_system_info", return_value={"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16}), \
                 patch.object(run_benchmarks, "generate_report", return_value=(tmp_path / "report.md", tmp_path / "report.html")) as mock_generate_report:
                run_benchmarks.run_benchmarks(cfg, args)

            self.assertTrue(results_path.exists())
            mock_generate_report.assert_called_once_with(results_path, html=True)

    def test_runner_skips_report_when_no_report_requested(self):
        def fake_bench(_info, _paths, _cfg):
            return [{
                "category": "random_access",
                "format": "parquet",
                "position": "0%",
                "wall_clock_seconds": 0.5,
                "mib_per_sec": 10.0,
            }]

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            results_path = tmp_path / "results.json"
            cfg = {
                "cache_dir": str(tmp_path / "cache"),
                "studies": [{"name": "demo", "input": "demo.edf", "sample_freq": 256}],
                "benchmarks": ["random_access"],
            }
            args = argparse.Namespace(
                config="benchmark/config/default.yaml",
                categories=None,
                output=str(results_path),
                dry_run=False,
                no_report=True,
                sas_token=None,
            )
            info = SimpleNamespace(
                sample_freq=256.0,
                channel_labels=["Fp1"],
                start_stamp=0,
                end_stamp=1,
                total_rows=2,
                n_segments=1,
                segment_plans=[SimpleNamespace(last_stamp=1)],
            )

            with patch.object(run_benchmarks, "_selected_benchmarks", return_value=[("random_access", "Random Access", fake_bench)]), \
                 patch.object(run_benchmarks, "resolve_input_path", return_value=Path("demo.edf")), \
                 patch.object(run_benchmarks, "ingest", return_value=(Path("demo_canonical"), "edf", 256.0)), \
                 patch.object(run_benchmarks.StudyInfo, "from_parquet", return_value=info), \
                 patch.object(run_benchmarks, "generate_variants", return_value={"parquet": Path("demo_canonical")}), \
                 patch.object(run_benchmarks, "_system_info", return_value={"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16}), \
                 patch.object(run_benchmarks, "generate_report") as mock_generate_report:
                run_benchmarks.run_benchmarks(cfg, args)

            self.assertTrue(results_path.exists())
            mock_generate_report.assert_not_called()


if __name__ == "__main__":
    unittest.main()

