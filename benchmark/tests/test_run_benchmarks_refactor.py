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
import h5py
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
from benchmark.core.ingest import _canonical_file, _ingest_edf, _iter_edf_tables, _iter_hdf5_tables, ingest
from benchmark.core.study_info import StudyInfo
from benchmark.core.variants import generate_variants
from benchmark.core.remote import bench_remote_query
import benchmark.scripts.run_benchmarks as run_benchmarks
import benchmark.scripts.generate_benchmark_report as benchmark_report


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
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True},
                }
            },
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
                    "common": {"repetitions": 2, "default_window": 45},
                    "core": {
                        "random_access": {"enabled": True, "variants": "all", "read_positions": [0.5]},
                        "channel_subset": {"channel_subsets": [3]},
                        "window_scaling": {"window_sizes": [15, 45]},
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
            self.assertIn("Window sizes: [15, 45]", dry_run)
            self.assertIn("Repetitions: 2", dry_run)
            self.assertIn("Default window: 45", dry_run)
            self.assertIn("Read positions: [0.5]", dry_run)
            self.assertIn("Channel subsets: [3]", dry_run)
            self.assertIn("Total benchmark runs: ~69", dry_run)
            self.assertIn("summary: cached=4 would-create=9 reuses-canonical=1 unknown=0", dry_run)

    def test_system_info_handles_missing_psutil(self):
        with patch.object(study_info, "psutil", None), \
             patch.object(study_info.os, "sysconf", side_effect=OSError, create=True), \
             patch.object(study_info.platform, "system", return_value="Windows"), \
             patch.object(study_info.platform, "release", return_value="11"), \
             patch.object(study_info.platform, "processor", return_value=""), \
             patch.object(study_info.platform, "machine", return_value="x86_64"), \
             patch.object(study_info.platform, "python_version", return_value="3.12"), \
             patch.object(study_info.os, "cpu_count", return_value=8):
            info = study_info._system_info()

        self.assertEqual(info["ram_gb"], None)
        self.assertEqual(info["cpu"], "x86_64")

    def test_estimate_runs_includes_int32_and_remote_query(self):
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 2},
                "core": {"window_scaling": {"window_sizes": [15, 45]}},
                "parquet_investigations": {
                    "int32_storage": {"enabled": True},
                    "remote_query": {
                        "enabled": True,
                        "full_study_chunk_sec": 3600,
                        "remote_float32_path": "parquet/demo/",
                    },
                },
                "other": {"baseline_comparison": {"enabled": True}},
            }
        })
        selected = [
            ("int32_storage", "H", benchmarks.bench_int32_storage),
            ("remote_query", "I", remote.bench_remote_query),
            ("baseline_comparison", "K", benchmarks.bench_baseline_comparison),
        ]

        self.assertEqual(bench_utils._estimate_runs(cfg, selected), 37)

    def test_runner_rejects_legacy_source_study_configs(self):
        cfg = {
            "studies": [{
                "name": "legacy",
                "source": "parquet",
                "remote_parquet_url": "parquet/legacy/",
                "sample_freq": 256,
            }],
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True},
                }
            },
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

    def test_selected_benchmarks_follow_nested_enabled_flags(self):
        cfg = {
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True},
                },
                "parquet_investigations": {
                    "compression": {"enabled": True},
                    "remote_query": {"enabled": False},
                },
                "other": {
                    "tuned_comparison": {"enabled": True, "block_sizes_minutes": [5, 10]},
                },
            },
        }
        args = argparse.Namespace(categories=None)

        selected = run_benchmarks._selected_benchmarks(cfg, args)

        self.assertEqual([cat_id for cat_id, _, _ in selected], ["random_access", "compression", "tuned_comparison"])

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
        self.assertEqual(cfg["canonical_parquet"]["chunk_writer_max_rowgroups"], 1)
        self.assertEqual(cfg["canonical_parquet"]["chunk_reader_max_rows"], 65_536)
        self.assertFalse(cfg["benchmarks"]["core"]["random_access"]["include_canonical"])

    def test_validate_config_rejects_non_positive_canonical_streaming_knobs(self):
        cfg = normalize_config({"canonical_parquet": {"chunk_writer_max_rowgroups": 0}})
        with self.assertRaisesRegex(ValueError, "canonical_parquet.chunk_writer_max_rowgroups"):
            validate_config(cfg)

        cfg = normalize_config({"canonical_parquet": {"chunk_reader_max_rows": -1}})
        with self.assertRaisesRegex(ValueError, "canonical_parquet.chunk_reader_max_rows"):
            validate_config(cfg)

    def test_validate_config_rejects_removed_canonical_alias_keys(self):
        cfg = normalize_config({"canonical_parquet": {"write_row_groups_per_chunk": 2}})
        with self.assertRaisesRegex(ValueError, "write_row_groups_per_chunk"):
            validate_config(cfg)

        cfg = normalize_config({"canonical_parquet": {"variant_read_batch_rows": 1024}})
        with self.assertRaisesRegex(ValueError, "variant_read_batch_rows"):
            validate_config(cfg)

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

    def test_ingest_uses_configured_canonical_write_chunk_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            parquet_file = tmp_path / "input.parquet"
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                parquet_file,
                compression="snappy",
            )

            with patch("benchmark.core.ingest._rewrite_parquet_input", return_value=2) as mock_rewrite:
                ingest(
                    parquet_file,
                    tmp_path / "cache",
                    sample_freq=2,
                    canonical_cfg={
                        "compression": "snappy",
                        "row_group_minutes": 1,
                        "chunk_writer_max_rowgroups": 3,
                    },
                    study_name="demo",
                )

            self.assertEqual(mock_rewrite.call_args.args[3], 120)
            self.assertEqual(mock_rewrite.call_args.args[4], 360)

    def test_ingest_hdf5_matrix_fallback_labels_avoid_double_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            h5_file = tmp_path / "input.h5"
            with h5py.File(str(h5_file), "w") as hf:
                hf.attrs["sample_freq"] = 256.0
                hf.create_dataset(
                    "data",
                    data=np.array([[0.1, 1.1], [0.2, 1.2]], dtype=np.float32),
                )

            canonical_file, detected_fmt, sample_freq = ingest(
                h5_file,
                tmp_path / "cache",
                canonical_cfg={"compression": "snappy", "row_group_minutes": 30},
                study_name="demo",
            )

            self.assertEqual(detected_fmt, "hdf5")
            self.assertEqual(sample_freq, 256.0)
            self.assertEqual(pq.read_schema(canonical_file).names, ["samplestamp", "ch_0", "ch_1"])

    def test_ingest_hdf5_matrix_decodes_byte_channel_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            h5_file = tmp_path / "input.h5"
            with h5py.File(str(h5_file), "w") as hf:
                hf.attrs["sample_freq"] = 256.0
                hf.attrs["channel_labels"] = np.array([b"Fp1", b"C3"], dtype="S3")
                hf.create_dataset(
                    "data",
                    data=np.array([[0.1, 1.1], [0.2, 1.2]], dtype=np.float32),
                )

            canonical_file, detected_fmt, sample_freq = ingest(
                h5_file,
                tmp_path / "cache",
                canonical_cfg={"compression": "snappy", "row_group_minutes": 30},
                study_name="demo",
            )

            self.assertEqual(detected_fmt, "hdf5")
            self.assertEqual(sample_freq, 256.0)
            self.assertEqual(pq.read_schema(canonical_file).names, ["samplestamp", "ch_Fp1", "ch_C3"])

    def test_iter_hdf5_tables_chunks_matrix_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            h5_file = Path(tmp) / "input.h5"
            with h5py.File(str(h5_file), "w") as hf:
                hf.attrs["channel_labels"] = np.array([b"Fp1", b"C3"], dtype="S3")
                hf.create_dataset("samplestamp", data=np.array([10, 11, 12, 13, 14], dtype=np.int64))
                hf.create_dataset(
                    "data",
                    data=np.array(
                        [[0.1, 1.1], [0.2, 1.2], [0.3, 1.3], [0.4, 1.4], [0.5, 1.5]],
                        dtype=np.float32,
                    ),
                )

            with h5py.File(str(h5_file), "r") as hf:
                tables = list(_iter_hdf5_tables(hf, row_group_size=2))

            self.assertEqual([table.num_rows for table in tables], [2, 2, 1])
            self.assertEqual(tables[0].column_names, ["samplestamp", "ch_Fp1", "ch_C3"])
            self.assertEqual(tables[0]["samplestamp"].to_pylist(), [10, 11])
            self.assertEqual(tables[-1]["samplestamp"].to_pylist(), [14])

    def test_iter_hdf5_tables_chunks_channel_group_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            h5_file = Path(tmp) / "input.h5"
            with h5py.File(str(h5_file), "w") as hf:
                channels = hf.create_group("channels")
                channels.create_dataset("Fp1", data=np.array([0.1, 0.2, 0.3, 0.4, 0.5], dtype=np.float32))
                channels.create_dataset("C3", data=np.array([1.1, 1.2, 1.3, 1.4, 1.5], dtype=np.float32))
                hf.create_dataset("samplestamp", data=np.array([20, 21, 22, 23, 24], dtype=np.int64))

            with h5py.File(str(h5_file), "r") as hf:
                tables = list(_iter_hdf5_tables(hf, row_group_size=2))

            self.assertEqual([table.num_rows for table in tables], [2, 2, 1])
            self.assertEqual(tables[0].column_names, ["samplestamp", "ch_C3", "ch_Fp1"])
            self.assertEqual(tables[1]["samplestamp"].to_pylist(), [22, 23])
            self.assertEqual(tables[-1]["ch_Fp1"].to_pylist(), [0.5])

    def test_iter_edf_tables_chunks_reader_windows(self):
        class FakeEdf:
            total_samples = 5
            sample_frequency = 1.0
            signal_labels = ["Fp1", "C3"]

            def __init__(self):
                self.calls = []

            def read_window(self, start_sample, n_samples, channel_indices=None):
                self.calls.append((start_sample, n_samples, channel_indices))
                base = np.arange(start_sample, start_sample + n_samples, dtype=np.float32)
                return np.vstack([base, base + 100.0])

        edf = FakeEdf()
        tables = list(_iter_edf_tables(edf, row_group_size=2))

        self.assertEqual(edf.calls, [(0, 2, None), (2, 2, None), (4, 1, None)])
        self.assertEqual([table.num_rows for table in tables], [2, 2, 1])
        self.assertEqual(tables[0].column_names, ["samplestamp", "ch_Fp1", "ch_C3"])
        self.assertEqual(tables[1]["samplestamp"].to_pylist(), [2, 3])
        self.assertEqual(tables[-1]["ch_C3"].to_pylist(), [104.0])

    def test_ingest_edf_streams_without_read_all_channels(self):
        class FakeEdfReader:
            def __init__(self, _path):
                self.total_samples = 5
                self.signal_labels = ["Fp1", "C3"]
                self.sample_frequency = 1.0
                self.calls = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

            def read_window(self, start_sample, n_samples, channel_indices=None):
                self.calls.append((start_sample, n_samples, channel_indices))
                base = np.arange(start_sample, start_sample + n_samples, dtype=np.float32)
                return np.vstack([base, base + 10.0])

            def read_all_channels(self):
                raise AssertionError("read_all_channels should not be used")

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out_file = tmp_path / "canonical.parquet"
            fake_reader = FakeEdfReader(Path("demo.edf"))
            with patch("benchmark.core.readers.EdfFileReader", return_value=fake_reader):
                sample_freq = _ingest_edf(Path("demo.edf"), out_file, None, "snappy", row_group_size=2)

            self.assertEqual(sample_freq, 1.0)
            self.assertEqual(fake_reader.calls, [(0, 2, None), (2, 2, None), (4, 1, None)])
            self.assertEqual(pq.read_schema(out_file).names, ["samplestamp", "ch_Fp1", "ch_C3"])

    def test_runner_rejects_no_variant_direct_source_erd_core_runs(self):
        cfg = {
            "studies": [{"name": "demo", "input": "demo.erd"}],
            "benchmarks": {
                "core": {
                    "random_access": {"enabled": True},
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
            "benchmarks": {
                "other": {
                    "baseline_comparison": {"enabled": True},
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
            "benchmarks": {
                "parquet_investigations": {
                    "compression": {
                        "enabled": False,
                        "variants": [{"codec": "snappy"}],
                    },
                },
                "other": {
                    "tuned_comparison": {
                        "block_sizes_minutes": [7, 15],
                        "parquet_codecs": ["lz4"],
                        "hdf5_compression": "lz4",
                        "chunk_sec": 123,
                    }
                },
            },
        }

        self.assertEqual(get_parquet_compression_variants(cfg), [{"codec": "snappy"}])
        self.assertEqual(get_tuned_block_sizes_minutes(cfg), [7, 15])
        self.assertEqual(get_tuned_parquet_codecs(cfg), ["lz4"])
        self.assertEqual(get_tuned_hdf5_compression(cfg), "lz4")
        self.assertEqual(get_tuned_chunk_sec(cfg), 123)

    def test_normalize_config_rejects_scalar_core_leaf_values(self):
        with self.assertRaisesRegex(ValueError, "benchmarks.core.random_access"):
            normalize_config({
                "benchmarks": {
                    "core": {
                        "random_access": True,
                    }
                }
            })

        with self.assertRaisesRegex(ValueError, "benchmarks.core.channel_subset"):
            normalize_config({
                "benchmarks": {
                    "core": {
                        "channel_subset": False,
                    }
                }
            })

        with self.assertRaisesRegex(ValueError, "benchmarks.core.window_scaling"):
            normalize_config({
                "benchmarks": {
                    "core": {
                        "window_scaling": 123,
                    }
                }
            })

    def test_normalize_config_rejects_list_style_benchmarks(self):
        with self.assertRaisesRegex(ValueError, "list-style benchmark selection"):
            normalize_config({"benchmarks": ["random_access"]})

    def test_normalize_config_rejects_removed_top_level_benchmark_fields(self):
        cases = {
            "repetitions": 2,
            "default_window": 30,
            "read_positions": [0.5],
            "channel_subsets": [4],
            "window_sizes": [60],
            "parquet_investigations": {"compression": {"enabled": True}},
            "tuned_comparison": {"enabled": True},
            "baseline_comparison": {"enabled": True},
        }

        for field, value in cases.items():
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, field):
                    normalize_config({field: value})

    def test_normalize_config_does_not_write_removed_top_level_mirrors(self):
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 2, "default_window": 30},
                "core": {
                    "random_access": {"enabled": True, "read_positions": [0.5]},
                    "channel_subset": {"enabled": True, "channel_subsets": [4]},
                    "window_scaling": {"enabled": True, "window_sizes": [60]},
                },
                "parquet_investigations": {
                    "compression": {"enabled": True},
                },
                "other": {
                    "tuned_comparison": {"enabled": True},
                    "baseline_comparison": {"enabled": True},
                },
            }
        })

        for field in (
            "repetitions",
            "default_window",
            "read_positions",
            "channel_subsets",
            "window_sizes",
            "parquet_investigations",
            "tuned_comparison",
            "baseline_comparison",
        ):
            with self.subTest(field=field):
                self.assertNotIn(field, cfg)

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

            self.assertIn("Generating test variants (skip if cached)", stdout.getvalue())
            self.assertIn("[cached] variant__pq_30m_lz4", stdout.getvalue())
            self.assertEqual(paths["parquet"], canonical)
            self.assertEqual(paths["variant__pq_30m_lz4"], out_file)

    def test_generate_variants_streams_parquet_variant_without_read_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1, 2, 3, 4], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2, 0.3, 0.4, 0.5], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )
            info = StudyInfo.from_parquet(canonical, sample_freq=1)

            with patch("benchmark.core.variants.pq.read_table", side_effect=AssertionError("read_table should not be used")):
                paths = generate_variants(
                    canonical,
                    info,
                    [{"id": "pq_stream", "format": "parquet", "row_group_minutes": 1, "compression": "lz4"}],
                    tmp_path / "variants",
                )

            self.assertEqual(pq.read_table(paths["variant__pq_stream"]).num_rows, 5)

    def test_generate_variants_uses_configured_chunk_reader_max_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1, 2], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2, 0.3], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )
            info = StudyInfo.from_parquet(canonical, sample_freq=1)
            captured = []

            def fake_iter(src_files, *, columns=None, batch_rows=None):
                captured.append(batch_rows)
                table = pa.table({
                    "samplestamp": pa.array([0, 1, 2], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2, 0.3], type=pa.float32()),
                })
                if columns is not None:
                    yield pa.table({name: table.column(name) for name in columns})
                else:
                    yield table

            with patch("benchmark.core.variants._iter_parquet_tables", side_effect=fake_iter):
                generate_variants(
                    canonical,
                    info,
                    [{"id": "pq_stream", "format": "parquet", "row_group_minutes": 1, "compression": "lz4"}],
                    tmp_path / "variants",
                    canonical_cfg={"chunk_reader_max_rows": 2},
                )

            self.assertEqual(captured, [2])

    def test_generate_variants_streams_hdf5_variants_without_read_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1, 2, 3, 4], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2, 0.3, 0.4, 0.5], type=pa.float32()),
                    "ch_C3": pa.array([1.1, 1.2, 1.3, 1.4, 1.5], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )
            info = StudyInfo.from_parquet(canonical, sample_freq=1)

            with patch("benchmark.core.variants.pq.read_table", side_effect=AssertionError("read_table should not be used")):
                paths = generate_variants(
                    canonical,
                    info,
                    [
                        {"id": "h5_col_stream", "format": "hdf5", "layout": "columnar", "chunk_minutes": 1, "compression": "lz4"},
                        {"id": "h5_rg_stream", "format": "hdf5", "layout": "rowgroup", "chunk_minutes": 1, "compression": "lz4"},
                    ],
                    tmp_path / "variants",
                )

            with h5py.File(paths["variant__h5_col_stream"], "r") as hf:
                self.assertEqual(hf["samplestamp"][:].tolist(), [0, 1, 2, 3, 4])
                np.testing.assert_allclose(hf["channels"]["Fp1"][:], np.array([0.1, 0.2, 0.3, 0.4, 0.5], dtype=np.float32))
            with h5py.File(paths["variant__h5_rg_stream"], "r") as hf:
                self.assertEqual(hf["samplestamp"][:].tolist(), [0, 1, 2, 3, 4])
                np.testing.assert_allclose(hf["data"][:, 0], np.array([0.1, 0.2, 0.3, 0.4, 0.5], dtype=np.float32))

    def test_parquet_to_edf_streams_without_read_table(self):
        created_writers = []

        class FakeWriter:
            def __init__(self, *_args, **_kwargs):
                self.headers = []
                self.blocks = []
                created_writers.append(self)

            def setSignalHeader(self, index, header):
                self.headers.append((index, header))

            def writeSamples(self, block):
                self.blocks.append([np.asarray(arr).tolist() for arr in block])

            def close(self):
                return None

        fake_pyedflib = SimpleNamespace(EdfWriter=FakeWriter)

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            canonical = tmp_path / "demo_canonical"
            canonical.mkdir()
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1, 2, 3, 4], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2, 0.3, 0.4, 0.5], type=pa.float32()),
                    "ch_C3": pa.array([1.1, 1.2, 1.3, 1.4, 1.5], type=pa.float32()),
                }),
                canonical / "part_00000.parquet",
                compression="snappy",
            )

            with patch.dict("sys.modules", {"pyedflib": fake_pyedflib}), \
                 patch("benchmark.core.setup.pq.read_table", side_effect=AssertionError("read_table should not be used")):
                setup._parquet_to_edf(canonical, tmp_path / "demo.edf", sample_freq=1.0)

            self.assertEqual(len(created_writers), 1)
            self.assertEqual(len(created_writers[0].headers), 2)
            self.assertTrue(created_writers[0].blocks)

    def test_generate_variants_passes_chunk_reader_max_rows_to_edf_conversion(self):
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
            info = StudyInfo.from_parquet(canonical, sample_freq=1)

            def fake_parquet_to_edf(_src, out_file, sample_freq=256.0, batch_rows=None):
                out_file.write_bytes(b"edf")

            with patch("benchmark.core.variants._parquet_to_edf", side_effect=fake_parquet_to_edf) as mock_convert:
                generate_variants(
                    canonical,
                    info,
                    [{"id": "edf_stream", "format": "edf"}],
                    tmp_path / "variants",
                    canonical_cfg={"chunk_reader_max_rows": 7},
                )

            self.assertEqual(mock_convert.call_args.kwargs["batch_rows"], 7)

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
            cfg = normalize_config({
                "canonical_parquet": {"chunk_reader_max_rows": 1},
                "benchmarks": {
                    "parquet_investigations": {
                        "compression": {
                            "enabled": True,
                            "variants": [{"codec": "none"}],
                        }
                    }
                },
            })

            with patch("benchmark.core.setup._iter_parquet_tables", wraps=setup._iter_parquet_tables) as mock_iter, \
                 patch("benchmark.core.setup.pq.read_table", side_effect=AssertionError("read_table should not be used")):
                setup._setup_parquet_compression_variants(paths, src_file, tmp_path / "variants", "demo", cfg)

            out_file = tmp_path / "variants" / "parquet_none.parquet"
            self.assertTrue(out_file.exists())
            self.assertEqual(paths["parquet_none"], out_file)
            self.assertTrue(mock_iter.called)
            self.assertTrue(all(call.kwargs["max_rows"] == 1 for call in mock_iter.call_args_list))

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

            with patch("benchmark.core.setup._iter_parquet_tables", wraps=setup._iter_parquet_tables) as mock_iter, \
                 patch("benchmark.core.setup.pq.read_table", side_effect=AssertionError("read_table should not be used")):
                setup._setup_int32_variants(
                    paths,
                    tmp_path / "variants",
                    "demo",
                    {"canonical_parquet": {"chunk_reader_max_rows": 1}},
                )

            self.assertTrue((tmp_path / "variants" / "parquet_int32_calibrated_zstd.parquet").exists())
            self.assertTrue((tmp_path / "variants" / "parquet_int32_nanovolt_snappy.parquet").exists())
            self.assertTrue(mock_iter.called)
            self.assertTrue(all(call.kwargs["max_rows"] == 1 for call in mock_iter.call_args_list))

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
            cfg = normalize_config({
                "canonical_parquet": {"chunk_reader_max_rows": 1},
                "benchmarks": {
                    "other": {
                        "tuned_comparison": {
                            "block_sizes_minutes": [5],
                            "parquet_codecs": ["lz4"],
                            "hdf5_compression": "lz4",
                        }
                    }
                },
            })

            with patch("benchmark.core.setup._iter_parquet_tables", wraps=setup._iter_parquet_tables) as mock_iter, \
                 patch("benchmark.core.setup.pq.read_table", side_effect=AssertionError("read_table should not be used")):
                setup._setup_tuned_variants(paths, tmp_path / "variants", info, cfg)

            self.assertTrue((tmp_path / "variants" / "tuned_pq_lz4_5m.parquet").exists())
            self.assertFalse((tmp_path / "variants" / "tuned_pq_5m.parquet").exists())
            self.assertTrue((tmp_path / "variants" / "tuned_h5_5m.h5").exists())
            self.assertIn("tuned_pq_lz4_5m", paths)
            self.assertNotIn("tuned_pq_5m", paths)
            self.assertTrue(mock_iter.called)
            self.assertTrue(all(call.kwargs["max_rows"] == 1 for call in mock_iter.call_args_list))

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
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 1, "default_window": 1},
                "core": {"window_scaling": {"window_sizes": [1]}},
                "other": {
                    "tuned_comparison": {
                        "block_sizes_minutes": [5],
                        "parquet_codecs": ["lz4"],
                        "hdf5_compression": "lz4",
                        "chunk_sec": 7,
                    }
                },
            }
        })
        paths = {
            "tuned_pq_lz4_5m": Path("dummy.parquet"),
            "tuned_h5_5m": Path("dummy.h5"),
        }

        with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((1, 2), dtype=np.float32))), \
             patch.object(benchmarks, "_read_tuned_pq", return_value=np.zeros((1, 2), dtype=np.float32)), \
             patch.object(benchmarks, "_read_h5_columnar_window", return_value=np.zeros((1, 2), dtype=np.float32)), \
             patch.object(benchmarks, "_chunk_ranges", return_value=[(0, 13)]) as mock_chunk_ranges:
            benchmarks.bench_tuned_comparison(info, paths, cfg)

        self.assertEqual(mock_chunk_ranges.call_count, 1)
        mock_chunk_ranges.assert_called_once_with(0, 19, 14)

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
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 1, "default_window": 1},
                "core": {"window_scaling": {"window_sizes": [1]}},
                "other": {"tuned_comparison": {"chunk_sec": 7}},
            }
        })
        paths = {"baseline_parquet": Path("baseline-input.parquet")}

        with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((5, 2), dtype=np.float32))), \
             patch.object(benchmarks, "_read_parquet_window", return_value=np.zeros((5, 2), dtype=np.float32)) as mock_read, \
             patch.object(benchmarks, "_chunk_ranges", return_value=[(0, 13)]) as mock_chunk_ranges, \
             patch.object(benchmarks, "_print_result") as mock_print_result:
            results = benchmarks.bench_baseline_comparison(info, paths, cfg)

        self.assertEqual(
            {row["category"] for row in results},
            {"baseline_random_access", "baseline_channel_subset", "baseline_window_scaling", "baseline_full_study"},
        )
        self.assertEqual({row["benchmark"] for row in results}, {"K.1", "K.2", "K.3", "K.4"})
        self.assertTrue(all(row.get("_printed_inline") for row in results))
        self.assertTrue(all(row["artifact"] == "Baseline input" for row in results))
        self.assertTrue(all(row["format"] == "baseline_parquet" for row in results))
        self.assertEqual(mock_chunk_ranges.call_count, 1)
        mock_chunk_ranges.assert_called_once_with(0, 19, 14)
        self.assertEqual(mock_read.call_args_list[0].args[0], Path("baseline-input.parquet"))
        self.assertEqual(mock_print_result.call_count, len(results))

    def test_print_result_prefixes_dotted_benchmark_ids(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            bench_utils._print_result({
                "benchmark": "K.3",
                "format": "baseline_parquet",
                "window_seconds": 300,
                "wall_clock_seconds": 0.0663,
                "mib_per_sec": 203.4,
            })

        line = stdout.getvalue().strip()
        self.assertIn("[K.3] baseline_parquet", line)
        self.assertIn("win=  300s", line)
        self.assertIn("time=0.0663s", line)

    def test_print_result_includes_first_run_time_when_present(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            bench_utils._print_result({
                "format": "parquet",
                "wall_clock_seconds": 0.05,
                "first_wall_clock_seconds": 0.12,
                "mib_per_sec": 10.0,
            })

        line = stdout.getvalue().strip()
        self.assertIn("time=0.0500s", line)
        self.assertIn("first=0.1200s", line)

    def test_print_result_includes_peak_rss_when_present(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            bench_utils._print_result({
                "format": "parquet",
                "wall_clock_seconds": 0.05,
                "peak_rss_mib": 123.4,
            })

        self.assertIn("rss=123.4 MiB", stdout.getvalue().strip())

    def test_timed_records_first_and_all_samples(self):
        peaks = iter([101.0, 103.0, 102.0])

        class FakeTracker:
            def __init__(self, *_args, **_kwargs):
                self.peak_rss_mib = None

            def __enter__(self):
                self.peak_rss_mib = next(peaks)
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        values = iter([1.0, 2.0, 3.0])
        with patch("benchmark.core.bench_utils._PeakRssTracker", FakeTracker), \
             patch("benchmark.core.bench_utils.time.perf_counter", side_effect=[0.0, 0.1, 1.0, 1.3, 2.0, 2.2]):
            timed = bench_utils._timed(lambda: next(values), reps=3)

        median_seconds, result = timed
        self.assertEqual(result, 3.0)
        self.assertAlmostEqual(median_seconds, 0.2)
        self.assertAlmostEqual(timed.first_seconds, 0.1)
        self.assertEqual(len(timed.all_seconds), 3)
        np.testing.assert_allclose(timed.all_seconds, np.array([0.1, 0.3, 0.2], dtype=np.float64))
        self.assertAlmostEqual(timed.peak_rss_mib, 103.0)

    def test_gap_safe_row_helpers_use_row_counts_not_stamp_spans(self):
        stamps = [0, 1, 2, 3, 100, 101, 102, 103]
        info = SimpleNamespace(stamp_at_row=lambda row: stamps[row])

        row_bounds = benchmarks._mid_row_window(len(stamps), 5)
        self.assertEqual(row_bounds, (3, 7))
        self.assertEqual(benchmarks._stamp_bounds(info, row_bounds), (3, 103))
        self.assertEqual(benchmarks._row_chunk_windows(len(stamps), 5), [(0, 4), (5, 7)])

    def test_read_target_window_accepts_h5_columnar_alias(self):
        target = {"reader_kind": "h5_columnar", "path": Path("demo.h5")}
        expected = np.zeros((1, 2), dtype=np.float32)

        with patch.object(benchmarks, "_read_h5_columnar_window", return_value=expected) as mock_read:
            result = benchmarks._read_target_window(target, SimpleNamespace(), ["ch_Fp1"], 10, 11)

        self.assertIs(result, expected)
        mock_read.assert_called_once_with(Path("demo.h5"), ["ch_Fp1"], 10, 11)

    def test_read_target_window_accepts_h5_rowgroup_alias(self):
        target = {"reader_kind": "h5_rowgroup", "path": Path("demo.h5")}
        expected = np.zeros((1, 2), dtype=np.float32)

        with patch.object(benchmarks, "_read_h5_rowgroup_window", return_value=expected) as mock_read:
            result = benchmarks._read_target_window(target, SimpleNamespace(), ["ch_Fp1"], 10, 11)

        self.assertIs(result, expected)
        mock_read.assert_called_once_with(Path("demo.h5"), ["ch_Fp1"], 10, 11)

    def test_read_target_window_accepts_legacy_hdf5_columnar_alias(self):
        target = {"reader_kind": "hdf5_columnar", "path": Path("demo.h5")}
        expected = np.zeros((1, 2), dtype=np.float32)

        with patch.object(benchmarks, "_read_h5_columnar_window", return_value=expected) as mock_read:
            result = benchmarks._read_target_window(target, SimpleNamespace(), ["ch_Fp1"], 10, 11)

        self.assertIs(result, expected)
        mock_read.assert_called_once_with(Path("demo.h5"), ["ch_Fp1"], 10, 11)

    def test_read_target_window_accepts_legacy_hdf5_rowgroup_alias(self):
        target = {"reader_kind": "hdf5_rowgroup", "path": Path("demo.h5")}
        expected = np.zeros((1, 2), dtype=np.float32)

        with patch.object(benchmarks, "_read_h5_rowgroup_window", return_value=expected) as mock_read:
            result = benchmarks._read_target_window(target, SimpleNamespace(), ["ch_Fp1"], 10, 11)

        self.assertIs(result, expected)
        mock_read.assert_called_once_with(Path("demo.h5"), ["ch_Fp1"], 10, 11)

    def test_bench_random_access_precomputes_gap_safe_bounds_outside_timed_reads(self):
        stamps = [0, 1, 2, 3, 100, 101, 102, 103]
        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            total_rows=len(stamps),
            stamp_at_row=lambda row: stamps[row],
        )
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 1, "default_window": 5},
                "core": {"random_access": {"read_positions": [0.5]}},
            }
        })
        target = {
            "artifact_id": "baseline_parquet",
            "variant_id": None,
            "artifact_kind": "baseline",
            "format_family": "parquet",
            "reader_kind": "parquet",
            "path": Path("baseline.parquet"),
            "display_label": "baseline_parquet",
            "sort_index": 0,
        }
        captured = []

        def fake_read(_target, _info, _columns, start_stamp, end_stamp, reader_state=None):
            captured.append((start_stamp, end_stamp, reader_state))
            return np.zeros((1, 5), dtype=np.float32)

        def fake_timed(fn, reps):
            original = info.stamp_at_row

            def _fail(_row):
                raise AssertionError("stamp_at_row should not be called inside timed reads")

            info.stamp_at_row = _fail
            try:
                data = fn()
            finally:
                info.stamp_at_row = original
            return 0.1, data

        with patch.object(benchmarks, "_core_targets", return_value=[target]), \
             patch.object(benchmarks, "_read_target_window", side_effect=fake_read), \
             patch.object(benchmarks, "_timed", side_effect=fake_timed):
            results = benchmarks.bench_random_access(info, {}, cfg)

        self.assertEqual(captured, [(3, 103, None)])
        self.assertTrue(all(row["first_wall_clock_seconds"] == row["wall_clock_seconds"] for row in results))

    def test_bench_channel_subset_preserves_all_label_in_results(self):
        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1", "Fp2"],
            channel_columns=["ch_Fp1", "ch_Fp2"],
            total_rows=20,
            stamp_at_row=lambda row: row,
        )
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 1, "default_window": 5},
                "core": {"channel_subset": {"enabled": True, "channel_subsets": [1]}},
            }
        })
        target = {
            "artifact_id": "baseline_parquet",
            "variant_id": None,
            "artifact_kind": "baseline",
            "format_family": "parquet",
            "reader_kind": "parquet",
            "path": Path("baseline.parquet"),
            "display_label": "baseline_parquet",
            "sort_index": 0,
        }

        def fake_timed_call(fn, _reps, precision=6):
            data = fn()
            return 0.1, data, {"wall_clock_seconds": round(0.1, precision), "first_wall_clock_seconds": round(0.1, precision)}

        with patch.object(benchmarks, "_core_targets", return_value=[target]), \
             patch.object(benchmarks, "_read_target_window", return_value=np.zeros((2, 5), dtype=np.float32)), \
             patch.object(benchmarks, "_timed_call", side_effect=fake_timed_call):
            results = benchmarks.bench_channel_subset(info, {}, cfg)

        self.assertEqual([row["channels"] for row in results], ["1", "all"])

    def test_comparison_workload_full_study_uses_gap_safe_row_chunks(self):
        stamps = [0, 1, 2, 3, 100, 101, 102, 103]
        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            total_rows=len(stamps),
            stamp_at_row=lambda row: stamps[row],
        )
        variant = {
            "format": "parquet",
            "reader_kind": "parquet",
            "path": Path("variant.parquet"),
            "result_fields": {"artifact": "variant"},
        }
        captured = []

        def fake_read(_target, _info, _columns, start_stamp, end_stamp, reader_state=None):
            captured.append((start_stamp, end_stamp, reader_state))
            return np.zeros((1, 4), dtype=np.float32)

        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 1, "default_window": 5},
                "core": {"window_scaling": {"window_sizes": [5]}},
                "other": {"tuned_comparison": {"chunk_sec": 5}},
            }
        })

        with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((1, 5), dtype=np.float32))), \
             patch.object(benchmarks, "_read_target_window", side_effect=fake_read):
            results = benchmarks._run_comparison_workload_suite(
                info,
                [variant],
                cfg,
                category_prefix="baseline",
                section_letter="K",
                skip_message="skip",
            )

        self.assertIn("baseline_full_study", {row["category"] for row in results})
        self.assertEqual(captured, [(0, 100, None), (101, 103, None)])

    def test_bench_filter_pipeline_uses_gap_safe_row_chunks(self):
        stamps = list(range(300)) + list(range(1000, 1300))
        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1", "Fp2"],
            channel_columns=["ch_Fp1", "ch_Fp2"],
            total_rows=len(stamps),
            stamp_at_row=lambda row: stamps[row],
            start_stamp=stamps[0],
            end_stamp=stamps[-1],
        )
        target = {
            "artifact_id": "baseline_parquet",
            "variant_id": None,
            "artifact_kind": "baseline",
            "format_family": "parquet",
            "reader_kind": "parquet",
            "path": Path("baseline.parquet"),
            "display_label": "baseline_parquet",
            "sort_index": 0,
        }
        captured = []

        def fake_read(_target, _info, columns, start_stamp, end_stamp, reader_state=None):
            captured.append((start_stamp, end_stamp, reader_state))
            return np.zeros((len(columns), 300), dtype=np.float32)

        with patch.object(benchmarks, "_core_targets", return_value=[target]), \
             patch.object(benchmarks, "_read_target_window", side_effect=fake_read), \
             patch.object(benchmarks, "_apply_bipolar_montage", side_effect=lambda matrix, _labels: matrix), \
             patch.object(benchmarks, "_apply_filters", side_effect=lambda matrix, _sos: matrix), \
             patch.object(benchmarks, "_build_sos", return_value="sos"), \
             patch.object(np.fft, "rfft", return_value=np.zeros((2, 1), dtype=np.complex64)):
            results = benchmarks.bench_filter_pipeline(info, {}, {})

        self.assertEqual({row["benchmark"] for row in results}, {"D.1", "D.2"})
        self.assertEqual(set((start, end) for start, end, _ in captured), {(0, 299), (1000, 1299)})

    def test_bench_random_access_edf_reopens_for_each_timed_read(self):
        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            total_rows=100,
            stamp_at_row=lambda row: row,
        )
        cfg = normalize_config({
            "benchmarks": {
                "common": {"repetitions": 3, "default_window": 1},
                "core": {"random_access": {"read_positions": [0.0, 0.5]}},
            }
        })
        target = {
            "artifact_id": "baseline_edf",
            "variant_id": None,
            "artifact_kind": "baseline",
            "format_family": "edf",
            "reader_kind": "edf",
            "path": Path("baseline.edf"),
            "display_label": "baseline_edf",
            "sort_index": 0,
        }
        seen_reader_states = []

        def fake_read(_target, _info, _columns, start_stamp, end_stamp, reader_state=None):
            seen_reader_states.append(reader_state)
            n = int(end_stamp) - int(start_stamp) + 1
            return np.zeros((1, n), dtype=np.float32)

        def fake_timed(fn, reps):
            data = None
            for _ in range(reps):
                data = fn()
            return 0.1, data

        with patch.object(benchmarks, "_core_targets", return_value=[target]), \
             patch.object(benchmarks, "_read_target_window", side_effect=fake_read), \
             patch.object(benchmarks, "_timed", side_effect=fake_timed):
            results = benchmarks.bench_random_access(info, {}, cfg)

        self.assertEqual(len(seen_reader_states), 6)
        self.assertTrue(all(state is None for state in seen_reader_states))
        self.assertTrue(all(row["first_wall_clock_seconds"] == row["wall_clock_seconds"] for row in results))

    def test_bench_filter_pipeline_edf_reopens_for_each_chunk_read(self):
        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1", "Fp2"],
            channel_columns=["ch_Fp1", "ch_Fp2"],
            total_rows=3600,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=3599,
        )
        target = {
            "artifact_id": "baseline_edf",
            "variant_id": None,
            "artifact_kind": "baseline",
            "format_family": "edf",
            "reader_kind": "edf",
            "path": Path("baseline.edf"),
            "display_label": "baseline_edf",
            "sort_index": 0,
        }
        seen_reader_states = []

        def fake_read(_target, _info, columns, start_stamp, end_stamp, reader_state=None):
            seen_reader_states.append(reader_state)
            n = int(end_stamp) - int(start_stamp) + 1
            return np.zeros((len(columns), n), dtype=np.float32)

        with patch.object(benchmarks, "_core_targets", return_value=[target]) as mock_core_targets, \
             patch.object(benchmarks, "_read_target_window", side_effect=fake_read), \
             patch.object(benchmarks, "_apply_bipolar_montage", side_effect=lambda matrix, _labels: matrix), \
             patch.object(benchmarks, "_apply_filters", side_effect=lambda matrix, _sos: matrix), \
             patch.object(benchmarks, "_build_sos", return_value="sos"), \
             patch.object(benchmarks, "_full_study_duration_hours", return_value=1), \
             patch.object(np.fft, "rfft", return_value=np.zeros((2, 1), dtype=np.complex64)):
            results = benchmarks.bench_filter_pipeline(info, {}, {})

        self.assertEqual({row["benchmark"] for row in results}, {"D.1", "D.2"})
        self.assertTrue(seen_reader_states)
        self.assertTrue(all(state is None for state in seen_reader_states))
        mock_core_targets.assert_called_once_with({}, {}, "filter_pipeline")

    def test_build_sos_returns_float32_coefficients(self):
        sos = signal._build_sos(256.0)

        self.assertEqual(sos.dtype, np.float32)

    def test_apply_filters_uses_float32_inputs(self):
        captured = {}

        def fake_sosfilt(sos_arg, matrix_arg, axis=1):
            captured["sos_dtype"] = sos_arg.dtype
            captured["matrix_dtype"] = matrix_arg.dtype
            captured["axis"] = axis
            return np.zeros_like(matrix_arg, dtype=np.float32)

        with patch("scipy.signal.sosfilt", side_effect=fake_sosfilt):
            filtered = signal._apply_filters(
                np.ones((2, 8), dtype=np.float32),
                np.ones((3, 6), dtype=np.float64),
            )

        self.assertEqual(captured, {"sos_dtype": np.float32, "matrix_dtype": np.float32, "axis": 1})
        self.assertEqual(filtered.dtype, np.float32)

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
            cfg = normalize_config({
                "benchmarks": {
                    "common": {"repetitions": 1, "default_window": 1},
                    "parquet_investigations": {
                        "compression": {
                            "enabled": True,
                            "variants": [{"codec": "none"}],
                        }
                    },
                },
            })

            with patch.object(benchmarks, "_timed", return_value=(0.1, np.zeros((1, 2), dtype=np.float32))):
                results = benchmarks.bench_compression(info, paths, cfg)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["codec"], "none")

            cfg["benchmarks"]["parquet_investigations"]["compression"]["enabled"] = False
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
            cfg = normalize_config({
                "benchmarks": {
                    "common": {"repetitions": 1, "default_window": 1},
                    "parquet_investigations": {
                        "compression": {
                            "enabled": True,
                            "variants": [{"codec": "none"}],
                        }
                    },
                },
            })

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
        cfg = normalize_config({
            "azure": {"storage_account": "acct", "container": "waveforms"},
            "benchmarks": {
                "parquet_investigations": {
                    "remote_query": {
                        "enabled": True,
                        "n_random_points": 1,
                        "window_sec": 10,
                        "remote_float32_path": "parquet/demo/",
                    }
                }
            },
        })

        with patch.object(remote, "_make_duckdb_connection", return_value=FakeCon()), \
             patch.object(remote, "_duckdb_remote_read", return_value=(0.1, 10)):
            results = remote.bench_remote_query(info, {"edf": Path("missing.edf")}, cfg)

        self.assertTrue(any(r["category"] == "remote_query" for r in results))

    def test_bench_remote_query_prints_structured_rows_inline(self):
        class FakeCon:
            def close(self):
                return None

        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1", "Fp2"],
            channel_columns=["ch_Fp1", "ch_Fp2"],
            total_rows=100,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=99,
        )
        cfg = normalize_config({
            "azure": {"storage_account": "acct", "container": "waveforms"},
            "benchmarks": {
                "parquet_investigations": {
                    "remote_query": {
                        "enabled": True,
                        "n_random_points": 1,
                        "window_sec": 10,
                        "remote_float32_path": "parquet/demo/",
                    }
                }
            },
        })

        stdout = io.StringIO()
        with patch.object(remote, "_make_duckdb_connection", return_value=FakeCon()), \
             patch.object(remote, "_duckdb_remote_read", return_value=(0.1, 10)), \
             redirect_stdout(stdout):
            results = remote.bench_remote_query(info, {"edf": Path("missing.edf")}, cfg)

        text = stdout.getvalue()
        self.assertTrue(all(r.get("_printed_inline") for r in results))
        self.assertLess(
            text.index("parquet_float32_snappy  subset=all"),
            text.index("DuckDB float32_snappy [10-20 (19ch)]"),
        )

    def test_bench_remote_query_rows_include_peak_rss_when_available(self):
        class FakeCon:
            def close(self):
                return None

        class FakeTracker:
            def __init__(self, *_args, **_kwargs):
                self.peak_rss_mib = 321.0

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["Fp1", "Fp2"],
            channel_columns=["ch_Fp1", "ch_Fp2"],
            total_rows=100,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=99,
        )
        cfg = normalize_config({
            "azure": {"storage_account": "acct", "container": "waveforms"},
            "benchmarks": {
                "parquet_investigations": {
                    "remote_query": {
                        "enabled": True,
                        "n_random_points": 1,
                        "window_sec": 10,
                        "full_study_chunk_sec": 20,
                        "remote_float32_path": "parquet/demo/",
                    }
                }
            },
        })

        with patch.object(remote, "_PeakRssTracker", FakeTracker), \
             patch.object(remote, "_make_duckdb_connection", return_value=FakeCon()), \
             patch.object(remote, "_duckdb_remote_read", return_value=(0.1, 10)):
            results = remote.bench_remote_query(info, {"edf": Path("missing.edf")}, cfg)

        self.assertTrue(results)
        self.assertTrue(all(row["peak_rss_mib"] == 321.0 for row in results))

    def test_bench_remote_query_skips_empty_10_20_subset(self):
        class FakeCon:
            def close(self):
                return None

        info = SimpleNamespace(
            sample_freq=1.0,
            channel_labels=["X1", "X2"],
            channel_columns=["ch_X1", "ch_X2"],
            total_rows=100,
            stamp_at_row=lambda row: row,
            start_stamp=0,
            end_stamp=99,
        )
        cfg = normalize_config({
            "azure": {"storage_account": "acct", "container": "waveforms"},
            "benchmarks": {
                "parquet_investigations": {
                    "remote_query": {
                        "enabled": True,
                        "n_random_points": 1,
                        "window_sec": 10,
                        "full_study_chunk_sec": 20,
                        "remote_float32_path": "parquet/demo/",
                    }
                }
            },
        })
        calls = []

        def fake_duckdb_read(_con, _path, columns, _start, _end):
            calls.append(list(columns))
            return 0.1, 10

        with patch.object(remote, "_make_duckdb_connection", return_value=FakeCon()), \
             patch.object(remote, "_duckdb_remote_read", side_effect=fake_duckdb_read):
            results = remote.bench_remote_query(info, {"edf": Path("missing.edf")}, cfg)

        self.assertEqual(calls, [["ch_X1", "ch_X2"]] * 6)
        self.assertEqual([row["channel_subset"] for row in results], ["all", "all"])

    def test_runner_skips_duplicate_print_for_inline_logged_results(self):
        def fake_bench(_info, _paths, _cfg):
            return [{
                "category": "remote_query",
                "benchmark": "I.1",
                "format": "parquet_float32_snappy",
                "channel_subset": "all",
                "window_seconds": 600,
                "total_wall_seconds": 1.0,
                "_printed_inline": True,
            }]

        info = SimpleNamespace(
            sample_freq=256.0,
            channel_labels=["Fp1"],
            channel_columns=["ch_Fp1"],
            total_rows=1000,
            start_stamp=0,
            end_stamp=999,
            segment_plans=[object()],
        )

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            results_path = tmp_path / "results.json"
            cfg = {
                "cache_dir": str(tmp_path / "cache"),
                "studies": [{"name": "demo", "input": "demo.parquet", "sample_freq": 256}],
                "benchmarks": {
                    "parquet_investigations": {
                        "remote_query": {"enabled": True},
                    }
                },
                "variants": [],
            }
            args = argparse.Namespace(
                config="benchmark/config/default.yaml",
                categories=None,
                output=str(results_path),
                dry_run=False,
                no_report=True,
                sas_token=None,
            )

            with patch.object(run_benchmarks, "_selected_benchmarks", return_value=[("remote_query", "I", fake_bench)]), \
                 patch.object(run_benchmarks, "resolve_input_path", return_value=Path("demo.parquet")), \
                 patch.object(run_benchmarks, "ingest", return_value=(Path("demo_canonical"), "parquet", 256.0)), \
                 patch.object(run_benchmarks.StudyInfo, "from_parquet", return_value=info), \
                 patch.object(run_benchmarks, "generate_variants", return_value={"parquet": Path("demo_canonical")}), \
                 patch.object(run_benchmarks, "_system_info", return_value={"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16}), \
                 patch.object(run_benchmarks, "_print_result") as mock_print_result:
                run_benchmarks.run_benchmarks(cfg, args)

        mock_print_result.assert_not_called()

    def test_save_results_retries_after_transient_permission_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "results.json"
            attempts = {"count": 0}
            real_replace = run_benchmarks._replace_results_file

            def flaky_replace(tmp_path, target):
                attempts["count"] += 1
                if attempts["count"] < 3:
                    raise PermissionError("transient lock")
                return real_replace(tmp_path, target)

            with patch.object(run_benchmarks, "_replace_results_file", new=flaky_replace), \
                 patch.object(run_benchmarks.time, "sleep") as mock_sleep:
                run_benchmarks._save_results({"ok": True}, out_path)

            self.assertEqual(attempts["count"], 3)
            self.assertEqual(json.loads(out_path.read_text(encoding="utf-8")), {"ok": True})
            self.assertEqual(mock_sleep.call_count, 2)

    def test_save_results_keeps_previous_file_as_backup_on_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "results.json"
            out_path.write_text(json.dumps({"old": True}), encoding="utf-8")

            with patch.object(run_benchmarks.sys, "platform", "win32"):
                run_benchmarks._save_results({"new": True}, out_path)

            backup_path = out_path.with_name("results.json.bak")
            self.assertEqual(json.loads(out_path.read_text(encoding="utf-8")), {"new": True})
            self.assertEqual(json.loads(backup_path.read_text(encoding="utf-8")), {"old": True})

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
                "benchmarks": {
                    "core": {
                        "random_access": {"enabled": True},
                    }
                },
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
                "benchmarks": {
                    "core": {
                        "random_access": {"enabled": True},
                    }
                },
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

    def test_report_timing_cell_shows_first_run_proxy(self):
        cell = benchmark_report.metric_cell(
            {
                "wall_clock_seconds": 0.05,
                "first_wall_clock_seconds": 0.12,
                "mib_per_sec": 10.0,
            },
            "wall_clock_seconds",
            "mib_per_sec",
        )

        self.assertIn("0.0500s", cell)
        self.assertIn("first 0.1200s", cell)

    def test_report_overview_explains_warm_vs_first_timing(self):
        study = {
            "name": "demo",
            "sample_freq": 256.0,
            "channels": ["Fp1"],
            "duration_seconds": 4.0,
            "total_stamps": 1024,
        }
        system = {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16}

        overview = benchmark_report.build_overview(study, system, set())
        self.assertIn("warm-cache-leaning median", overview)
        self.assertIn("first_wall_clock_seconds", overview)


if __name__ == "__main__":
    unittest.main()
