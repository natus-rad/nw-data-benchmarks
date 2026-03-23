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
from benchmark.core.config_helpers import get_parquet_compression_variants, get_tuned_block_sizes_minutes
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

    def test_nested_config_helpers_read_new_schema(self):
        cfg = {
            "parquet_investigations": {
                "compression": {
                    "enabled": False,
                    "variants": [{"codec": "snappy"}],
                },
            },
            "tuned_comparison": {"block_sizes_minutes": [7, 15]},
        }

        self.assertEqual(get_parquet_compression_variants(cfg), [{"codec": "snappy"}])
        self.assertEqual(get_tuned_block_sizes_minutes(cfg), [7, 15])

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
                    "format": "hdf5",
                    "layout": "columnar",
                    "chunk_minutes": 5,
                    "dtype": "float64",
                }], output_base)

            with self.assertRaisesRegex(ValueError, "compression=lz4"):
                generate_variants(canonical, info, [{
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
            spec = [{"format": "parquet", "row_group_minutes": 30, "compression": "lz4"}]

            generate_variants(canonical, info, spec, output_base)
            out_file = output_base / "parquet_30m_flo_lz4.parquet"
            self.assertTrue(out_file.exists())

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                paths = generate_variants(canonical, info, spec, output_base)

            self.assertIn("[cached] parquet_30m_flo_lz4", stdout.getvalue())
            self.assertEqual(paths["parquet"], out_file)

    def test_generate_variants_promotes_legacy_wrapped_single_file_cache(self):
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
            legacy_part = output_base / "parquet_30m_flo_lz4" / "part_00000.parquet"
            legacy_part.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.table({
                    "samplestamp": pa.array([0, 1], type=pa.int64()),
                    "ch_Fp1": pa.array([0.1, 0.2], type=pa.float32()),
                }),
                legacy_part,
                compression="lz4",
            )

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                paths = generate_variants(
                    canonical,
                    info,
                    [{"format": "parquet", "row_group_minutes": 30, "compression": "lz4"}],
                    output_base,
                )

            out_file = output_base / "parquet_30m_flo_lz4.parquet"
            self.assertTrue(out_file.exists())
            self.assertFalse(legacy_part.exists())
            self.assertIn("legacy single-file dataset", stdout.getvalue())
            self.assertEqual(paths["parquet"], out_file)

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

