import argparse
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from benchmark.core import azure_storage, bench_utils, benchmarks, readers, remote, setup, signal, study_info
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

