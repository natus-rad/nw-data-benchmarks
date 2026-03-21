import tempfile
import unittest
from pathlib import Path

from benchmark.scripts.generate_benchmark_report import (
    ReportGenerationError,
    latest_results_file,
    render_report,
)


class GenerateBenchmarkReportTests(unittest.TestCase):
    def test_latest_results_file_picks_newest_matching_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "2026-01-01T00-00-00_benchmark_results.json"
            newer = root / "2026-01-02T00-00-00_benchmark_results.json"
            other = root / "ignore_me.json"
            older.write_text("{}", encoding="utf-8")
            newer.write_text("{}", encoding="utf-8")
            other.write_text("{}", encoding="utf-8")
            self.assertEqual(latest_results_file(root), newer)

    def test_render_report_includes_derived_sections_and_missing_category_notes(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 2,
                    "sample_freq": 100.0,
                    "total_stamps": 1000,
                    "duration_seconds": 10.0,
                }
            ],
            "benchmarks": [
                {"category": "random_access", "format": "parquet", "position": "0%", "wall_clock_seconds": 0.5, "mib_per_sec": 10.0},
                {"category": "random_access", "format": "edf", "position": "0%", "wall_clock_seconds": 1.0, "mib_per_sec": 5.0},
                {"category": "compression", "format": "parquet", "codec": "snappy", "file_size_bytes": 1024, "file_size_mib": 1.0, "wall_clock_seconds": 0.2},
                {"category": "compression", "format": "parquet", "codec": "zstd_3", "file_size_bytes": 512, "file_size_mib": 0.5, "wall_clock_seconds": 0.3},
                {
                    "category": "precision_loss",
                    "window_seconds": 60,
                    "num_channels": 2,
                    "worst_max_abs_error": 0.01,
                    "avg_snr_db": 95.0,
                    "channels": [{"channel": "Fp1", "max_abs_error": 0.01, "rms_error": 0.001, "snr_db": 95.0}],
                },
            ],
        }
        template = "# Report\n\n${overview}\n\n${summary}\n\n${key_observations}\n\n${sections}\n"
        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))
        self.assertIn("Random access (median 1-minute read)", rendered)
        self.assertIn("Ratio vs raw float32", rendered)
        self.assertIn("This category was not present in the input results file.", rendered)

    def test_latest_results_file_errors_when_directory_has_no_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ReportGenerationError):
                latest_results_file(Path(tmp))

    def test_render_report_keeps_j1_random_access_separate_from_j4_full_study(self) -> None:
        payload = {
            "run_id": "2026-03-21T00-00-00",
            "system": {"os": "Windows", "python": "3.12", "cpu_count": 8, "ram_gb": 16},
            "studies": [
                {
                    "name": "demo",
                    "channels": 46,
                    "sample_freq": 256.0,
                    "total_stamps": 11854000,
                    "duration_seconds": 46304.7,
                }
            ],
            "benchmarks": [
                {
                    "category": "tuned_random_access",
                    "format": "parquet_snappy",
                    "block_size": "5m",
                    "variant": "tuned_pq_5m",
                    "window_seconds": 60,
                    "wall_clock_seconds": 0.066929,
                    "mib_per_sec": 40.271,
                },
                {
                    "category": "tuned_random_access",
                    "format": "hdf5_lz4",
                    "block_size": "5m",
                    "variant": "tuned_h5_5m",
                    "window_seconds": 60,
                    "wall_clock_seconds": 0.055349,
                    "mib_per_sec": 48.697,
                },
                {
                    "category": "tuned_full_study",
                    "format": "parquet_snappy",
                    "block_size": "5m",
                    "variant": "tuned_pq_5m",
                    "total_samples": 11854000,
                    "wall_clock_seconds": 14.594,
                    "mib_per_sec": 142.534,
                },
                {
                    "category": "tuned_full_study",
                    "format": "hdf5_lz4",
                    "block_size": "5m",
                    "variant": "tuned_h5_5m",
                    "total_samples": 11854000,
                    "wall_clock_seconds": 12.216,
                    "mib_per_sec": 170.279,
                },
            ],
        }
        template = "# Report\n\n${sections}\n"
        rendered = render_report(payload, template, Path("benchmark/results/demo.json"))
        self.assertIn("### J.1 Random Access", rendered)
        self.assertIn("| 5m | 0.0669s | 0.0553s |", rendered)
        self.assertIn("### J.4 Full-Study Sequential Read", rendered)
        self.assertIn("| 5m | 14.59s | 12.22s |", rendered)


if __name__ == "__main__":
    unittest.main()