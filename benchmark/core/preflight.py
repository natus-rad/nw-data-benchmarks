"""Preflight environment checks run before a benchmark session starts.

Fails fast on missing disk space or unreachable Azure storage instead of
letting a long benchmark run die partway through. Skip with --skip-preflight.
"""
from __future__ import annotations

import shutil
import urllib.error
import urllib.request
from pathlib import Path

from .constants import Category

# Downloaded inputs are re-materialized as canonical Parquet plus generated
# variants, so budget a multiple of the raw download size.
_WORKING_SET_MULTIPLIER = 3
_AZURE_PROBE_TIMEOUT_SEC = 10


class PreflightError(RuntimeError):
    """Raised when the environment cannot support the requested run."""


def _studies_needing_download(cfg: dict) -> list[dict]:
    studies = []
    for study in cfg.get("studies", []):
        if study.get("remote_only"):
            continue
        input_value = str(study.get("input", "")).strip()
        if input_value and not Path(input_value).expanduser().exists():
            studies.append(study)
    return studies


def _check_disk_space(cfg: dict) -> str:
    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    probe_dir = cache_dir if cache_dir.exists() else Path.cwd()
    free_mib = shutil.disk_usage(probe_dir).free / (1024 * 1024)

    needed_mib = 0.0
    unsized = []
    for study in _studies_needing_download(cfg):
        approx = study.get("approx_download_mib")
        if isinstance(approx, (int, float)) and approx > 0:
            needed_mib += float(approx) * _WORKING_SET_MULTIPLIER
        else:
            unsized.append(str(study.get("name", "<unnamed>")))

    if needed_mib and free_mib < needed_mib:
        raise PreflightError(
            f"Insufficient disk space for cache dir '{cache_dir}': "
            f"{free_mib:,.0f} MiB free, but the selected studies need roughly "
            f"{needed_mib:,.0f} MiB (download size x{_WORKING_SET_MULTIPLIER} for canonical "
            "Parquet and generated variants). Free up space, choose a smaller "
            "config, or rerun with --skip-preflight to proceed anyway."
        )
    summary = f"disk ok: {free_mib:,.0f} MiB free"
    if needed_mib:
        summary += f", ~{needed_mib:,.0f} MiB estimated for downloads and variants"
    if unsized:
        summary += f" (no size estimate for: {', '.join(unsized)})"
    return summary


def _needs_azure(cfg: dict, selected_ids: set[str]) -> bool:
    if any(study.get("remote_only") for study in cfg.get("studies", [])):
        return True
    if _studies_needing_download(cfg):
        return True
    return Category.REMOTE_QUERY in selected_ids


def _check_azure_reachable(cfg: dict) -> str:
    azure_cfg = cfg.get("azure") or {}
    account = azure_cfg.get("storage_account")
    if not account:
        # Input resolution fails fast with its own message in this case, so a
        # warning is enough here.
        return (
            "warn: run appears to need Azure blob access but no "
            "azure.storage_account is configured; remote inputs will fail to resolve"
        )
    url = f"https://{account}.blob.core.windows.net/"
    request = urllib.request.Request(url, method="HEAD")
    try:
        urllib.request.urlopen(request, timeout=_AZURE_PROBE_TIMEOUT_SEC)
    except urllib.error.HTTPError:
        # Any HTTP response (400/403/...) proves DNS + TLS + service reachability;
        # anonymous HEAD on the account root is expected to be rejected.
        pass
    except OSError as exc:
        raise PreflightError(
            f"Azure storage account '{account}' is not reachable at {url}: {exc}. "
            "Check network connectivity, or rerun with --skip-preflight if the "
            "run only uses local inputs."
        ) from exc
    return f"azure ok: {account} reachable"


def run_preflight_checks(cfg: dict, selected_ids: set[str]) -> None:
    """Validate disk space and Azure reachability for the planned run.

    Raises PreflightError with an actionable message on the first failed check.
    """
    print("\nPreflight checks:")
    print(f"  [preflight] {_check_disk_space(cfg)}")
    if _needs_azure(cfg, selected_ids):
        print(f"  [preflight] {_check_azure_reachable(cfg)}")
    else:
        print("  [preflight] azure: not required for this run (all inputs local)")


__all__ = ["PreflightError", "run_preflight_checks"]
