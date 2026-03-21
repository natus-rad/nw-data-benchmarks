from __future__ import annotations

import argparse
import os
from pathlib import Path


def _get_blob_service_client(cfg: dict, args: argparse.Namespace):
    """Build a BlobServiceClient from config + auth.

    Priority:
      1. Anonymous access (if azure.anonymous: true in config) — no credentials needed.
      2. --sas-token flag or AZURE_STORAGE_SAS_TOKEN env var.
      3. DefaultAzureCredential (az login, managed identity, workload identity, etc.).
    """
    from azure.storage.blob import BlobServiceClient

    account = cfg["azure"]["storage_account"]
    account_url = f"https://{account}.blob.core.windows.net"

    if cfg.get("azure", {}).get("anonymous", False):
        return BlobServiceClient(account_url=account_url)

    sas = getattr(args, "sas_token", None) or os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    if sas:
        return BlobServiceClient(account_url=account_url, credential=sas)

    try:
        from azure.identity import DefaultAzureCredential
    except ImportError as exc:
        raise RuntimeError(
            "Non-anonymous Azure access without a SAS token requires the 'azure-identity' "
            "package. Install it with:\n\n"
            "    pip install azure-identity\n\n"
            "Alternatively, provide a SAS token via the --sas-token flag or the "
            "AZURE_STORAGE_SAS_TOKEN environment variable, or set azure.anonymous: true "
            "in the config if your storage account allows anonymous access."
        ) from exc
    return BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())


def download_study(cfg: dict, study: dict, args: argparse.Namespace) -> Path:
    """Download study data from Azure Blob to the local cache."""
    if "local_path" in study:
        local = Path(study["local_path"])
        if not local.exists():
            raise FileNotFoundError(f"Study local_path does not exist: {local}")
        print(f"  [local] {study['name']} -> {local}")
        return local

    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    source = study.get("source", "parquet")

    if source == "parquet":
        prefix = study["remote_parquet_url"].rstrip("/")
        study_cache = cache_dir / Path(prefix).name
        check_glob = "*.parquet"
    else:
        prefix = study["blob_prefix"].rstrip("/")
        study_cache = cache_dir / study["name"]
        check_glob = "*.erd"

    if study_cache.exists() and any(study_cache.rglob(check_glob)):
        print(f"  [cached] {study['name']} -> {study_cache}")
        return study_cache

    study_cache.mkdir(parents=True, exist_ok=True)
    container = cfg["azure"]["container"]

    print(f"  [download] {study['name']} from {container}/{prefix} ...")
    client = _get_blob_service_client(cfg, args)
    container_client = client.get_container_client(container)

    count = 0
    for blob in container_client.list_blobs(name_starts_with=prefix):
        rel = blob.name[len(prefix):].lstrip("/")
        if not rel:
            continue
        local_path = study_cache / rel
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            container_client.download_blob(blob).readinto(f)
        count += 1
        print(f"    {rel} ({blob.size / 1024 / 1024:.1f} MiB)")
    print(f"  [download] {count} files -> {study_cache}")
    return study_cache


def _download_edf_from_azure(cfg: dict, edf_blob_path: str,
                             args: argparse.Namespace) -> tuple[float, Path]:
    """Download full EDF from Azure, return (seconds, local_path)."""
    import time

    cache_dir = Path(cfg.get("cache_dir", ".benchmark_cache"))
    local_path = cache_dir / "remote_edf_download" / Path(edf_blob_path).name
    local_path.parent.mkdir(parents=True, exist_ok=True)

    client = _get_blob_service_client(cfg, args)
    container = cfg["azure"]["container"]
    container_client = client.get_container_client(container)

    t0 = time.perf_counter()
    with open(local_path, "wb") as f:
        container_client.download_blob(edf_blob_path).readinto(f)
    elapsed = time.perf_counter() - t0

    return elapsed, local_path
