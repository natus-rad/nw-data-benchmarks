from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse, unquote


def _get_blob_service_client(cfg: dict, args: argparse.Namespace | None = None):
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

    sas = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    if args:
        sas = getattr(args, "sas_token", None) or sas
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


def _parse_blob_reference(input_value: str, default_container: str) -> tuple[str, str]:
    """Return ``(container, blob_path)`` for a remote input reference.

    Supported forms:
      - ``parquet/study/...`` (relative blob path inside the configured container)
      - ``https://<account>.blob.core.windows.net/<container>/<blob-path>``
      - ``azure://<container>/<blob-path>``
    """
    value = str(input_value).strip()
    if not value:
        raise ValueError("input is empty")

    if value.startswith("azure://"):
        rest = value[len("azure://"):]
        if "/" not in rest:
            raise ValueError(
                "azure:// input must be of the form azure://<container>/<blob-path>"
            )
        container, blob_path = rest.split("/", 1)
        return container, blob_path.lstrip("/")

    parsed = urlparse(value)
    if parsed.scheme in {"http", "https"}:
        path = unquote(parsed.path.lstrip("/"))
        if "/" not in path:
            raise ValueError(
                "Azure blob URL must include both container and blob path"
            )
        container, blob_path = path.split("/", 1)
        return container, blob_path

    return default_container, value.lstrip("/")


def _download_blob(container_client, blob_name: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "wb") as f:
        container_client.download_blob(blob_name).readinto(f)


def _is_directory_marker(blob, all_blob_names: set[str]) -> bool:
    name = blob.name.rstrip("/")
    if getattr(blob, "size", None) != 0:
        return False
    marker_prefix = name + "/"
    return any(other.startswith(marker_prefix) for other in all_blob_names if other != name)


def download_input_from_azure(cfg: dict, input_value: str,
                              args: argparse.Namespace) -> Path:
    """Download a remote study input (single file or blob prefix) into cache."""
    azure_cfg = cfg.get("azure", {})
    if "container" not in azure_cfg:
        raise FileNotFoundError(
            "Azure configuration is missing 'azure.container', so a remote input "
            "cannot be resolved."
        )

    default_container = azure_cfg["container"]
    container_name, blob_ref = _parse_blob_reference(input_value, default_container)
    blob_ref = blob_ref.rstrip("/")
    if not blob_ref:
        raise FileNotFoundError("Remote input path is empty after normalization.")
    cache_leaf = Path(blob_ref).name or "remote_input"

    cache_root = Path(cfg.get("cache_dir", ".benchmark_cache"))
    client = _get_blob_service_client(cfg, args)
    container_client = client.get_container_client(container_name)

    listed = list(container_client.list_blobs(name_starts_with=blob_ref))
    if not listed:
        raise FileNotFoundError(
            f"No Azure blobs found in container '{container_name}' matching '{blob_ref}'."
        )

    exact_match = next((blob for blob in listed if blob.name == blob_ref), None)
    prefix_with_slash = blob_ref + "/"
    prefix_matches = [blob for blob in listed if blob.name.startswith(prefix_with_slash)]

    treat_as_single_file = exact_match is not None and not str(input_value).endswith("/")
    if treat_as_single_file:
        local_path = cache_root / cache_leaf
        if local_path.exists():
            print(f"  [cached] remote input -> {local_path}")
            return local_path

        print(f"  [download] remote input file: {container_name}/{blob_ref}")
        _download_blob(container_client, blob_ref, local_path)
        print(f"  [download] 1 file -> {local_path}")
        return local_path

    if not prefix_matches:
        raise FileNotFoundError(
            f"Azure blob '{blob_ref}' exists as a file, but no directory-style input "
            f"was found under '{blob_ref}/'. Add a trailing slash only for prefixes."
        )

    local_root = cache_root / cache_leaf
    sentinel = local_root / ".download_complete"
    if sentinel.exists() and any(p.is_file() for p in local_root.rglob("*") if p != sentinel):
        print(f"  [cached] remote input -> {local_root}")
        return local_root

    print(f"  [download] remote input prefix: {container_name}/{blob_ref}/")
    local_root.mkdir(parents=True, exist_ok=True)
    count = 0
    skipped = 0
    all_blob_names = {blob.name.rstrip("/") for blob in prefix_matches}
    for blob in prefix_matches:
        rel = blob.name[len(prefix_with_slash):].lstrip("/")
        if not rel:
            continue
        if _is_directory_marker(blob, all_blob_names):
            print(f"    [skip marker] {rel}")
            skipped += 1
            continue
        local_path = local_root / rel
        if local_path.parent.exists() and local_path.parent.is_file():
            raise RuntimeError(
                f"Remote input has a real file/directory conflict at '{rel}'. "
                "A parent path already exists as a file in the local cache."
            )
        if local_path.exists() and local_path.is_dir():
            raise RuntimeError(
                f"Remote input has a real file/directory conflict at '{rel}'. "
                "The target local path already exists as a directory."
            )
        _download_blob(container_client, blob.name, local_path)
        count += 1
        size = getattr(blob, "size", 0) / 1024 / 1024
        print(f"    {rel} ({size:.1f} MiB)")
    sentinel.write_text("ok\n", encoding="utf-8")
    summary = f"  [download] {count} files -> {local_root}"
    if skipped:
        summary += f" ({skipped} marker blob(s) skipped)"
    print(summary)
    return local_root


def resolve_input_path(cfg: dict, study: dict,
                       args: argparse.Namespace) -> Path:
    """Resolve ``study['input']`` to a local path.

    If the input already exists on disk, it is used directly.
    Otherwise the value is treated as a remote Azure blob path/URL and is
    downloaded into the benchmark cache.
    """
    if "input" not in study:
        name = study.get("name", "<unnamed>")
        raise ValueError(
            f"Study '{name}' must define 'input'. Legacy study configs are no longer "
            "supported; migrate source/local_path/remote_parquet_url/blob_prefix to "
            "the universal input format."
        )

    input_value = str(study["input"]).strip()
    local_path = Path(input_value).expanduser()
    if local_path.exists():
        return local_path

    try:
        return download_input_from_azure(cfg, input_value, args)
    except Exception as exc:
        name = study.get("name", "<unnamed>")
        raise FileNotFoundError(
            f"Study '{name}' input '{input_value}' was not found locally and could not "
            f"be resolved as a remote Azure blob path: {exc}"
        ) from exc


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
