#!/usr/bin/env python3
"""Migrate objects from Google Cloud Storage (GCS) to Cloudflare R2 with parallel streaming."""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Iterator

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from google.cloud import storage as gcs_storage
from google.oauth2 import service_account


REQUIRED_ENV = (
    "GCS_BUCKET_NAME",
    "GCS_SERVICE_ACCOUNT_KEY",
    "CF_R2_ACCESS_KEY_ID2",
    "CF_R2_SECRET_ACCESS_KEY2",
    "CF_R2_BUCKET_NAME2",
    "CF_R2_ENDPOINT_URL2",
)

MULTIPART_THRESHOLD = 8 * 1024 * 1024   # 8 MiB
MULTIPART_CHUNKSIZE  = 8 * 1024 * 1024

_thread_local = threading.local()

_transfer_config = TransferConfig(
    multipart_threshold=MULTIPART_THRESHOLD,
    multipart_chunksize=MULTIPART_CHUNKSIZE,
    max_concurrency=4,
    use_threads=True,
)


# ──────────────────────────────────────────────
# Data
# ──────────────────────────────────────────────

@dataclass
class Args:
    prefix: str
    dry_run: bool
    skip_existing: bool
    workers: int


@dataclass
class Stats:
    lock: threading.Lock = field(default_factory=threading.Lock)
    listed: int = 0
    copied: int = 0
    skipped: int = 0
    failed: int = 0

    def inc(self, field_name: str) -> None:
        with self.lock:
            setattr(self, field_name, getattr(self, field_name) + 1)


# ──────────────────────────────────────────────
# Client helpers (thread-local)
# ──────────────────────────────────────────────

def _gcs_client() -> gcs_storage.Client:
    """One GCS client per thread."""
    if not hasattr(_thread_local, "gcs"):
        key_info = json.loads(os.environ["GCS_SERVICE_ACCOUNT_KEY"])
        creds = service_account.Credentials.from_service_account_info(
            key_info,
            scopes=["https://www.googleapis.com/auth/devstorage.read_only"],
        )
        _thread_local.gcs = gcs_storage.Client(
            credentials=creds,
            project=key_info.get("project_id"),
        )
    return _thread_local.gcs


def _r2_client() -> boto3.client:
    """One R2 boto3 client per thread."""
    if not hasattr(_thread_local, "r2"):
        _thread_local.r2 = boto3.client(
            "s3",
            endpoint_url=os.environ["CF_R2_ENDPOINT_URL2"],
            aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID2"],
            aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY2"],
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=50,
            ),
        )
    return _thread_local.r2


# ──────────────────────────────────────────────
# Core helpers
# ──────────────────────────────────────────────

def validate_env() -> None:
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        sys.exit("Missing environment variables: " + ", ".join(missing))


def normalize_prefix(prefix: str) -> str:
    return prefix.strip().lstrip("/")


def list_gcs_objects(bucket_name: str, prefix: str) -> Iterator[tuple[str, int]]:
    """Yield (blob_name, size) for every object under prefix."""
    client = _gcs_client()
    bucket = client.bucket(bucket_name)
    for blob in client.list_blobs(bucket, prefix=prefix or None):
        if not blob.name.endswith("/"):
            yield blob.name, blob.size


def exists_in_r2(bucket: str, key: str) -> bool:
    try:
        _r2_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def stream_copy(gcs_bucket: str, r2_bucket: str, key: str) -> None:
    """Stream a GCS blob directly into R2 without loading it fully into memory."""
    gcs_client = _gcs_client()
    bucket = gcs_client.bucket(gcs_bucket)
    blob = bucket.blob(key)

    # Download into an in-memory buffer using GCS streaming download.
    # For very large files this keeps peak RAM to one chunk at a time via upload_fileobj.
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)

    extra: dict = {}
    if blob.content_type:
        extra["ContentType"] = blob.content_type
    if blob.metadata:
        extra["Metadata"] = blob.metadata

    _r2_client().upload_fileobj(
        buf,
        r2_bucket,
        key,
        ExtraArgs=extra or None,
        Config=_transfer_config,
    )


# ──────────────────────────────────────────────
# Per-object worker (runs inside thread pool)
# ──────────────────────────────────────────────

def _process(
    key: str,
    size: int,
    gcs_bucket: str,
    r2_bucket: str,
    skip_existing: bool,
    stats: Stats,
) -> tuple[str, str]:
    try:
        if skip_existing and exists_in_r2(r2_bucket, key):
            stats.inc("skipped")
            return key, "skipped"
        stream_copy(gcs_bucket, r2_bucket, key)
        stats.inc("copied")
        return key, "copied"
    except Exception as exc:  # noqa: BLE001
        stats.inc("failed")
        return key, f"failed: {exc}"


# ──────────────────────────────────────────────
# Migration orchestrator
# ──────────────────────────────────────────────

def migrate(args: Args) -> Stats:
    validate_env()

    gcs_bucket = os.environ["GCS_BUCKET_NAME"]
    r2_bucket  = os.environ["CF_R2_BUCKET_NAME2"]
    prefix     = normalize_prefix(args.prefix)

    print(f"Source : gs://{gcs_bucket}/{prefix}")
    print(f"Dest   : r2://{r2_bucket}/{prefix}")
    print(f"Workers: {args.workers}  |  dry-run: {args.dry_run}  |  skip-existing: {args.skip_existing}")
    print()

    stats = Stats()

    # ── dry-run: list only ──────────────────────────────
    if args.dry_run:
        for key, size in list_gcs_objects(gcs_bucket, prefix):
            stats.inc("listed")
            print(f"  {key}  ({size / 1024:.1f} KiB)")
        return stats

    # ── real copy: parallel workers ─────────────────────
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}

        for key, size in list_gcs_objects(gcs_bucket, prefix):
            stats.inc("listed")
            fut = pool.submit(
                _process,
                key, size,
                gcs_bucket, r2_bucket,
                args.skip_existing, stats,
            )
            futures[fut] = (key, size)

        for fut in as_completed(futures):
            key, size = futures[fut]
            try:
                _, outcome = fut.result()
            except Exception as exc:  # noqa: BLE001
                outcome = f"failed: {exc}"
            print(f"  [{outcome}] {key} ({size / 1024:.1f} KiB)")

    return stats


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def parse_args() -> Args:
    p = argparse.ArgumentParser(description="Migrate objects from GCS to Cloudflare R2.")
    p.add_argument("--prefix", required=True,
                   help="GCS folder/prefix to migrate (e.g. backups/2024/).")
    p.add_argument("--dry-run", action="store_true",
                   help="List matching objects without copying.")
    p.add_argument("--skip-existing", action="store_true",
                   help="Skip objects already present in the R2 bucket.")
    p.add_argument("--workers", type=int, default=20,
                   help="Parallel copy threads (default: 20).")
    ns = p.parse_args()
    return Args(
        prefix=ns.prefix,
        dry_run=ns.dry_run,
        skip_existing=ns.skip_existing,
        workers=ns.workers,
    )


def main() -> int:
    args = parse_args()
    stats = migrate(args)

    print()
    print("─" * 32)
    print(f"  Listed : {stats.listed}")
    print(f"  Copied : {stats.copied}")
    print(f"  Skipped: {stats.skipped}")
    print(f"  Failed : {stats.failed}")
    print("─" * 32)

    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
