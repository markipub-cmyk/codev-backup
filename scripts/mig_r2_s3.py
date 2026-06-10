#!/usr/bin/env python3
"""Migrate objects from Cloudflare R2 to AWS S3 with parallel streaming transfers."""

from __future__ import annotations

import argparse
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


REQUIRED_ENV = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "CF_R2_ACCESS_KEY_ID",
    "CF_R2_SECRET_ACCESS_KEY",
    "CF_R2_BUCKET_NAME",
    "CF_R2_ENDPOINT_URL",
    "S3_BUCKET_NAME",
)

# 8 MiB multipart chunk; files above this are uploaded in parallel parts.
MULTIPART_THRESHOLD = 8 * 1024 * 1024
MULTIPART_CHUNKSIZE = 8 * 1024 * 1024

# boto3 S3 clients are not thread-safe; keep one per thread.
_thread_local = threading.local()


# ──────────────────────────────────────────────
# Data
# ──────────────────────────────────────────────

@dataclass
class Args:
    prefixes: list[str]
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

def _r2() -> boto3.client:
    if not hasattr(_thread_local, "r2"):
        _thread_local.r2 = boto3.client(
            "s3",
            endpoint_url=os.environ["CF_R2_ENDPOINT_URL"],
            aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=50,
            ),
        )
    return _thread_local.r2


def _s3() -> boto3.client:
    if not hasattr(_thread_local, "s3"):
        _thread_local.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            config=Config(
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=50,
            ),
        )
    return _thread_local.s3


_transfer_config = TransferConfig(
    multipart_threshold=MULTIPART_THRESHOLD,
    multipart_chunksize=MULTIPART_CHUNKSIZE,
    max_concurrency=4,         # parallel parts per large file
    use_threads=True,
)


# ──────────────────────────────────────────────
# Core helpers
# ──────────────────────────────────────────────

def normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def validate_env() -> None:
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        sys.exit("Missing environment variables: " + ", ".join(missing))


def list_objects(bucket: str, prefix: str) -> Iterator[tuple[str, int]]:
    """Yield (key, size) for every real object under prefix."""
    paginator = _r2().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                yield key, obj.get("Size", 0)


def exists_in_s3(bucket: str, key: str) -> bool:
    try:
        _s3().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def stream_copy(source_bucket: str, dest_bucket: str, key: str) -> None:
    """Stream an object from R2 straight into S3 (no full-memory buffer)."""
    response = _r2().get_object(Bucket=source_bucket, Key=key)
    body = response["Body"]          # streaming; not yet read into memory

    extra: dict = {}
    ct = response.get("ContentType")
    if ct:
        extra["ContentType"] = ct
    meta = response.get("Metadata")
    if meta:
        extra["Metadata"] = meta

    _s3().upload_fileobj(
        body,
        dest_bucket,
        key,
        ExtraArgs=extra or None,
        Config=_transfer_config,
    )


# ──────────────────────────────────────────────
# Per-object task (runs in thread pool)
# ──────────────────────────────────────────────

def _process(
    key: str,
    size: int,
    source_bucket: str,
    dest_bucket: str,
    skip_existing: bool,
    stats: Stats,
) -> tuple[str, str]:
    """Returns (key, outcome) where outcome is 'copied' | 'skipped' | 'failed:<msg>'."""
    try:
        if skip_existing and exists_in_s3(dest_bucket, key):
            stats.inc("skipped")
            return key, "skipped"
        stream_copy(source_bucket, dest_bucket, key)
        stats.inc("copied")
        return key, "copied"
    except Exception as exc:  # noqa: BLE001
        stats.inc("failed")
        return key, f"failed: {exc}"


# ──────────────────────────────────────────────
# Main migration
# ──────────────────────────────────────────────

def migrate(args: Args) -> Stats:
    validate_env()

    source_bucket = os.environ["CF_R2_BUCKET_NAME"]
    dest_bucket = os.environ["S3_BUCKET_NAME"]
    prefixes = [normalize_prefix(p) for p in args.prefixes]

    print(f"Workers: {args.workers}  |  dry-run: {args.dry_run}  |  skip-existing: {args.skip_existing}")
    print(f"Prefixes ({len(prefixes)}):")
    for p in prefixes:
        print(f"  r2://{source_bucket}/{p}  →  s3://{dest_bucket}/{p}")
    print()

    stats = Stats()

    for prefix in prefixes:
        print(f"── prefix: {prefix or '(root)'} ──")

        # ── dry-run: just list ──────────────────────────────
        if args.dry_run:
            for key, size in list_objects(source_bucket, prefix):
                stats.inc("listed")
                print(f"  {key}  ({size / 1024:.1f} KiB)")
            continue

        # ── real copy: feed pool as objects are listed ──────
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {}

            for key, size in list_objects(source_bucket, prefix):
                stats.inc("listed")
                fut = pool.submit(
                    _process,
                    key, size,
                    source_bucket, dest_bucket,
                    args.skip_existing, stats,
                )
                futures[fut] = (key, size)

            for fut in as_completed(futures):
                key, size = futures[fut]
                try:
                    _, outcome = fut.result()
                except Exception as exc:  # noqa: BLE001
                    outcome = f"failed: {exc}"
                size_label = f"{size / 1024:.1f} KiB"
                print(f"  [{outcome}] {key} ({size_label})")

    return stats


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def parse_args() -> Args:
    p = argparse.ArgumentParser(description="Migrate objects from Cloudflare R2 to AWS S3.")
    p.add_argument("--prefix", dest="prefixes", action="append", required=True,
                   metavar="PREFIX",
                   help="R2 folder/prefix to migrate (e.g. backups/2024/). "
                        "Repeat the flag to migrate multiple prefixes in one run.")
    p.add_argument("--dry-run", action="store_true",
                   help="List matching objects without copying.")
    p.add_argument("--skip-existing", action="store_true",
                   help="Skip objects already present in the S3 bucket.")
    p.add_argument("--workers", type=int, default=20,
                   help="Parallel copy threads (default: 20).")
    ns = p.parse_args()
    return Args(
        prefixes=ns.prefixes,
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
