#!/usr/bin/env python3
"""Copy objects from a Cloudflare R2 bucket prefix to an AWS S3 bucket."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import boto3
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


@dataclass
class MigrationStats:
    listed: int = 0
    copied: int = 0
    skipped: int = 0
    failed: int = 0


def normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def validate_env() -> None:
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        print("Missing required environment variables:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)


def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CF_R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
    )


def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def iter_r2_objects(client, bucket: str, prefix: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            yield key, obj.get("Size", 0)


def s3_object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def copy_object(r2, s3, source_bucket: str, dest_bucket: str, key: str) -> None:
    response = r2.get_object(Bucket=source_bucket, Key=key)
    body = response["Body"].read()

    put_args = {
        "Bucket": dest_bucket,
        "Key": key,
        "Body": body,
    }

    content_type = response.get("ContentType")
    if content_type:
        put_args["ContentType"] = content_type

    metadata = response.get("Metadata")
    if metadata:
        put_args["Metadata"] = metadata

    s3.put_object(**put_args)


def migrate(prefix: str, dry_run: bool, skip_existing: bool) -> MigrationStats:
    validate_env()

    source_bucket = os.environ["CF_R2_BUCKET_NAME"]
    dest_bucket = os.environ["S3_BUCKET_NAME"]
    prefix = normalize_prefix(prefix)

    r2 = r2_client()
    s3 = s3_client()
    stats = MigrationStats()

    print(f"Source: r2://{source_bucket}/{prefix or ''}")
    print(f"Destination: s3://{dest_bucket}/{prefix or ''}")
    print(f"Mode: {'dry-run' if dry_run else 'copy'}")
    print()

    for key, size in iter_r2_objects(r2, source_bucket, prefix):
        stats.listed += 1
        size_kb = size / 1024
        print(f"[{stats.listed}] {key} ({size_kb:.1f} KiB)")

        if dry_run:
            continue

        if skip_existing and s3_object_exists(s3, dest_bucket, key):
            print("  -> skipped (already exists in S3)")
            stats.skipped += 1
            continue

        try:
            copy_object(r2, s3, source_bucket, dest_bucket, key)
            print("  -> copied")
            stats.copied += 1
        except ClientError as exc:
            print(f"  -> failed: {exc}", file=sys.stderr)
            stats.failed += 1

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate objects from Cloudflare R2 to AWS S3.")
    parser.add_argument(
        "--prefix",
        required=True,
        help="Folder/prefix inside the R2 bucket to migrate (e.g. backups/2024/).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching objects without copying them.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip objects that already exist in the destination S3 bucket.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = migrate(prefix=args.prefix, dry_run=args.dry_run, skip_existing=args.skip_existing)

    print()
    print("Summary")
    print(f"  Listed:   {stats.listed}")
    print(f"  Copied:   {stats.copied}")
    print(f"  Skipped:  {stats.skipped}")
    print(f"  Failed:   {stats.failed}")

    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
