"""Bronze layer: raw files exactly as SEC served them, in MinIO (S3 API).

Key layout (Hive-style partitions make it easy to list or reprocess one day):
    edgar/daily-index/filing_date=2026-11-20/master.idx
    edgar/form4/filing_date=2026-11-20/0001193125-26-470298.xml
    staging/form4-parsed/filing_date=2026-11-20/records.jsonl
    sec-datasets/form345/quarter=2026q1/2026q1_form345.zip
"""

from __future__ import annotations

from datetime import date
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from washington.config import Settings


def index_key(day: date) -> str:
    return f"edgar/daily-index/filing_date={day.isoformat()}/master.idx"


def filings_prefix(day: date) -> str:
    return f"edgar/form4/filing_date={day.isoformat()}/"


def filing_key(day: date, accession_no: str) -> str:
    return f"{filings_prefix(day)}{accession_no}.xml"


def parsed_key(day: date) -> str:
    return f"staging/form4-parsed/filing_date={day.isoformat()}/records.jsonl"


def dataset_key(quarter: str) -> str:
    return f"sec-datasets/form345/quarter={quarter}/{quarter}_form345.zip"


class BronzeStore:
    def __init__(self, s3_client: Any, bucket: str) -> None:
        self.s3 = s3_client
        self.bucket = bucket

    @classmethod
    def from_settings(cls, settings: Settings) -> BronzeStore:
        client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        )
        return cls(client, settings.bronze_bucket)

    def ensure_bucket(self) -> None:
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.s3.create_bucket(Bucket=self.bucket)

    def put_text(self, key: str, text: str, content_type: str = "text/plain") -> None:
        self.s3.put_object(
            Bucket=self.bucket, Key=key, Body=text.encode("utf-8"), ContentType=content_type
        )

    def put_bytes(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def get_text(self, key: str) -> str:
        return self.get_bytes(key).decode("utf-8")

    def get_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys
