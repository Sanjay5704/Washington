from __future__ import annotations

from datetime import date

from washington import bronze


def test_bronze_keys_are_partitioned_by_filing_date() -> None:
    day = date(2026, 11, 20)
    assert bronze.index_key(day) == "edgar/daily-index/filing_date=2026-11-20/master.idx"
    assert bronze.filing_key(day, "0001-26-1") == "edgar/form4/filing_date=2026-11-20/0001-26-1.xml"
    assert bronze.filing_key(day, "x").startswith(bronze.filings_prefix(day))
    assert bronze.dataset_key("2026q1") == "sec-datasets/form345/quarter=2026q1/2026q1_form345.zip"


class FakeS3:
    """Tiny in-memory stand-in for the boto3 S3 client."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_: object) -> None:  # noqa: N803
        self.objects[Key] = Body

    def get_paginator(self, name: str) -> FakeS3:
        return self

    def paginate(self, Bucket: str, Prefix: str):  # noqa: N803
        yield {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)]}


def test_list_keys_by_prefix() -> None:
    store = bronze.BronzeStore(FakeS3(), "bronze")
    day = date(2026, 11, 20)
    store.put_text(bronze.filing_key(day, "a"), "<x/>")
    store.put_text(bronze.filing_key(date(2026, 11, 19), "b"), "<x/>")
    assert store.list_keys(bronze.filings_prefix(day)) == [bronze.filing_key(day, "a")]
