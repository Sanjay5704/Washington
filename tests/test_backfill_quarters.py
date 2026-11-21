from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPARK_JOB = Path(__file__).parents[1] / "spark" / "backfill_form345.py"


def test_quarters_helper() -> None:
    pytest.importorskip("pyspark")  # the job module imports pyspark at top level
    spec = importlib.util.spec_from_file_location("backfill", SPARK_JOB)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    assert module.quarters("2021q3", "2022q2") == ["2021q3", "2021q4", "2022q1", "2022q2"]
    assert len(module.quarters("2021q2", "2026q1")) == 20
