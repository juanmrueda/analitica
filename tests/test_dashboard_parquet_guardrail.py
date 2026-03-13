from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

import dashboard


def _create_report(path: Path) -> None:
    payload = {"daily": [{"date": "2026-01-01"}], "traffic_acquisition": {}, "hourly": []}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _create_core_parquet_bundle(report_path: Path) -> None:
    bundle_dir = report_path.parent / dashboard.REPORT_PARQUET_DIRNAME
    bundle_dir.mkdir(parents=True, exist_ok=True)
    for dataset_key in dashboard.PARQUET_CORE_DATASETS:
        parquet_path = bundle_dir / f"{dataset_key}.parquet"
        pd.DataFrame([{"date": "2026-01-01"}]).to_parquet(parquet_path, index=False, engine="pyarrow")


def test_parquet_bundle_health_missing_bundle(tmp_path: Path) -> None:
    report_path = tmp_path / "reports" / "tenant_a" / "tenant_a_historical.json"
    _create_report(report_path)
    health = dashboard._parquet_bundle_health(report_path)
    assert not health["ok"]
    assert "daily" in health["missing"]


def test_parquet_bundle_health_ok(tmp_path: Path) -> None:
    report_path = tmp_path / "reports" / "tenant_b" / "tenant_b_historical.json"
    _create_report(report_path)
    _create_core_parquet_bundle(report_path)
    health = dashboard._parquet_bundle_health(report_path)
    assert health["ok"]
    assert health["missing"] == []
    assert health["stale"] == []


def test_parquet_bundle_health_detects_stale(tmp_path: Path) -> None:
    report_path = tmp_path / "reports" / "tenant_c" / "tenant_c_historical.json"
    _create_report(report_path)
    _create_core_parquet_bundle(report_path)
    time.sleep(0.01)
    # Touch report after parquet files so guardrail marks stale datasets.
    _create_report(report_path)
    previous_tolerance = dashboard.PARQUET_STALE_TOLERANCE_NS
    dashboard.PARQUET_STALE_TOLERANCE_NS = 0
    try:
        health = dashboard._parquet_bundle_health(report_path)
    finally:
        dashboard.PARQUET_STALE_TOLERANCE_NS = previous_tolerance
    assert not health["ok"]
    assert len(health["stale"]) >= 1
