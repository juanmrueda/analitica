from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

import dashboard


def _write_parquet_dataset(report_path: Path, dataset_key: str, rows: list[dict[str, Any]]) -> Path:
    parquet_dir = report_path.parent / dashboard.REPORT_PARQUET_DIRNAME
    parquet_dir.mkdir(parents=True, exist_ok=True)
    path = parquet_dir / f"{dataset_key}.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False, engine="pyarrow")
    return path


def _write_report_payload(report_path: Path, payload: dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_daily_df_json_fallback(report_path: Path) -> None:
    df = dashboard.load_daily_df_from_report_path(report_path)
    assert list(df["date"]) == [date(2026, 1, 1), date(2026, 1, 2)]
    assert float(df["total_spend"].sum()) == 60.0
    assert {"meta_spend", "google_spend", "total_conv", "ga4_sessions"}.issubset(df.columns)


def test_load_daily_df_prefers_parquet(report_path: Path) -> None:
    _write_parquet_dataset(
        report_path,
        "daily",
        [
            {
                "date": "2026-01-05",
                "meta_spend": 0,
                "google_spend": 99,
                "total_spend": 99,
                "meta_clicks": 0,
                "google_clicks": 0,
                "total_clicks": 0,
                "meta_conv": 0,
                "google_conv": 0,
                "total_conv": 0,
                "meta_impr": 0,
                "google_impr": 0,
                "total_impr": 0,
                "ga4_sessions": 0,
                "ga4_users": 0,
                "ga4_avg_sess": 0,
                "ga4_bounce": 0,
            }
        ],
    )
    df = dashboard.load_daily_df_from_report_path(report_path)
    assert len(df) == 1
    assert list(df["date"]) == [date(2026, 1, 5)]
    assert float(df.iloc[0]["total_spend"]) == 99.0


def test_load_hourly_df_prefers_parquet(report_path: Path) -> None:
    _write_parquet_dataset(
        report_path,
        "hourly",
        [
            {
                "timestamp": "2026-01-03 07:00:00",
                "date": "2026-01-03",
                "hour": 7,
                "meta_spend": 1,
                "google_spend": 2,
                "total_spend": 3,
                "meta_clicks": 1,
                "google_clicks": 1,
                "total_clicks": 2,
                "meta_conv": 0,
                "google_conv": 1,
                "total_conv": 1,
                "meta_impr": 10,
                "google_impr": 20,
                "total_impr": 30,
                "ga4_sessions": 0,
                "ga4_users": 0,
                "ga4_avg_sess": 0,
                "ga4_bounce": 0,
            }
        ],
    )
    df = dashboard.load_hourly_df_from_report_path(report_path)
    assert len(df) == 1
    assert list(df["date"]) == [date(2026, 1, 3)]
    assert int(df.iloc[0]["hour"]) == 7
    assert float(df.iloc[0]["total_spend"]) == 3.0


def test_campaign_unified_fallback_builds_from_raw(report_path: Path) -> None:
    df = dashboard.load_campaign_unified_df_from_report_path(report_path)
    assert not df.empty
    assert {"Meta", "Google"} == set(df["platform"].astype(str))
    google_row = df[df["platform"] == "Google"].iloc[0]
    assert float(google_row["spend"]) == 20.0
    assert {"ctr", "cpc", "cpl"}.issubset(df.columns)


def test_campaign_unified_prefers_prebuilt_parquet(report_path: Path) -> None:
    _write_parquet_dataset(
        report_path,
        dashboard.PARQUET_CAMPAIGN_UNIFIED_DATASET,
        [
            {
                "date": "2026-01-10",
                "platform": "Google",
                "campaign_id": "pq_campaign",
                "campaign_name": "Campaign PQ",
                "spend": 77,
                "impressions": 700,
                "clicks": 35,
                "conversions": 7,
            }
        ],
    )
    df = dashboard.load_campaign_unified_df_from_report_path(report_path)
    assert len(df) == 1
    assert str(df.iloc[0]["campaign_id"]) == "pq_campaign"
    assert float(df.iloc[0]["spend"]) == 77.0


def test_piece_enriched_fallback_uses_paid_piece_daily(report_path: Path) -> None:
    df = dashboard.load_piece_enriched_df_from_report_path(report_path)
    assert not df.empty
    row = df.iloc[0]
    assert str(row["piece_id"]) == "ad_1"
    assert str(row["piece_name"]) == "Ad One"
    assert str(row["preview_image"]) == "https://example.com/preview.png"
    assert float(row["cpl"]) == 5.0


def test_piece_enriched_prefers_prebuilt_parquet(report_path: Path) -> None:
    _write_parquet_dataset(
        report_path,
        dashboard.PARQUET_PIECE_ENRICHED_DATASET,
        [
            {
                "date": "2026-01-12",
                "platform": "Meta",
                "campaign_id": "meta_99",
                "campaign_name": "Meta PQ",
                "piece_id": "",
                "piece_name": "",
                "ad_id": "ad_pq",
                "ad_name": "Ad PQ",
                "preview_image": "",
                "preview_url": "https://example.com/pq.png",
                "image_url": "",
                "thumbnail_url": "",
                "spend": 24,
                "impressions": 120,
                "clicks": 12,
                "conversions": 4,
            }
        ],
    )
    df = dashboard.load_piece_enriched_df_from_report_path(report_path)
    assert len(df) == 1
    row = df.iloc[0]
    assert str(row["piece_id"]) == "ad_pq"
    assert str(row["piece_name"]) == "Ad PQ"
    assert str(row["preview_image"]) == "https://example.com/pq.png"
    assert float(row["cpl"]) == 6.0


def test_daily_loader_falls_back_when_parquet_dataset_missing(report_path: Path, sample_report_payload: dict[str, Any]) -> None:
    # Create an unrelated parquet dataset to ensure daily loader still falls back to JSON.
    _write_parquet_dataset(
        report_path,
        "meta_campaign_daily",
        [{"date": "2026-01-01", "campaign_id": "meta_1", "spend": 10}],
    )
    _write_report_payload(report_path, sample_report_payload)
    df = dashboard.load_daily_df_from_report_path(report_path)
    assert len(df) == 2
    assert float(df["total_spend"].sum()) == 60.0
