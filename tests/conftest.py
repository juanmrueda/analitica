from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import dashboard


def _build_sample_report_payload() -> dict[str, Any]:
    return {
        "daily": [
            {
                "date": "2026-01-01",
                "meta": {"spend": 10, "clicks": 5, "conversions": 2, "impressions": 100},
                "google_ads": {"cost": 20, "clicks": 8, "conversions": 1, "impressions": 200},
                "ga4": {
                    "sessions": 30,
                    "totalUsers": 25,
                    "averageSessionDuration": 80,
                    "bounceRate": 0.40,
                },
                "normalization": {},
            },
            {
                "date": "2026-01-02",
                "meta": {"spend": 11, "clicks": 6, "conversions": 2, "impressions": 120},
                "google_ads": {"cost": 19, "clicks": 9, "conversions": 2, "impressions": 220},
                "ga4": {
                    "sessions": 33,
                    "totalUsers": 27,
                    "averageSessionDuration": 82,
                    "bounceRate": 0.39,
                },
                "normalization": {},
            },
        ],
        "hourly": [
            {
                "datetime": "2026-01-01 09:00:00",
                "date": "2026-01-01",
                "hour": 9,
                "meta": {"spend": 3, "clicks": 1, "conversions": 1, "impressions": 25},
                "google_ads": {"cost": 6, "clicks": 2, "conversions": 0, "impressions": 40},
                "normalization": {},
            },
            {
                "datetime": "2026-01-01 10:00:00",
                "date": "2026-01-01",
                "hour": 10,
                "meta": {"spend": 2, "clicks": 1, "conversions": 0, "impressions": 20},
                "google_ads": {"cost": 4, "clicks": 1, "conversions": 1, "impressions": 38},
                "normalization": {},
            },
        ],
        "traffic_acquisition": {
            "meta_campaign_daily": [
                {
                    "date": "2026-01-01",
                    "platform": "Meta",
                    "campaign_id": "meta_1",
                    "campaign_name": "Meta One",
                    "spend": 10,
                    "impressions": 100,
                    "clicks": 5,
                    "conversions": 2,
                }
            ],
            "google_campaign_daily": [
                {
                    "date": "2026-01-01",
                    "platform": "Google",
                    "campaign_id": "google_1",
                    "campaign_name": "Google One",
                    "cost": 20,
                    "impressions": 200,
                    "clicks": 8,
                    "conversions": 1,
                    "advertising_channel_type": "SEARCH",
                }
            ],
            "paid_piece_daily": [
                {
                    "date": "2026-01-01",
                    "platform": "Meta",
                    "campaign_id": "meta_1",
                    "campaign_name": "Meta One",
                    "ad_id": "ad_1",
                    "ad_name": "Ad One",
                    "piece_id": "",
                    "piece_name": "",
                    "preview_url": "https://example.com/preview.png",
                    "spend": 10,
                    "impressions": 100,
                    "clicks": 5,
                    "conversions": 2,
                }
            ],
            "paid_device_daily": [
                {
                    "date": "2026-01-01",
                    "platform": "Meta",
                    "device": "mobile",
                    "spend": 5,
                    "impressions": 70,
                    "clicks": 3,
                    "conversions": 1,
                }
            ],
            "paid_lead_demographics_daily": [
                {
                    "date": "2026-01-01",
                    "platform": "Meta",
                    "breakdown": "age_gender",
                    "age_range": "18_24",
                    "gender": "female",
                    "leads": 2,
                    "spend": 10,
                    "impressions": 100,
                    "clicks": 5,
                }
            ],
            "paid_lead_geo_daily": [
                {
                    "date": "2026-01-01",
                    "platform": "Meta",
                    "country_code": "HN",
                    "country_name": "",
                    "region": "",
                    "leads": 2,
                    "spend": 10,
                    "impressions": 100,
                    "clicks": 5,
                }
            ],
            "ga4_channel_daily": [
                {
                    "date": "2026-01-01",
                    "sessionDefaultChannelGroup": "Paid Social",
                    "sessions": 30,
                    "conversions": 3,
                }
            ],
            "ga4_top_pages_daily": [
                {
                    "date": "2026-01-01",
                    "pagePath": "/home",
                    "pageTitle": "Home",
                    "screenPageViews": 100,
                    "sessions": 50,
                    "averageSessionDuration": 60,
                }
            ],
            "ga4_event_daily": [],
        },
    }


@pytest.fixture(autouse=True)
def clear_streamlit_cache() -> None:
    dashboard.st.cache_data.clear()
    yield
    dashboard.st.cache_data.clear()


@pytest.fixture
def sample_report_payload() -> dict[str, Any]:
    return _build_sample_report_payload()


@pytest.fixture
def report_path(tmp_path: Path, sample_report_payload: dict[str, Any]) -> Path:
    path = tmp_path / "reports" / "tenant_x" / "tenant_x_historical.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sample_report_payload), encoding="utf-8")
    return path
