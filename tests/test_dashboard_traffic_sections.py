from __future__ import annotations

from datetime import date

import pandas as pd

import dashboard_traffic_sections


def _source_platform(source_medium: str) -> str:
    txt = str(source_medium or "").lower()
    if "google" in txt:
        return "Google"
    if "facebook" in txt or "instagram" in txt or "meta" in txt:
        return "Meta"
    return "Other"


def _channel_df_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "sessionDefaultChannelGroup": "Paid Search",
                "sessionSourceMedium": "google / cpc",
                "sessions": 100,
                "totalUsers": 90,
                "averageSessionDuration": 70,
                "bounceRate": 0.40,
                "conversions": 10,
            },
            {
                "date": "2026-03-01",
                "sessionDefaultChannelGroup": "Paid Social",
                "sessionSourceMedium": "facebook / cpc",
                "sessions": 80,
                "totalUsers": 75,
                "averageSessionDuration": 50,
                "bounceRate": 0.55,
                "conversions": 6,
            },
        ]
    )


def test_traffic_decision_cards_event_dynamic_conversion() -> None:
    channel_df = _channel_df_sample()
    event_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "eventName": "form_submit",
                "sessionSourceMedium": "google / cpc",
                "conversions": 25,
            },
            {
                "date": "2026-03-01",
                "eventName": "form_submit",
                "sessionSourceMedium": "facebook / cpc",
                "conversions": 8,
            },
        ]
    )
    snapshot = dashboard_traffic_sections.compute_traffic_quality_snapshot(
        channel_df=channel_df,
        event_df=event_df,
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        ga4_event_name="form_submit",
        default_ga4_event_name="fallback_event",
        source_platform_fn=_source_platform,
        fallback_metrics={"sessions": 0.0, "users": 0.0, "bounce": 0.0, "avg_sess": 0.0, "conv": 0.0},
    )
    assert snapshot["used_event_conversions"] is True
    assert float(snapshot["conv_acq"]) == 25.0
    assert float(snapshot["sessions"]) == 100.0


def test_traffic_decision_cards_fallback_global_conversion() -> None:
    channel_df = _channel_df_sample()
    snapshot = dashboard_traffic_sections.compute_traffic_quality_snapshot(
        channel_df=channel_df,
        event_df=pd.DataFrame(),
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        ga4_event_name="form_submit",
        default_ga4_event_name="fallback_event",
        source_platform_fn=_source_platform,
        fallback_metrics={"sessions": 0.0, "users": 0.0, "bounce": 0.0, "avg_sess": 0.0, "conv": 0.0},
    )
    assert snapshot["used_event_conversions"] is False
    assert float(snapshot["conv_acq"]) == 10.0


def test_traffic_platform_filter_by_source_medium() -> None:
    channel_df = _channel_df_sample()
    roll, _meta = dashboard_traffic_sections.build_source_medium_roll(
        channel_df=channel_df,
        event_df=pd.DataFrame(),
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        ga4_event_name="form_submit",
        default_ga4_event_name="fallback_event",
        source_platform_fn=_source_platform,
    )
    assert not roll.empty
    assert set(roll["source_medium"].tolist()) == {"google / cpc"}


def test_traffic_source_medium_section_handles_missing_columns() -> None:
    channel_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "sessionDefaultChannelGroup": "Paid Search",
                "sessions": 20,
                "totalUsers": 18,
                "averageSessionDuration": 40,
                "bounceRate": 0.3,
                "conversions": 2,
            }
        ]
    )
    roll, meta = dashboard_traffic_sections.build_source_medium_roll(
        channel_df=channel_df,
        event_df=pd.DataFrame(),
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        ga4_event_name="form_submit",
        default_ga4_event_name="fallback_event",
        source_platform_fn=_source_platform,
    )
    assert not roll.empty
    assert meta["attribution_limited"] is True


def test_traffic_landing_views_per_session() -> None:
    pages_df = pd.DataFrame(
        [
            {"date": "2026-03-01", "pagePath": "/a", "pageTitle": "A", "screenPageViews": 120, "sessions": 40, "averageSessionDuration": 60},
            {"date": "2026-03-01", "pagePath": "/b", "pageTitle": "B", "screenPageViews": 50, "sessions": 25, "averageSessionDuration": 45},
        ]
    )
    roll = dashboard_traffic_sections.build_top_pages_roll(
        pages_df,
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
    )
    row_a = roll.loc[roll["pagePath"] == "/a"].iloc[0]
    assert float(row_a["views_per_session"]) == 3.0


def test_traffic_metric_timeseries_uses_event_conversions() -> None:
    channel_df = _channel_df_sample()
    event_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "eventName": "form_submit",
                "sessionSourceMedium": "google / cpc",
                "conversions": 7,
            }
        ]
    )
    fallback_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "ga4_sessions": 100,
                "ga4_users": 90,
                "ga4_avg_sess": 70,
                "ga4_bounce": 0.4,
                "google_conv": 2,
                "meta_conv": 1,
                "total_conv": 3,
            }
        ]
    )
    trend, meta = dashboard_traffic_sections.build_traffic_metric_timeseries(
        channel_df=channel_df,
        event_df=event_df,
        fallback_daily_df=fallback_df,
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        ga4_event_name="form_submit",
        default_ga4_event_name="fallback_event",
        source_platform_fn=_source_platform,
        metric_key="conv_acq",
    )
    assert float(trend.iloc[0]["value"]) == 7.0
    assert meta["conversion_source_label"] == "Evento GA4: form_submit"


def test_hourly_active_users_matrix_filters_platform() -> None:
    hourly_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "dayOfWeekName": "Sunday",
                "hour": 8,
                "sessionSourceMedium": "google / cpc",
                "activeUsers": 20,
            },
            {
                "date": "2026-03-01",
                "dayOfWeekName": "Sunday",
                "hour": 8,
                "sessionSourceMedium": "facebook / cpc",
                "activeUsers": 11,
            },
        ]
    )
    matrix = dashboard_traffic_sections.build_hourly_active_users_matrix(
        hourly_df,
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        source_platform_fn=_source_platform,
    )
    assert float(matrix.loc[8, "Sunday"]) == 20.0


def test_country_users_roll_exposes_flag_and_filters_platform() -> None:
    country_df = pd.DataFrame(
        [
            {
                "date": "2026-03-01",
                "country": "Costa Rica",
                "countryId": "CR",
                "sessionSourceMedium": "google / cpc",
                "totalUsers": 30,
            },
            {
                "date": "2026-03-01",
                "country": "Colombia",
                "countryId": "CO",
                "sessionSourceMedium": "facebook / cpc",
                "totalUsers": 50,
            },
        ]
    )
    roll = dashboard_traffic_sections.build_country_users_roll(
        country_df,
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        source_platform_fn=_source_platform,
    )
    assert len(roll) == 1
    assert roll.iloc[0]["country"] == "Costa Rica"
    assert roll.iloc[0]["flag"] == "🇨🇷"


def test_tech_roll_groups_other_after_top_n() -> None:
    tech_df = pd.DataFrame(
        [
            {"date": "2026-03-01", "browser": "Chrome", "sessionSourceMedium": "google / cpc", "totalUsers": 40},
            {"date": "2026-03-01", "browser": "Safari", "sessionSourceMedium": "google / cpc", "totalUsers": 20},
            {"date": "2026-03-01", "browser": "Firefox", "sessionSourceMedium": "google / cpc", "totalUsers": 10},
            {"date": "2026-03-01", "browser": "Edge", "sessionSourceMedium": "google / cpc", "totalUsers": 5},
        ]
    )
    roll = dashboard_traffic_sections.build_tech_roll(
        tech_df,
        label_column="browser",
        platform="Google",
        start_date=date(2026, 3, 1),
        end_date=date(2026, 3, 1),
        source_platform_fn=_source_platform,
        top_n=2,
    )
    assert roll["browser"].tolist() == ["Chrome", "Safari", "Other"]
    assert float(roll.iloc[-1]["users"]) == 15.0
