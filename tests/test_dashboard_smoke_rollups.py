from __future__ import annotations

from pathlib import Path

import dashboard


def _iso_bounds(report_path: Path) -> tuple[str, str]:
    daily = dashboard.load_daily_df_from_report_path(report_path)
    start_iso = daily["date"].min().isoformat()
    end_iso = daily["date"].max().isoformat()
    return start_iso, end_iso


def test_cached_rollups_smoke(report_path: Path) -> None:
    path_str, modified_ns, size_bytes = dashboard._report_cache_signature(report_path)
    start_iso, end_iso = _iso_bounds(report_path)
    filter_key = dashboard._campaign_filters_cache_key({})

    channels = dashboard._cached_channels_roll_from_report(
        path_str, modified_ns, size_bytes, start_iso, end_iso
    )
    assert not channels.empty
    assert {"sessionDefaultChannelGroup", "sessions", "conversions"}.issubset(channels.columns)

    pages = dashboard._cached_top_pages_roll_from_report(
        path_str, modified_ns, size_bytes, start_iso, end_iso
    )
    assert not pages.empty
    assert {"pagePath", "pageTitle", "views", "sessions", "avg_session"}.issubset(pages.columns)

    campaigns = dashboard._cached_campaign_roll_from_report(
        path_str, modified_ns, size_bytes, start_iso, end_iso, "All", filter_key
    )
    assert not campaigns.empty
    assert {"platform", "campaign_id", "campaign_name", "spend", "conversions", "cpl"}.issubset(campaigns.columns)

    pieces = dashboard._cached_top_pieces_roll_from_report(
        path_str, modified_ns, size_bytes, start_iso, end_iso, "All", filter_key
    )
    assert not pieces.empty
    assert {"platform", "piece_id", "piece_name", "inversion", "conversiones", "cpl"}.issubset(pieces.columns)


def test_cached_campaign_filter_values_smoke(report_path: Path) -> None:
    path_str, modified_ns, size_bytes = dashboard._report_cache_signature(report_path)
    start_iso, end_iso = _iso_bounds(report_path)
    values = dashboard._cached_campaign_filter_values_from_report(
        path_str,
        modified_ns,
        size_bytes,
        "campaign_name",
        "All",
        start_iso,
        end_iso,
    )
    assert "Google One" in values
    assert "Meta One" in values
