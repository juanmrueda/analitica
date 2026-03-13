from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import pandas as pd

PlatformKpiSeriesFn = Callable[[pd.DataFrame, str, str], pd.Series | None]
Ga4KpiSeriesFn = Callable[[pd.DataFrame, str], pd.Series | None]
KpiTrendSubtitleFn = Callable[[str], str]


def _trend_series_values(
    series: pd.Series | None,
    *,
    force_target_len: int | None = None,
    is_single_day: bool,
    use_hourly_real: bool,
    additive: bool,
) -> list[float] | None:
    if series is None:
        return None
    if pd.api.types.is_numeric_dtype(series):
        numeric_series = series.fillna(0.0)
    else:
        numeric_series = pd.to_numeric(series, errors="coerce").fillna(0.0)
    if force_target_len is not None:
        target_len = max(int(force_target_len), 0)
        if target_len == 0:
            return []
        if numeric_series.empty:
            return []
        if len(numeric_series) == target_len:
            return [float(v) for v in numeric_series.tolist()]
        if additive:
            projected_value = float(numeric_series.sum()) / float(target_len)
        else:
            projected_value = float(numeric_series.mean())
        return [projected_value] * target_len
    if use_hourly_real or not is_single_day:
        if numeric_series.empty:
            return []
        return [float(v) for v in numeric_series.tolist()]
    target_len = 24
    if numeric_series.empty:
        return []
    if additive:
        projected_value = float(numeric_series.sum()) / float(target_len)
    else:
        projected_value = float(numeric_series.mean())
    return [projected_value] * target_len


def _hourly_compare_series_values(
    hourly_df: pd.DataFrame | None,
    *,
    x_values: list[Any],
    prefix: str,
    kpi_key: str,
    additive: bool,
    platform_kpi_series_fn: PlatformKpiSeriesFn,
) -> list[float] | None:
    if not isinstance(hourly_df, pd.DataFrame) or hourly_df.empty or not x_values:
        return None
    hd = hourly_df.copy()
    if "timestamp" not in hd.columns:
        return None
    if not pd.api.types.is_datetime64_any_dtype(hd["timestamp"]):
        hd["timestamp"] = pd.to_datetime(hd["timestamp"], errors="coerce")
    hd = hd.dropna(subset=["timestamp"]).copy()
    if hd.empty:
        return None
    hd["hour"] = pd.to_datetime(hd["timestamp"], errors="coerce").dt.hour
    metric_series = platform_kpi_series_fn(hd, prefix, kpi_key)
    if metric_series is None:
        return None
    hd["_metric_value"] = pd.to_numeric(metric_series, errors="coerce").fillna(0.0)
    if additive:
        hourly_roll = hd.groupby("hour", dropna=False)["_metric_value"].mean()
    else:
        hourly_roll = hd.groupby("hour", dropna=False)["_metric_value"].mean()
    out: list[float] = []
    for raw_x in x_values:
        ts = pd.to_datetime(raw_x, errors="coerce")
        if pd.isna(ts):
            out.append(0.0)
            continue
        out.append(float(hourly_roll.get(int(ts.hour), 0.0)))
    return out


def build_overview_trend_payload_from_frames(
    ld: pd.DataFrame,
    ld_prev: pd.DataFrame,
    hourly_candidate: pd.DataFrame | None,
    hourly_compare_candidate: pd.DataFrame | None,
    active_overview_kpi: str,
    compare_active: bool,
    *,
    paid_trend_kpi_keys: set[str] | tuple[str, ...],
    kpi_catalog: dict[str, dict[str, Any]],
    platform_kpi_series_fn: PlatformKpiSeriesFn,
    ga4_kpi_series_fn: Ga4KpiSeriesFn,
    kpi_trend_subtitle_fn: KpiTrendSubtitleFn,
) -> dict[str, Any]:
    trend_label = str(kpi_catalog.get(active_overview_kpi, {}).get("label", active_overview_kpi))
    payload: dict[str, Any] = {
        "x_values": [],
        "x_tick_format": "%d %b",
        "x_hover_format": "%d %b %Y",
        "is_single_day": False,
        "trend_subtitle": kpi_trend_subtitle_fn(active_overview_kpi),
        "hourly_projection_note": "",
        "google_current": None,
        "meta_current": None,
        "ga4_current": None,
        "google_compare": None,
        "meta_compare": None,
        "ga4_compare": None,
    }
    if ld.empty:
        return payload

    cur_df = ld.sort_values("date").copy()
    prev_df = ld_prev.sort_values("date").copy() if compare_active and not ld_prev.empty else pd.DataFrame()
    is_single_day = bool(cur_df["date"].nunique() == 1)
    payload["is_single_day"] = is_single_day

    hd_day = pd.DataFrame()
    should_try_hourly = bool(
        is_single_day
        and active_overview_kpi in paid_trend_kpi_keys
        and isinstance(hourly_candidate, pd.DataFrame)
        and not hourly_candidate.empty
    )
    if should_try_hourly:
        hd = hourly_candidate.copy()
        if "date" in hd.columns:
            date_series = hd["date"]
            if pd.api.types.is_datetime64_any_dtype(date_series):
                hd["date"] = date_series.dt.date
            else:
                hd["date"] = pd.to_datetime(date_series, errors="coerce").dt.date
        if "timestamp" in hd.columns and not pd.api.types.is_datetime64_any_dtype(hd["timestamp"]):
            hd["timestamp"] = pd.to_datetime(hd["timestamp"], errors="coerce")
        if "date" in hd.columns:
            selected_day = cur_df["date"].iloc[0]
            hd_day = hd[hd["date"] == selected_day].copy()
    use_hourly_real = bool(
        is_single_day
        and active_overview_kpi in paid_trend_kpi_keys
        and not hd_day.empty
        and "timestamp" in hd_day.columns
    )

    hd_compare = pd.DataFrame()
    should_try_hourly_compare = bool(
        compare_active
        and use_hourly_real
        and isinstance(hourly_compare_candidate, pd.DataFrame)
        and not hourly_compare_candidate.empty
    )
    if should_try_hourly_compare:
        hd_prev = hourly_compare_candidate.copy()
        if "date" in hd_prev.columns:
            prev_date_series = hd_prev["date"]
            if pd.api.types.is_datetime64_any_dtype(prev_date_series):
                hd_prev["date"] = prev_date_series.dt.date
            else:
                hd_prev["date"] = pd.to_datetime(prev_date_series, errors="coerce").dt.date
        if "timestamp" in hd_prev.columns and not pd.api.types.is_datetime64_any_dtype(hd_prev["timestamp"]):
            hd_prev["timestamp"] = pd.to_datetime(hd_prev["timestamp"], errors="coerce")
        hd_compare = hd_prev.dropna(subset=["timestamp"]).sort_values("timestamp").copy()

    payload["trend_subtitle"] = (
        ("Hourly investment over time" if active_overview_kpi == "spend" else f"Tendencia por hora de {trend_label.lower()}")
        if use_hourly_real
        else ("Vista por hora del dia seleccionado" if is_single_day else kpi_trend_subtitle_fn(active_overview_kpi))
    )

    plot_df = hd_day.sort_values("timestamp").copy() if use_hourly_real else cur_df
    if use_hourly_real:
        payload["x_values"] = list(plot_df["timestamp"])
        payload["x_tick_format"] = "%H:%M"
        payload["x_hover_format"] = "%d %b %Y %H:%M"
    elif is_single_day:
        selected_day = cur_df["date"].iloc[0]
        day_start = datetime.combine(selected_day, datetime.min.time())
        payload["x_values"] = list(pd.date_range(start=day_start, periods=24, freq="h"))
        payload["x_tick_format"] = "%H:%M"
        payload["x_hover_format"] = "%d %b %Y %H:%M"
        payload["hourly_projection_note"] = (
            "Vista horaria proyectada con distribucion uniforme del total diario "
            "(no hay datos horarios disponibles en el JSON actual)."
        )
    else:
        payload["x_values"] = list(cur_df["date"])
        payload["x_tick_format"] = "%d %b"
        payload["x_hover_format"] = "%d %b %Y"

    target_len = len(payload["x_values"])
    if target_len == 0:
        return payload

    additive = active_overview_kpi in {"spend", "conv", "clicks", "impr", "sessions", "users"}
    if active_overview_kpi in paid_trend_kpi_keys:
        payload["google_current"] = _trend_series_values(
            platform_kpi_series_fn(plot_df, "google", active_overview_kpi),
            is_single_day=is_single_day,
            use_hourly_real=use_hourly_real,
            additive=additive,
        )
        payload["meta_current"] = _trend_series_values(
            platform_kpi_series_fn(plot_df, "meta", active_overview_kpi),
            is_single_day=is_single_day,
            use_hourly_real=use_hourly_real,
            additive=additive,
        )
        if compare_active:
            google_compare_values: list[float] | None = None
            meta_compare_values: list[float] | None = None
            if use_hourly_real and not hd_compare.empty:
                google_compare_values = _hourly_compare_series_values(
                    hd_compare,
                    x_values=list(payload.get("x_values", [])),
                    prefix="google",
                    kpi_key=active_overview_kpi,
                    additive=additive,
                    platform_kpi_series_fn=platform_kpi_series_fn,
                )
                meta_compare_values = _hourly_compare_series_values(
                    hd_compare,
                    x_values=list(payload.get("x_values", [])),
                    prefix="meta",
                    kpi_key=active_overview_kpi,
                    additive=additive,
                    platform_kpi_series_fn=platform_kpi_series_fn,
                )
            if google_compare_values is None and not prev_df.empty:
                google_compare_values = _trend_series_values(
                    platform_kpi_series_fn(prev_df, "google", active_overview_kpi),
                    force_target_len=target_len,
                    is_single_day=is_single_day,
                    use_hourly_real=use_hourly_real,
                    additive=additive,
                )
            if meta_compare_values is None and not prev_df.empty:
                meta_compare_values = _trend_series_values(
                    platform_kpi_series_fn(prev_df, "meta", active_overview_kpi),
                    force_target_len=target_len,
                    is_single_day=is_single_day,
                    use_hourly_real=use_hourly_real,
                    additive=additive,
                )
            payload["google_compare"] = google_compare_values
            payload["meta_compare"] = meta_compare_values
    else:
        payload["ga4_current"] = _trend_series_values(
            ga4_kpi_series_fn(cur_df, active_overview_kpi),
            is_single_day=is_single_day,
            use_hourly_real=use_hourly_real,
            additive=additive,
        )
        if compare_active and not prev_df.empty:
            payload["ga4_compare"] = _trend_series_values(
                ga4_kpi_series_fn(prev_df, active_overview_kpi),
                force_target_len=target_len,
                is_single_day=is_single_day,
                use_hourly_real=use_hourly_real,
                additive=additive,
            )
    return payload
