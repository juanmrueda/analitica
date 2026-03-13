from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import plotly.graph_objects as go

PlatformKpiSeriesFn = Callable[[pd.DataFrame, str, str], pd.Series | None]
Ga4KpiSeriesFn = Callable[[pd.DataFrame, str], pd.Series | None]
KpiTrendSubtitleFn = Callable[[str], str]
DateRangeResolverFn = Callable[[str, str], tuple[Any, Any]]


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


def build_overview_trend_payload_from_report(
    path_str: str,
    modified_ns: int,
    size_bytes: int,
    start_iso: str,
    end_iso: str,
    prev_start_iso: str,
    prev_end_iso: str,
    active_overview_kpi: str,
    compare_active: bool,
    *,
    parquet_daily_dataset: str,
    parquet_hourly_dataset: str,
    paid_trend_kpi_keys: set[str] | tuple[str, ...],
    parquet_cache_signature_fn: Callable[[Path, str], tuple[str, int, int] | None],
    load_parquet_df_cached_fn: Callable[[str, int, int], pd.DataFrame],
    load_daily_df_cached_fn: Callable[[str, int, int], pd.DataFrame],
    load_hourly_df_cached_fn: Callable[[str, int, int], pd.DataFrame],
    normalize_daily_table_fn: Callable[[pd.DataFrame], pd.DataFrame],
    normalize_hourly_table_fn: Callable[[pd.DataFrame], pd.DataFrame],
    resolve_cached_range_fn: DateRangeResolverFn,
    build_payload_from_frames_fn: Callable[
        [pd.DataFrame, pd.DataFrame, pd.DataFrame | None, pd.DataFrame | None, str, bool], dict[str, Any]
    ],
) -> dict[str, Any]:
    report_path = Path(path_str)
    daily_pq_sig = parquet_cache_signature_fn(report_path, parquet_daily_dataset)
    if daily_pq_sig is not None:
        daily_df = normalize_daily_table_fn(load_parquet_df_cached_fn(*daily_pq_sig))
    else:
        daily_df = load_daily_df_cached_fn(path_str, modified_ns, size_bytes)
    if daily_df.empty or "date" not in daily_df.columns:
        return build_payload_from_frames_fn(
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            active_overview_kpi,
            compare_active,
        )
    start_day, end_day = resolve_cached_range_fn(start_iso, end_iso)
    prev_start_day, prev_end_day = resolve_cached_range_fn(prev_start_iso, prev_end_iso)

    cur_df = daily_df.copy()
    if pd.api.types.is_datetime64_any_dtype(cur_df["date"]):
        cur_df["date"] = cur_df["date"].dt.date
    else:
        cur_df["date"] = pd.to_datetime(cur_df["date"], errors="coerce").dt.date
    cur_df = cur_df.dropna(subset=["date"])
    prev_df = cur_df.copy()
    cur_df = cur_df[(cur_df["date"] >= start_day) & (cur_df["date"] <= end_day)].copy()
    prev_df = prev_df[(prev_df["date"] >= prev_start_day) & (prev_df["date"] <= prev_end_day)].copy()

    hourly_candidate = pd.DataFrame()
    hourly_compare_candidate = pd.DataFrame()
    if (
        active_overview_kpi in paid_trend_kpi_keys
        and not cur_df.empty
        and cur_df["date"].nunique() == 1
    ):
        hourly_pq_sig = parquet_cache_signature_fn(report_path, parquet_hourly_dataset)
        if hourly_pq_sig is not None:
            hourly_df = normalize_hourly_table_fn(load_parquet_df_cached_fn(*hourly_pq_sig))
        else:
            hourly_df = load_hourly_df_cached_fn(path_str, modified_ns, size_bytes)
        if not hourly_df.empty and "date" in hourly_df.columns:
            hourly_candidate = hourly_df.copy()
            if pd.api.types.is_datetime64_any_dtype(hourly_candidate["date"]):
                hourly_candidate["date"] = hourly_candidate["date"].dt.date
            else:
                hourly_candidate["date"] = pd.to_datetime(hourly_candidate["date"], errors="coerce").dt.date
            hourly_candidate = hourly_candidate.dropna(subset=["date"])
            hourly_candidate = hourly_candidate[
                (hourly_candidate["date"] >= start_day) & (hourly_candidate["date"] <= end_day)
            ].copy()
            if compare_active:
                hourly_compare_candidate = hourly_df.copy()
                if pd.api.types.is_datetime64_any_dtype(hourly_compare_candidate["date"]):
                    hourly_compare_candidate["date"] = hourly_compare_candidate["date"].dt.date
                else:
                    hourly_compare_candidate["date"] = pd.to_datetime(
                        hourly_compare_candidate["date"], errors="coerce"
                    ).dt.date
                hourly_compare_candidate = hourly_compare_candidate.dropna(subset=["date"])
                hourly_compare_candidate = hourly_compare_candidate[
                    (hourly_compare_candidate["date"] >= prev_start_day)
                    & (hourly_compare_candidate["date"] <= prev_end_day)
                ].copy()

    return build_payload_from_frames_fn(
        cur_df,
        prev_df,
        hourly_candidate,
        hourly_compare_candidate,
        active_overview_kpi,
        compare_active,
    )


def render_overview_trend_chart(
    *,
    st_module: Any,
    payload: dict[str, Any],
    active_overview_kpi: str,
    platform: str,
    compare_active: bool,
    compare_label: str,
    paid_trend_kpi_keys: set[str] | tuple[str, ...],
    kpi_catalog: dict[str, dict[str, Any]],
    kpi_trend_subtitle_fn: Callable[[str], str],
    kpi_axis_title_fn: Callable[[str], str],
    kpi_hover_value_template_fn: Callable[[str], str],
    pbi_layout_fn: Callable[..., Any],
    html_escape_fn: Callable[[str], str],
    c_google: str,
    c_meta: str,
    c_accent: str,
    c_mute: str,
) -> None:
    def _has_series_values(values: Any) -> bool:
        return isinstance(values, list) and len(values) > 0

    trend_label = str(kpi_catalog.get(active_overview_kpi, {}).get("label", active_overview_kpi))
    hover_value_template = kpi_hover_value_template_fn(active_overview_kpi)

    compare_note_html = ""
    if compare_active:
        if active_overview_kpi in paid_trend_kpi_keys:
            if platform == "Google":
                compare_has_values = _has_series_values(payload.get("google_compare"))
            elif platform == "Meta":
                compare_has_values = _has_series_values(payload.get("meta_compare"))
            else:
                compare_has_values = _has_series_values(payload.get("google_compare")) or _has_series_values(
                    payload.get("meta_compare")
                )
        else:
            compare_has_values = _has_series_values(payload.get("ga4_compare"))
        if compare_has_values:
            compare_note_text = f"Comparación activa · {compare_label}"
            compare_note_class = "viz-compare-note"
        else:
            compare_note_text = "Comparación activa · sin datos comparativos"
            compare_note_class = "viz-compare-note viz-compare-note-empty"
        compare_note_html = f"<div class='{compare_note_class}'>{html_escape_fn(compare_note_text)}</div>"

    trend_subtitle = str(payload.get("trend_subtitle") or kpi_trend_subtitle_fn(active_overview_kpi))
    st_module.markdown(
        f"""
        <div class="viz-card">
          <div class="viz-head">
            <p class="viz-title">Performance Across Platforms</p>
            {compare_note_html}
          </div>
          <div class="viz-sub">{html_escape_fn(trend_subtitle)}</div>
        """,
        unsafe_allow_html=True,
    )
    x_values = payload.get("x_values", [])
    if not isinstance(x_values, list) or not x_values:
        st_module.info("Sin datos para el periodo seleccionado.")
        st_module.markdown("</div>", unsafe_allow_html=True)
        return

    is_single_day = bool(payload.get("is_single_day", False))
    x_tick_format = str(payload.get("x_tick_format", "%d %b"))
    x_hover_format = str(payload.get("x_hover_format", "%d %b %Y"))
    hourly_projection_note = str(payload.get("hourly_projection_note", ""))
    fig = go.Figure()
    traces_added = 0

    def _add_series_trace(
        values: Any,
        *,
        name: str,
        hover_label: str,
        color: str,
        compare_series: bool = False,
    ) -> None:
        nonlocal traces_added
        if not isinstance(values, list) or not values:
            return
        fig.add_trace(
            go.Scatter(
                x=x_values,
                y=values,
                mode="lines+markers" if is_single_day else "lines",
                name=name,
                line={
                    "color": color,
                    "width": 2 if compare_series else 4,
                    "dash": "dot" if compare_series else "solid",
                    "shape": "linear",
                },
                opacity=0.75 if compare_series else 1.0,
                hovertemplate=f"%{{x|{x_hover_format}}}<br>{hover_label}: {hover_value_template}<extra></extra>",
            )
        )
        if not compare_series:
            traces_added += 1

    if active_overview_kpi in paid_trend_kpi_keys:
        if platform in ("All", "Google"):
            _add_series_trace(
                payload.get("google_current"),
                name="Google Ads",
                hover_label="Google",
                color=c_google,
            )
            if compare_active:
                _add_series_trace(
                    payload.get("google_compare"),
                    name="Google Ads (Comparación)",
                    hover_label="Google (comparación)",
                    color=c_google,
                    compare_series=True,
                )
        if platform in ("All", "Meta"):
            _add_series_trace(
                payload.get("meta_current"),
                name="Meta Ads",
                hover_label="Meta",
                color=c_meta,
            )
            if compare_active:
                _add_series_trace(
                    payload.get("meta_compare"),
                    name="Meta Ads (Comparación)",
                    hover_label="Meta (comparación)",
                    color=c_meta,
                    compare_series=True,
                )
    else:
        _add_series_trace(
            payload.get("ga4_current"),
            name="GA4",
            hover_label=html_escape_fn(trend_label),
            color=c_accent,
        )
        if compare_active:
            _add_series_trace(
                payload.get("ga4_compare"),
                name="GA4 (Comparación)",
                hover_label=f"{html_escape_fn(trend_label)} (comparación)",
                color=c_accent,
                compare_series=True,
            )
    if traces_added == 0:
        st_module.info("No hay datos de tendencia para la métrica seleccionada.")
        st_module.markdown("</div>", unsafe_allow_html=True)
        return

    pbi_layout_fn(fig, yaxis_title=kpi_axis_title_fn(active_overview_kpi), xaxis_title="")
    fig.update_layout(height=355, hovermode="x unified", showlegend=True)
    fig.update_xaxes(tickformat=x_tick_format, tickfont={"size": 10, "color": c_mute})
    if is_single_day:
        fig.update_xaxes(dtick=2 * 60 * 60 * 1000)
    fig.update_yaxes(tickfont={"size": 10, "color": c_mute})
    fmt = str(kpi_catalog.get(active_overview_kpi, {}).get("fmt", "int"))
    if fmt == "pct":
        fig.update_yaxes(tickformat=".1%")
    elif fmt == "money":
        fig.update_yaxes(tickprefix="$", separatethousands=True)
    else:
        fig.update_yaxes(tickformat=",.0f")
    st_module.plotly_chart(fig, width="stretch")
    if hourly_projection_note:
        st_module.caption(hourly_projection_note)
    st_module.markdown("</div>", unsafe_allow_html=True)
