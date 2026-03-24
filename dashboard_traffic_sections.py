from __future__ import annotations

from datetime import date
from typing import Any, Callable

import pandas as pd

SourcePlatformFn = Callable[[Any], str]
PctDeltaFn = Callable[[float | None, float | None], float | None]
FmtCompactFn = Callable[[float | int | None], str]
FmtDurationFn = Callable[[float | None], str]
FmtPctFn = Callable[[float | None], str]
FmtDeltaFn = Callable[[float | None], str]


def _sdiv(numerator: float, denominator: float) -> float | None:
    try:
        den = float(denominator)
    except Exception:
        return None
    if den == 0:
        return None
    try:
        return float(numerator) / den
    except Exception:
        return None


def _coerce_day(raw_value: Any) -> date | None:
    try:
        parsed = pd.to_datetime(raw_value, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date() if hasattr(parsed, "date") else None


def _normalize_range(start_date: Any, end_date: Any) -> tuple[date | None, date | None]:
    start_day = _coerce_day(start_date)
    end_day = _coerce_day(end_date)
    if start_day is None or end_day is None:
        return start_day, end_day
    if start_day > end_day:
        return end_day, start_day
    return start_day, end_day


def _filter_by_date_range(df: pd.DataFrame, start_date: Any, end_date: Any) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    if "date" not in df.columns:
        return df.copy()
    start_day, end_day = _normalize_range(start_date, end_date)
    if start_day is None or end_day is None:
        return df.copy()
    out = df.copy()
    parsed_dates = pd.to_datetime(out["date"], errors="coerce").dt.date
    valid_mask = parsed_dates.notna()
    mask = valid_mask & (parsed_dates >= start_day) & (parsed_dates <= end_day)
    filtered = out.loc[mask].copy()
    if not filtered.empty:
        filtered["date"] = parsed_dates.loc[mask].to_numpy()
    return filtered


def _num_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series([float(default)] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(float(default))


def _text_series(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype="object")
    out = df[column].fillna("").astype(str).str.strip()
    return out.mask(out == "", default)


def _weighted_average(values: pd.Series, weights: pd.Series) -> float | None:
    if values.empty or weights.empty:
        return None
    val = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0)
    valid = val.notna() & w.notna()
    if not bool(valid.any()):
        return None
    val = val.loc[valid].astype(float)
    w = w.loc[valid].astype(float)
    total_weight = float(w.sum())
    if total_weight <= 0:
        return None
    return float((val * w).sum()) / total_weight


def _resolve_event_name(event_name: str, default_event_name: str) -> str:
    chosen = str(event_name or "").strip()
    if chosen:
        return chosen
    return str(default_event_name or "").strip()


def _resolve_conversion_col(df: pd.DataFrame) -> str | None:
    for candidate in ("conversions", "eventCount", "event_count"):
        if candidate in df.columns:
            return candidate
    return None


def _filter_by_platform(
    df: pd.DataFrame,
    *,
    platform: str,
    source_platform_fn: SourcePlatformFn,
    source_column: str = "sessionSourceMedium",
    platform_column: str = "platform",
) -> tuple[pd.DataFrame, bool]:
    selected = str(platform or "All").strip()
    if selected not in {"Google", "Meta"}:
        return df.copy(), False
    if df.empty:
        return df.copy(), False

    out = df.copy()
    used_attr_limited = False

    if platform_column in out.columns:
        platform_series = _text_series(out, platform_column)
        has_platform = platform_series.str.len().gt(0).any()
        if has_platform:
            filtered = out.loc[platform_series.str.lower() == selected.lower()].copy()
            if not filtered.empty:
                return filtered, False

    if source_column in out.columns:
        source_series = _text_series(out, source_column)
        has_source = source_series.str.len().gt(0).any()
        if has_source:
            mapped = source_series.map(lambda raw: str(source_platform_fn(raw or "")).strip())
            filtered = out.loc[mapped.str.lower() == selected.lower()].copy()
            return filtered, False

    used_attr_limited = True
    return out, used_attr_limited


def _resolve_base_channel_df(
    channel_df: pd.DataFrame,
    *,
    platform: str,
    start_date: Any,
    end_date: Any,
    source_platform_fn: SourcePlatformFn,
) -> tuple[pd.DataFrame, bool]:
    ch = _filter_by_date_range(channel_df, start_date, end_date)
    ch, attr_limited = _filter_by_platform(
        ch,
        platform=platform,
        source_platform_fn=source_platform_fn,
        source_column="sessionSourceMedium",
        platform_column="platform",
    )
    if ch.empty:
        return ch, attr_limited
    out = ch.copy()
    out["sessionDefaultChannelGroup"] = _text_series(out, "sessionDefaultChannelGroup", "Unknown")
    out["sessionSourceMedium"] = _text_series(out, "sessionSourceMedium", "(sin source / medium)")
    out["sessions"] = _num_series(out, "sessions", 0.0)
    out["totalUsers"] = _num_series(out, "totalUsers", 0.0)
    out["averageSessionDuration"] = _num_series(out, "averageSessionDuration", 0.0)
    out["bounceRate"] = _num_series(out, "bounceRate", 0.0)
    out["conversions"] = _num_series(out, "conversions", 0.0)
    return out, attr_limited


def _resolve_event_df(
    event_df: pd.DataFrame,
    *,
    platform: str,
    start_date: Any,
    end_date: Any,
    ga4_event_name: str,
    default_ga4_event_name: str,
    source_platform_fn: SourcePlatformFn,
) -> tuple[pd.DataFrame, bool]:
    ev = _filter_by_date_range(event_df, start_date, end_date)
    attr_limited = False
    if ev.empty:
        return ev, attr_limited

    out = ev.copy()
    event_col = "eventName" if "eventName" in out.columns else ("event_name" if "event_name" in out.columns else None)
    if event_col:
        resolved_name = _resolve_event_name(ga4_event_name, default_ga4_event_name)
        out = out.loc[_text_series(out, event_col).str.lower() == resolved_name.lower()].copy()

    out, platform_attr_limited = _filter_by_platform(
        out,
        platform=platform,
        source_platform_fn=source_platform_fn,
        source_column="sessionSourceMedium",
        platform_column="platform",
    )
    attr_limited = attr_limited or platform_attr_limited
    if out.empty:
        return out, attr_limited
    conv_col = _resolve_conversion_col(out)
    if conv_col is None:
        out["conversions"] = 0.0
    elif conv_col != "conversions":
        out["conversions"] = _num_series(out, conv_col, 0.0)
    else:
        out["conversions"] = _num_series(out, "conversions", 0.0)
    out["sessionSourceMedium"] = _text_series(out, "sessionSourceMedium", "(sin source / medium)")
    return out, attr_limited


def compute_traffic_quality_snapshot(
    *,
    channel_df: pd.DataFrame,
    event_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    ga4_event_name: str,
    default_ga4_event_name: str,
    source_platform_fn: SourcePlatformFn,
    fallback_metrics: dict[str, float | None],
) -> dict[str, Any]:
    ch, ch_attr_limited = _resolve_base_channel_df(
        channel_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    ev, ev_attr_limited = _resolve_event_df(
        event_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        ga4_event_name=ga4_event_name,
        default_ga4_event_name=default_ga4_event_name,
        source_platform_fn=source_platform_fn,
    )

    fallback_sessions = float(fallback_metrics.get("sessions") or 0.0)
    fallback_users = float(fallback_metrics.get("users") or 0.0)
    fallback_bounce = float(fallback_metrics.get("bounce") or 0.0)
    fallback_avg_sess = float(fallback_metrics.get("avg_sess") or 0.0)
    fallback_conv = float(fallback_metrics.get("conv") or 0.0)

    sessions = float(_num_series(ch, "sessions", 0.0).sum()) if not ch.empty else fallback_sessions
    users = float(_num_series(ch, "totalUsers", 0.0).sum()) if not ch.empty else fallback_users
    avg_sess = _weighted_average(_num_series(ch, "averageSessionDuration", 0.0), _num_series(ch, "sessions", 0.0))
    bounce = _weighted_average(_num_series(ch, "bounceRate", 0.0), _num_series(ch, "sessions", 0.0))
    if avg_sess is None:
        avg_sess = fallback_avg_sess
    if bounce is None:
        bounce = fallback_bounce

    channel_conv = float(_num_series(ch, "conversions", 0.0).sum()) if not ch.empty else 0.0
    event_conv = float(_num_series(ev, "conversions", 0.0).sum()) if not ev.empty else 0.0
    event_name_resolved = _resolve_event_name(ga4_event_name, default_ga4_event_name)
    used_event = event_conv > 0
    if used_event:
        conv_acq = event_conv
        conv_source_label = f"Evento GA4: {event_name_resolved}"
    elif channel_conv > 0:
        conv_acq = channel_conv
        conv_source_label = "Conversiones GA4 globales"
    else:
        conv_acq = fallback_conv
        conv_source_label = "Conversiones fallback (paid)"

    cvr_sess_conv = _sdiv(conv_acq, sessions) or 0.0

    return {
        "sessions": sessions,
        "users": users,
        "bounce": bounce,
        "avg_sess": avg_sess,
        "conv_acq": conv_acq,
        "cvr_sess_conv": cvr_sess_conv,
        "used_event_conversions": used_event,
        "conversion_source_label": conv_source_label,
        "attribution_limited": bool(ch_attr_limited or ev_attr_limited),
    }


def build_channel_roll(
    *,
    channel_df: pd.DataFrame,
    event_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    ga4_event_name: str,
    default_ga4_event_name: str,
    source_platform_fn: SourcePlatformFn,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    ch, ch_attr_limited = _resolve_base_channel_df(
        channel_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    if ch.empty:
        empty = pd.DataFrame(columns=["sessionDefaultChannelGroup", "sessions", "conversions", "cvr_sess_conv"])
        return empty, {"attribution_limited": ch_attr_limited, "used_event_conversions": False}

    ch_roll = (
        ch.groupby(["date", "sessionDefaultChannelGroup", "sessionSourceMedium"], as_index=False)
        .agg(sessions=("sessions", "sum"), conversions=("conversions", "sum"))
    )
    channel_sessions = (
        ch_roll.groupby("sessionDefaultChannelGroup", as_index=False)
        .agg(sessions=("sessions", "sum"))
        .sort_values("sessions", ascending=False)
        .reset_index(drop=True)
    )

    ev, ev_attr_limited = _resolve_event_df(
        event_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        ga4_event_name=ga4_event_name,
        default_ga4_event_name=default_ga4_event_name,
        source_platform_fn=source_platform_fn,
    )

    used_event = False
    if not ev.empty:
        ev_roll = ev.groupby(["date", "sessionSourceMedium"], as_index=False).agg(conversions=("conversions", "sum"))
        ev_sum = float(_num_series(ev_roll, "conversions", 0.0).sum())
        if ev_sum > 0:
            used_event = True
            conv_by_channel = (
                ch_roll.merge(ev_roll, how="left", on=["date", "sessionSourceMedium"], suffixes=("", "_event"))
                .assign(conversions_event=lambda d: _num_series(d, "conversions_event", 0.0))
                .groupby("sessionDefaultChannelGroup", as_index=False)
                .agg(conversions=("conversions_event", "sum"))
            )
        else:
            conv_by_channel = (
                ch_roll.groupby("sessionDefaultChannelGroup", as_index=False)
                .agg(conversions=("conversions", "sum"))
            )
    else:
        conv_by_channel = (
            ch_roll.groupby("sessionDefaultChannelGroup", as_index=False)
            .agg(conversions=("conversions", "sum"))
        )

    out = channel_sessions.merge(conv_by_channel, on="sessionDefaultChannelGroup", how="left")
    out["conversions"] = _num_series(out, "conversions", 0.0)
    out["cvr_sess_conv"] = out.apply(
        lambda row: _sdiv(float(row.get("conversions", 0.0)), float(row.get("sessions", 0.0))) or 0.0,
        axis=1,
    )
    out = out.sort_values("sessions", ascending=False).reset_index(drop=True)
    meta = {
        "attribution_limited": bool(ch_attr_limited or ev_attr_limited),
        "used_event_conversions": used_event,
    }
    return out, meta


def build_source_medium_roll(
    *,
    channel_df: pd.DataFrame,
    event_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    ga4_event_name: str,
    default_ga4_event_name: str,
    source_platform_fn: SourcePlatformFn,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    ch, ch_attr_limited = _resolve_base_channel_df(
        channel_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    if ch.empty:
        empty = pd.DataFrame(
            columns=[
                "source_medium",
                "sessions",
                "users",
                "bounce_rate",
                "avg_session",
                "conversions",
                "cvr_sess_conv",
                "share_sessions",
            ]
        )
        return empty, {"attribution_limited": ch_attr_limited, "used_event_conversions": False}

    ch_roll = (
        ch.groupby(["date", "sessionSourceMedium"], as_index=False)
        .agg(
            sessions=("sessions", "sum"),
            users=("totalUsers", "sum"),
            bounce_rate=("bounceRate", "mean"),
            avg_session=("averageSessionDuration", "mean"),
            conversions=("conversions", "sum"),
        )
        .rename(columns={"sessionSourceMedium": "source_medium"})
    )

    ev, ev_attr_limited = _resolve_event_df(
        event_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        ga4_event_name=ga4_event_name,
        default_ga4_event_name=default_ga4_event_name,
        source_platform_fn=source_platform_fn,
    )

    used_event = False
    if not ev.empty:
        ev_roll = (
            ev.groupby("sessionSourceMedium", as_index=False)
            .agg(conversions=("conversions", "sum"))
            .rename(columns={"sessionSourceMedium": "source_medium"})
        )
        if float(_num_series(ev_roll, "conversions", 0.0).sum()) > 0:
            used_event = True
            conv_by_source = ev_roll
        else:
            conv_by_source = (
                ch_roll.groupby("source_medium", as_index=False).agg(conversions=("conversions", "sum"))
            )
    else:
        conv_by_source = (
            ch_roll.groupby("source_medium", as_index=False).agg(conversions=("conversions", "sum"))
        )

    grouped = (
        ch_roll.groupby("source_medium", as_index=False)
        .agg(
            sessions=("sessions", "sum"),
            users=("users", "sum"),
            bounce_rate=("bounce_rate", "mean"),
            avg_session=("avg_session", "mean"),
        )
        .merge(conv_by_source, on="source_medium", how="left")
    )
    grouped["conversions"] = _num_series(grouped, "conversions", 0.0)
    grouped["cvr_sess_conv"] = grouped.apply(
        lambda row: _sdiv(float(row.get("conversions", 0.0)), float(row.get("sessions", 0.0))) or 0.0,
        axis=1,
    )
    total_sessions = float(_num_series(grouped, "sessions", 0.0).sum())
    grouped["share_sessions"] = grouped["sessions"].map(lambda value: (_sdiv(float(value), total_sessions) or 0.0))
    grouped = grouped.sort_values("sessions", ascending=False).reset_index(drop=True)
    meta = {
        "attribution_limited": bool(ch_attr_limited or ev_attr_limited),
        "used_event_conversions": used_event,
    }
    return grouped, meta


def build_top_pages_roll(
    pg_df: pd.DataFrame,
    *,
    start_date: Any,
    end_date: Any,
) -> pd.DataFrame:
    pg = _filter_by_date_range(pg_df, start_date, end_date)
    if pg.empty:
        return pd.DataFrame(
            columns=["pagePath", "pageTitle", "views", "sessions", "avg_session", "views_per_session"]
        )
    out = pg.copy()
    out["pagePath"] = _text_series(out, "pagePath")
    out["pageTitle"] = _text_series(out, "pageTitle")
    out["screenPageViews"] = _num_series(out, "screenPageViews", 0.0)
    out["sessions"] = _num_series(out, "sessions", 0.0)
    out["averageSessionDuration"] = _num_series(out, "averageSessionDuration", 0.0)
    roll = (
        out.groupby(["pagePath", "pageTitle"], as_index=False)
        .agg(
            views=("screenPageViews", "sum"),
            sessions=("sessions", "sum"),
            avg_session=("averageSessionDuration", "mean"),
        )
        .sort_values("views", ascending=False)
        .reset_index(drop=True)
    )
    roll["views_per_session"] = roll.apply(
        lambda row: _sdiv(float(row.get("views", 0.0)), float(row.get("sessions", 0.0))) or 0.0,
        axis=1,
    )
    return roll


def render_decision_cards(
    *,
    st_module: Any,
    current_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any],
    pct_delta_fn: PctDeltaFn,
    fmt_compact_fn: FmtCompactFn,
    fmt_duration_fn: FmtDurationFn,
    fmt_pct_fn: FmtPctFn,
    fmt_delta_compact_fn: FmtDeltaFn,
    c_accent: str,
) -> None:
    st_module.markdown(
        f"""
        <style>
          .st-key-traffic-decision-wrap [data-testid="stMetric"] {{
            margin: 0 !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            border-radius: 18px !important;
            padding: 0.95rem !important;
            min-height: 8.4rem !important;
            background: rgba(255,255,255,0.84) !important;
            box-shadow: 0 10px 22px rgba(15,23,42,0.05);
          }}
          .st-key-traffic-decision-wrap [data-testid="stMetricLabel"] {{
            color: #4d627f !important;
            font-weight: 700 !important;
          }}
          .st-key-traffic-decision-wrap [data-testid="stMetricValue"] {{
            color: #201D1D !important;
            font-weight: 800 !important;
          }}
          .st-key-traffic-decision-wrap [data-testid="stMetricDelta"] {{
            font-weight: 700 !important;
          }}
          .st-key-traffic-decision-wrap .traffic-cards-head {{
            margin-bottom: 0.55rem;
            padding: 0.18rem 0.05rem;
            border-left: 3px solid {c_accent};
            padding-left: 0.55rem;
            color: #334761;
            font-size: 0.94rem;
            font-weight: 700;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    cards = [
        {
            "label": "Sesiones",
            "value": fmt_compact_fn(current_snapshot.get("sessions")),
            "delta": fmt_delta_compact_fn(pct_delta_fn(current_snapshot.get("sessions"), previous_snapshot.get("sessions"))),
            "delta_color": "normal",
        },
        {
            "label": "Usuarios",
            "value": fmt_compact_fn(current_snapshot.get("users")),
            "delta": fmt_delta_compact_fn(pct_delta_fn(current_snapshot.get("users"), previous_snapshot.get("users"))),
            "delta_color": "normal",
        },
        {
            "label": "Tasa de Rebote",
            "value": fmt_pct_fn(current_snapshot.get("bounce")),
            "delta": fmt_delta_compact_fn(pct_delta_fn(current_snapshot.get("bounce"), previous_snapshot.get("bounce"))),
            "delta_color": "inverse",
        },
        {
            "label": "Tiempo Promedio de Interaccion",
            "value": fmt_duration_fn(current_snapshot.get("avg_sess")),
            "delta": fmt_delta_compact_fn(pct_delta_fn(current_snapshot.get("avg_sess"), previous_snapshot.get("avg_sess"))),
            "delta_color": "normal",
        },
        {
            "label": "Conversiones de Adquisicion",
            "value": fmt_compact_fn(current_snapshot.get("conv_acq")),
            "delta": fmt_delta_compact_fn(pct_delta_fn(current_snapshot.get("conv_acq"), previous_snapshot.get("conv_acq"))),
            "delta_color": "normal",
        },
        {
            "label": "CVR Sesion -> Conversion",
            "value": fmt_pct_fn(current_snapshot.get("cvr_sess_conv")),
            "delta": fmt_delta_compact_fn(
                pct_delta_fn(current_snapshot.get("cvr_sess_conv"), previous_snapshot.get("cvr_sess_conv"))
            ),
            "delta_color": "normal",
        },
    ]

    with st_module.container(key="traffic-decision-wrap"):
        st_module.markdown(
            "<div class='traffic-cards-head'>Tarjetas de Decision (Calidad de Trafico)</div>",
            unsafe_allow_html=True,
        )
        row1 = st_module.columns(3, gap="small")
        row2 = st_module.columns(3, gap="small")
        for col, item in zip(row1 + row2, cards):
            col.metric(
                item["label"],
                item["value"],
                item["delta"],
                delta_color=item["delta_color"],
            )

    if bool(current_snapshot.get("attribution_limited")):
        st_module.caption("Atribucion limitada por fuente: faltan campos para segmentar plataforma con precision.")
    source_label = str(current_snapshot.get("conversion_source_label", "")).strip()
    if source_label:
        st_module.caption(f"Fuente de conversion en tarjetas: {source_label}.")


def render_source_medium_table(
    *,
    st_module: Any,
    roll: pd.DataFrame,
    fmt_duration_fn: FmtDurationFn,
    fmt_pct_fn: FmtPctFn,
) -> None:
    if roll.empty:
        st_module.info("Sin datos de Source / Medium para el rango seleccionado.")
        return

    def _style_high_good(value: Any, *, reverse: bool = False) -> str:
        try:
            val = float(value)
        except Exception:
            return ""
        if pd.isna(val):
            return ""
        if reverse:
            green = int(34 + ((239 - 34) * val))
            red = int(197 + ((68 - 197) * val))
        else:
            red = int(34 + ((239 - 34) * val))
            green = int(197 + ((68 - 197) * val))
        return f"background-color: rgba({red}, {green}, 94, 0.22); font-weight: 600;"

    display = roll.copy()
    display = display.rename(
        columns={
            "source_medium": "Source / Medium",
            "sessions": "Sesiones",
            "users": "Usuarios",
            "bounce_rate": "Rebote",
            "avg_session": "Tiempo Promedio",
            "conversions": "Conversiones",
            "cvr_sess_conv": "CVR Sesion -> Conv",
            "share_sessions": "Share Sesiones",
        }
    )[
        [
            "Source / Medium",
            "Sesiones",
            "Usuarios",
            "Rebote",
            "Tiempo Promedio",
            "Conversiones",
            "CVR Sesion -> Conv",
            "Share Sesiones",
        ]
    ]

    cvr = pd.to_numeric(display["CVR Sesion -> Conv"], errors="coerce")
    bounce = pd.to_numeric(display["Rebote"], errors="coerce")
    cvr_min, cvr_max = float(cvr.min(skipna=True) or 0.0), float(cvr.max(skipna=True) or 0.0)
    bounce_min, bounce_max = float(bounce.min(skipna=True) or 0.0), float(bounce.max(skipna=True) or 0.0)

    def _normalize_value(value: Any, min_value: float, max_value: float) -> float:
        try:
            current = float(value)
        except Exception:
            return 0.0
        if max_value <= min_value:
            return 0.5
        return max(0.0, min(1.0, (current - min_value) / (max_value - min_value)))

    styled = (
        display.style.format(
            {
                "Sesiones": lambda v: f"{float(v):,.0f}",
                "Usuarios": lambda v: f"{float(v):,.0f}",
                "Rebote": lambda v: fmt_pct_fn(float(v)),
                "Tiempo Promedio": lambda v: fmt_duration_fn(float(v)),
                "Conversiones": lambda v: f"{float(v):,.0f}",
                "CVR Sesion -> Conv": lambda v: fmt_pct_fn(float(v)),
                "Share Sesiones": lambda v: fmt_pct_fn(float(v)),
            }
        )
        .applymap(
            lambda value: _style_high_good(_normalize_value(value, cvr_min, cvr_max), reverse=False),
            subset=["CVR Sesion -> Conv"],
        )
        .applymap(
            lambda value: _style_high_good(_normalize_value(value, bounce_min, bounce_max), reverse=True),
            subset=["Rebote"],
        )
    )
    st_module.dataframe(styled, width="stretch", hide_index=True)

