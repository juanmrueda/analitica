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

TRAFFIC_DECISION_CARD_KEYS: tuple[str, ...] = (
    "sessions",
    "users",
    "bounce",
    "avg_sess",
    "conv_acq",
    "cvr_sess_conv",
)
WEEKDAY_ORDER: tuple[str, ...] = (
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
)


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


def _resolve_dimension_df(
    raw_df: pd.DataFrame,
    *,
    platform: str,
    start_date: Any,
    end_date: Any,
    source_platform_fn: SourcePlatformFn,
) -> tuple[pd.DataFrame, bool]:
    base = _filter_by_date_range(raw_df, start_date, end_date)
    filtered, attr_limited = _filter_by_platform(
        base,
        platform=platform,
        source_platform_fn=source_platform_fn,
        source_column="sessionSourceMedium",
        platform_column="platform",
    )
    if filtered.empty:
        return filtered, attr_limited
    out = filtered.copy()
    if "sessionSourceMedium" in out.columns:
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


def _build_fallback_daily_roll(fallback_daily_df: pd.DataFrame, platform: str) -> pd.DataFrame:
    if fallback_daily_df.empty:
        return pd.DataFrame(columns=["date", "sessions", "users", "avg_sess", "bounce", "fallback_conv"])
    out = fallback_daily_df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date"]).copy()
    out["sessions"] = _num_series(out, "ga4_sessions", 0.0)
    out["users"] = _num_series(out, "ga4_users", 0.0)
    out["avg_sess"] = _num_series(out, "ga4_avg_sess", 0.0)
    out["bounce"] = _num_series(out, "ga4_bounce", 0.0)
    if str(platform or "All").strip() == "Google":
        out["fallback_conv"] = _num_series(out, "google_conv", 0.0)
    elif str(platform or "All").strip() == "Meta":
        out["fallback_conv"] = _num_series(out, "meta_conv", 0.0)
    else:
        out["fallback_conv"] = _num_series(out, "total_conv", 0.0)
    return out[["date", "sessions", "users", "avg_sess", "bounce", "fallback_conv"]]


def _group_channel_daily_roll(ch: pd.DataFrame) -> pd.DataFrame:
    if ch.empty:
        return pd.DataFrame(
            columns=["date", "sessions", "users", "avg_sess", "bounce", "channel_conversions"]
        )
    rows: list[dict[str, Any]] = []
    grouped = ch.groupby("date", dropna=False)
    for raw_day, day_df in grouped:
        sessions = float(_num_series(day_df, "sessions", 0.0).sum())
        rows.append(
            {
                "date": raw_day,
                "sessions": sessions,
                "users": float(_num_series(day_df, "totalUsers", 0.0).sum()),
                "avg_sess": _weighted_average(
                    _num_series(day_df, "averageSessionDuration", 0.0),
                    _num_series(day_df, "sessions", 0.0),
                )
                or 0.0,
                "bounce": _weighted_average(
                    _num_series(day_df, "bounceRate", 0.0),
                    _num_series(day_df, "sessions", 0.0),
                )
                or 0.0,
                "channel_conversions": float(_num_series(day_df, "conversions", 0.0).sum()),
            }
        )
    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def _group_event_daily_roll(ev: pd.DataFrame) -> pd.DataFrame:
    if ev.empty:
        return pd.DataFrame(columns=["date", "event_conversions"])
    grouped = (
        ev.groupby("date", as_index=False)
        .agg(event_conversions=("conversions", "sum"))
        .sort_values("date")
        .reset_index(drop=True)
    )
    grouped["date"] = pd.to_datetime(grouped["date"], errors="coerce").dt.date
    return grouped.dropna(subset=["date"]).reset_index(drop=True)


def build_traffic_metric_timeseries(
    *,
    channel_df: pd.DataFrame,
    event_df: pd.DataFrame,
    fallback_daily_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    ga4_event_name: str,
    default_ga4_event_name: str,
    source_platform_fn: SourcePlatformFn,
    metric_key: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    start_day, end_day = _normalize_range(start_date, end_date)
    if start_day is None or end_day is None:
        return pd.DataFrame(columns=["date", "value"]), {
            "conversion_source_label": "",
            "used_event_conversions": False,
            "attribution_limited": False,
        }

    ch, ch_attr_limited = _resolve_base_channel_df(
        channel_df,
        platform=platform,
        start_date=start_day,
        end_date=end_day,
        source_platform_fn=source_platform_fn,
    )
    ev, ev_attr_limited = _resolve_event_df(
        event_df,
        platform=platform,
        start_date=start_day,
        end_date=end_day,
        ga4_event_name=ga4_event_name,
        default_ga4_event_name=default_ga4_event_name,
        source_platform_fn=source_platform_fn,
    )
    channel_roll = _group_channel_daily_roll(ch)
    event_roll = _group_event_daily_roll(ev)
    fallback_roll = _build_fallback_daily_roll(_filter_by_date_range(fallback_daily_df, start_day, end_day), platform)

    date_frame = pd.DataFrame({"date": pd.date_range(start=start_day, end=end_day, freq="D").date})
    merged = (
        date_frame.merge(channel_roll, on="date", how="left")
        .merge(event_roll, on="date", how="left")
        .merge(fallback_roll, on="date", how="left", suffixes=("", "_fallback"))
    )
    for column in (
        "sessions",
        "users",
        "avg_sess",
        "bounce",
        "channel_conversions",
        "event_conversions",
        "fallback_conv",
    ):
        merged[column] = _num_series(merged, column, 0.0)

    total_event_conversions = float(merged["event_conversions"].sum())
    total_channel_conversions = float(merged["channel_conversions"].sum())
    resolved_event_name = _resolve_event_name(ga4_event_name, default_ga4_event_name)
    if total_event_conversions > 0:
        merged["conv_acq"] = merged["event_conversions"]
        conversion_source_label = f"Evento GA4: {resolved_event_name}"
        used_event_conversions = True
    elif total_channel_conversions > 0:
        merged["conv_acq"] = merged["channel_conversions"]
        conversion_source_label = "Conversiones GA4 globales"
        used_event_conversions = False
    else:
        merged["conv_acq"] = merged["fallback_conv"]
        conversion_source_label = "Conversiones fallback (paid)"
        used_event_conversions = False

    merged["sessions"] = merged["sessions"].where(merged["sessions"] > 0, merged["sessions_fallback"])
    merged["users"] = merged["users"].where(merged["users"] > 0, merged["users_fallback"])
    merged["avg_sess"] = merged["avg_sess"].where(merged["avg_sess"] > 0, merged["avg_sess_fallback"])
    merged["bounce"] = merged["bounce"].where(merged["bounce"] > 0, merged["bounce_fallback"])
    merged["cvr_sess_conv"] = merged.apply(
        lambda row: _sdiv(float(row.get("conv_acq", 0.0)), float(row.get("sessions", 0.0))) or 0.0,
        axis=1,
    )

    metric_column = metric_key if metric_key in TRAFFIC_DECISION_CARD_KEYS else "sessions"
    out = merged[["date", metric_column]].rename(columns={metric_column: "value"}).copy()
    meta = {
        "conversion_source_label": conversion_source_label,
        "used_event_conversions": used_event_conversions,
        "attribution_limited": bool(ch_attr_limited or ev_attr_limited),
    }
    return out, meta


def build_hourly_active_users_matrix(
    hourly_users_df: pd.DataFrame,
    *,
    platform: str,
    start_date: Any,
    end_date: Any,
    source_platform_fn: SourcePlatformFn,
) -> pd.DataFrame:
    base, _ = _resolve_dimension_df(
        hourly_users_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    if base.empty:
        return pd.DataFrame(index=list(range(24)), columns=list(WEEKDAY_ORDER)).fillna(0.0)
    out = base.copy()
    out["dayOfWeekName"] = _text_series(out, "dayOfWeekName", "Unknown")
    out["hour"] = _num_series(out, "hour", 0.0).astype(int)
    out["activeUsers"] = _num_series(out, "activeUsers", 0.0)
    pivot = (
        out.groupby(["hour", "dayOfWeekName"], as_index=False)
        .agg(activeUsers=("activeUsers", "sum"))
        .pivot(index="hour", columns="dayOfWeekName", values="activeUsers")
        .reindex(index=list(range(24)), columns=list(WEEKDAY_ORDER))
        .fillna(0.0)
    )
    return pivot


def _flag_emoji(country_id: Any) -> str:
    raw = str(country_id or "").strip().upper()
    if len(raw) != 2 or not raw.isalpha():
        return ""
    return chr(127397 + ord(raw[0])) + chr(127397 + ord(raw[1]))


def build_country_users_roll(
    country_df: pd.DataFrame,
    *,
    platform: str,
    start_date: Any,
    end_date: Any,
    source_platform_fn: SourcePlatformFn,
) -> pd.DataFrame:
    base, _ = _resolve_dimension_df(
        country_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    if base.empty:
        return pd.DataFrame(columns=["country", "countryId", "users", "flag", "country_label"])
    out = base.copy()
    out["country"] = _text_series(out, "country", "Unknown")
    out["countryId"] = _text_series(out, "countryId", "")
    out["totalUsers"] = _num_series(out, "totalUsers", 0.0)
    grouped = (
        out.groupby(["country", "countryId"], as_index=False)
        .agg(users=("totalUsers", "sum"))
        .sort_values("users", ascending=False)
        .reset_index(drop=True)
    )
    grouped["flag"] = grouped["countryId"].map(_flag_emoji)
    grouped["country_label"] = grouped.apply(
        lambda row: f"{row['flag']} {row['country']}".strip(),
        axis=1,
    )
    return grouped


def build_city_users_roll(
    city_df: pd.DataFrame,
    *,
    platform: str,
    start_date: Any,
    end_date: Any,
    source_platform_fn: SourcePlatformFn,
) -> pd.DataFrame:
    base, _ = _resolve_dimension_df(
        city_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    if base.empty:
        return pd.DataFrame(columns=["city", "country", "countryId", "users", "city_label"])
    out = base.copy()
    out["city"] = _text_series(out, "city", "Unknown")
    out["country"] = _text_series(out, "country", "Unknown")
    out["countryId"] = _text_series(out, "countryId", "")
    out["totalUsers"] = _num_series(out, "totalUsers", 0.0)
    grouped = (
        out.groupby(["city", "country", "countryId"], as_index=False)
        .agg(users=("totalUsers", "sum"))
        .sort_values("users", ascending=False)
        .reset_index(drop=True)
    )
    grouped["city_label"] = grouped.apply(
        lambda row: f"{row['city']} ({row['country']})".strip(),
        axis=1,
    )
    return grouped


def build_tech_roll(
    tech_df: pd.DataFrame,
    *,
    label_column: str,
    platform: str,
    start_date: Any,
    end_date: Any,
    source_platform_fn: SourcePlatformFn,
    top_n: int = 6,
) -> pd.DataFrame:
    base, _ = _resolve_dimension_df(
        tech_df,
        platform=platform,
        start_date=start_date,
        end_date=end_date,
        source_platform_fn=source_platform_fn,
    )
    if base.empty:
        return pd.DataFrame(columns=[label_column, "users"])
    out = base.copy()
    out[label_column] = _text_series(out, label_column, "Unknown")
    out["totalUsers"] = _num_series(out, "totalUsers", 0.0)
    grouped = (
        out.groupby(label_column, as_index=False)
        .agg(users=("totalUsers", "sum"))
        .sort_values("users", ascending=False)
        .reset_index(drop=True)
    )
    if len(grouped) <= max(int(top_n), 1):
        return grouped
    head = grouped.head(max(int(top_n), 1)).copy()
    other_value = float(grouped.iloc[max(int(top_n), 1):]["users"].sum())
    if other_value > 0:
        other_row = pd.DataFrame([{label_column: "Other", "users": other_value}])
        head = pd.concat([head, other_row], ignore_index=True)
    return head


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

    display["Sesiones"] = pd.to_numeric(display["Sesiones"], errors="coerce").fillna(0.0).map(
        lambda value: f"{float(value):,.0f}"
    )
    display["Usuarios"] = pd.to_numeric(display["Usuarios"], errors="coerce").fillna(0.0).map(
        lambda value: f"{float(value):,.0f}"
    )
    display["Rebote"] = pd.to_numeric(display["Rebote"], errors="coerce").fillna(0.0).map(
        lambda value: fmt_pct_fn(float(value))
    )
    display["Tiempo Promedio"] = pd.to_numeric(
        display["Tiempo Promedio"], errors="coerce"
    ).fillna(0.0).map(lambda value: fmt_duration_fn(float(value)))
    display["Conversiones"] = pd.to_numeric(display["Conversiones"], errors="coerce").fillna(0.0).map(
        lambda value: f"{float(value):,.0f}"
    )
    display["CVR Sesion -> Conv"] = pd.to_numeric(
        display["CVR Sesion -> Conv"], errors="coerce"
    ).fillna(0.0).map(lambda value: fmt_pct_fn(float(value)))
    display["Share Sesiones"] = pd.to_numeric(
        display["Share Sesiones"], errors="coerce"
    ).fillna(0.0).map(lambda value: fmt_pct_fn(float(value)))

    st_module.dataframe(display, width="stretch", hide_index=True)
