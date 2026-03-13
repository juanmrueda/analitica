from __future__ import annotations

import html
import textwrap
from typing import Any, Callable

import pandas as pd

SafeDivideFn = Callable[[float, float], float | None]
FmtCompactFn = Callable[[float | int | None], str]
FmtMoneyFn = Callable[[float | int | None], str]


def render_funnel_and_ga4(
    *,
    st_module: Any,
    section_set: set[str],
    df_sel: pd.DataFrame,
    metric_cols: dict[str, str],
    platform: str,
    ga4_event_df: pd.DataFrame,
    ga4_conversion_event_name: str,
    default_ga4_event_name: str,
    start_date: Any,
    end_date: Any,
    sdiv_fn: SafeDivideFn,
    fmt_compact_fn: FmtCompactFn,
    fmt_money_fn: FmtMoneyFn,
    c_google: str,
    c_meta: str,
    c_accent: str,
    c_mute: str,
) -> None:
    if "funnel" in section_set:
        impr = float(df_sel[metric_cols["impr"]].sum())
        clicks = float(df_sel[metric_cols["clicks"]].sum())
        conv = float(df_sel[metric_cols["conv"]].sum())
        sess_total = float(df_sel["ga4_sessions"].sum())
        sess = (
            sess_total
            if platform == "All"
            else sess_total * (sdiv_fn(clicks, float(df_sel["total_clicks"].sum())) or 0.0)
        )
        if impr <= 0 and clicks > 0:
            impr = clicks / 0.03

        funnel_vals = [
            ("Impresiones", max(impr, 0.0)),
            ("Clics", max(min(clicks, impr), 0.0)),
            ("Sesiones", max(min(sess, clicks), 0.0)),
            ("Conversiones", max(min(conv, sess), 0.0)),
        ]
        top_val = max(float(funnel_vals[0][1]), 1.0)
        funnel_stage_colors = [c_google, c_accent, c_mute, c_meta]
        rows_html: list[str] = []
        prev_val: float | None = None
        for idx, (name, value) in enumerate(funnel_vals):
            pct = 0.0 if value <= 0 else min(max((value / top_val) * 100.0, 0.0), 100.0)
            stage_color = funnel_stage_colors[idx % len(funnel_stage_colors)]
            if prev_val is None or prev_val <= 0:
                drop_html = "<span class='funnel-drop funnel-drop-base' title='Etapa base'>Base</span>"
            else:
                drop_pct = max(0.0, min(100.0, (1.0 - sdiv_fn(value, prev_val)) * 100.0))
                drop_html = (
                    "<span class='funnel-drop' title='Ca\u00edda vs etapa anterior'>"
                    f"&darr; {drop_pct:.1f}%</span>"
                )
            rows_html.append(
                f"<div class='funnel-row'>"
                f"<div class='funnel-fill' style='width:{pct:.2f}%; background:{stage_color}33; border:1px solid {stage_color}66;'></div>"
                f"<div class='funnel-content'>"
                f"<span class='funnel-name'>{name}</span>"
                f"<span class='funnel-metrics'><span class='funnel-value'>{fmt_compact_fn(value)}</span>{drop_html}</span>"
                f"</div>"
                f"</div>"
            )
            prev_val = value

        st_module.markdown(
            textwrap.dedent(
                f"""
                <div class='funnel-card'>
                  <div class='funnel-title'>Funnel de Conversi\u00f3n</div>
                  <div class='funnel-stack'>{''.join(rows_html)}</div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    if "ga4_conversion" in section_set:
        ga4_event_name = (
            str(ga4_conversion_event_name or default_ga4_event_name).strip() or default_ga4_event_name
        )
        ga4_filtered = ga4_event_df.copy() if not ga4_event_df.empty else pd.DataFrame()
        ga4_conv_total = 0.0
        if not ga4_filtered.empty:
            if "date" in ga4_filtered.columns:
                ga4_filtered = ga4_filtered[
                    (ga4_filtered["date"] >= start_date) & (ga4_filtered["date"] <= end_date)
                ]
            event_col = (
                "eventName"
                if "eventName" in ga4_filtered.columns
                else ("event_name" if "event_name" in ga4_filtered.columns else None)
            )
            if event_col:
                ga4_filtered = ga4_filtered[
                    ga4_filtered[event_col].astype(str).str.strip().str.lower()
                    == ga4_event_name.lower()
                ]
            if platform in ("Google", "Meta") and "platform" in ga4_filtered.columns:
                ga4_filtered = ga4_filtered[
                    ga4_filtered["platform"].astype(str).str.strip().str.lower()
                    == platform.lower()
                ]
            conv_col = (
                "conversions"
                if "conversions" in ga4_filtered.columns
                else (
                    "eventCount"
                    if "eventCount" in ga4_filtered.columns
                    else ("event_count" if "event_count" in ga4_filtered.columns else None)
                )
            )
            if conv_col:
                ga4_conv_total = float(
                    pd.to_numeric(ga4_filtered[conv_col], errors="coerce").fillna(0.0).sum()
                )

        spend_total = float(df_sel[metric_cols["spend"]].sum()) if not df_sel.empty else 0.0
        ga4_cpl = sdiv_fn(spend_total, ga4_conv_total)
        st_module.markdown(
            textwrap.dedent(
                f"""
                <div class='ga4-conv-card'>
                  <div class='ga4-conv-title'>Conversiones GA4</div>
                  <div class='ga4-conv-event'>Evento: {html.escape(ga4_event_name)}</div>
                  <div class='ga4-conv-grid'>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>Conversiones</span>
                      <span class='ga4-conv-value'>{fmt_compact_fn(ga4_conv_total)}</span>
                    </div>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>Inversi\u00f3n</span>
                      <span class='ga4-conv-value'>{fmt_money_fn(spend_total)}</span>
                    </div>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>Plataforma</span>
                      <span class='ga4-conv-value'>{html.escape(platform)}</span>
                    </div>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>CPL GA4</span>
                      <span class='ga4-conv-value'>{fmt_money_fn(ga4_cpl)}</span>
                    </div>
                  </div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )
