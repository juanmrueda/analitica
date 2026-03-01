
#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

REPORT_PATH = Path(__file__).resolve().parent / "reports" / "yap" / "YAP_historical.json"
LOGO_PATH = Path(__file__).resolve().parent / "assets" / "logo-ipalmera-growth-marketing.webp"
LOGO_PLACEHOLDER = "https://via.placeholder.com/260x80/F8FAFC/0F172A?text=iPalmera+Logo"
META_ACCOUNT_ID = "1808641036591815"
GOOGLE_CUSTOMER_ID = "6495122409"
GA4_GTC_SOLICITAR_CODIGO_EVENT = "form_gtc_otp_solicitar_codigo"

C_GOOGLE = "#1A73E8"
C_META = "#FF2D55"
C_ACCENT = "#0A84FF"
C_TEXT = "#1D1D1F"
C_MUTE = "#6E6E73"
C_PANEL_BORDER = "#E5E5EA"
C_PANEL_BG = "#FFFFFF"
C_GRID = "#ECECF1"


def sf(v: Any) -> float:
    if v in (None, ""):
        return 0.0
    if isinstance(v, list):
        return sum(sf(x) for x in v)
    if isinstance(v, dict):
        return sf(v.get("value", 0.0))
    if isinstance(v, str):
        v = v.strip().replace(",", "")
    return float(v)


def sdiv(a: float, b: float) -> float | None:
    return None if b == 0 else a / b


def fmt_money(v: float | None) -> str:
    return "N/A" if v is None else f"${v:,.2f}"


def fmt_pct(v: float | None) -> str:
    return "N/A" if v is None else f"{v*100:.2f}%"


def fmt_delta(v: float | None) -> str:
    return "N/A" if v is None else f"{v:+.1f}% vs periodo anterior"


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    sec = max(float(seconds), 0.0)
    if sec < 60:
        return f"{sec:.0f} s"
    mins = int(sec // 60)
    rem = int(round(sec % 60))
    if mins < 60:
        return f"{mins} min {rem}s"
    hrs = mins // 60
    mins_rem = mins % 60
    return f"{hrs} h {mins_rem} min"


def pct_delta(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or prev == 0:
        return None
    return ((cur - prev) / abs(prev)) * 100.0


def apply_theme() -> None:
    st.markdown(
        """
        <style>
          #MainMenu { visibility: hidden; }
          [data-testid="stToolbarActions"] { display: none !important; }
          [data-testid="stStatusWidget"] { display: none !important; }
          [data-testid="stAppDeployButton"] { display: none !important; }
          [data-testid="stHeaderActionElements"] { display: none !important; }
          header[data-testid="stHeader"] {
            background: transparent !important;
            border-bottom: none !important;
          }
          div[data-testid="stToolbar"] {
            background: transparent !important;
            border: none !important;
          }
          [data-testid="collapsedControl"] {
            position: fixed;
            top: 0.8rem;
            left: 0.8rem;
            z-index: 1200;
          }
          [data-testid="collapsedControl"] button,
          [data-testid="stSidebarCollapseButton"] button {
            width: 2.25rem !important;
            height: 2.25rem !important;
            border-radius: 10px !important;
            border: 1px solid rgba(255, 255, 255, 0.18) !important;
            background: rgba(15, 23, 42, 0.92) !important;
            color: #E5E7EB !important;
            box-shadow: 0 6px 14px rgba(2, 6, 23, 0.28) !important;
          }
          [data-testid="collapsedControl"] button:hover,
          [data-testid="stSidebarCollapseButton"] button:hover {
            background: rgba(2, 6, 23, 0.95) !important;
          }
          [data-testid="collapsedControl"] button svg,
          [data-testid="stSidebarCollapseButton"] button svg {
            fill: #E5E7EB !important;
            stroke: #E5E7EB !important;
          }
          .stApp {
            color: #1D1D1F;
            font-family: "SF Pro Display", "SF Pro Text", "Avenir Next", "Helvetica Neue", sans-serif;
            background:
              radial-gradient(1200px 600px at 10% -10%, rgba(10,132,255,0.10), transparent 55%),
              radial-gradient(900px 500px at 95% 0%, rgba(255,45,85,0.07), transparent 50%),
              linear-gradient(180deg, #F4F6F9 0%, #EEF1F6 100%);
          }
          .block-container {
            max-width: 104rem;
            padding-top: 0.7rem;
            padding-bottom: 2.2rem;
          }
          [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0E1014 0%, #171A20 100%);
            border-right: 1px solid #2A2F39;
          }
          [data-testid="stSidebarContent"] {
            display: flex;
            flex-direction: column;
            min-height: 100vh;
          }
          [data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
          [data-testid="stSidebar"] label,
          [data-testid="stSidebar"] p,
          [data-testid="stSidebar"] h1,
          [data-testid="stSidebar"] h2,
          [data-testid="stSidebar"] h3 {
            color: #F6F7FB !important;
          }
          .logo-subtitle {
            margin-top: -0.35rem;
            margin-bottom: 0.15rem;
            text-align: center;
            font-size: 1.05rem;
            letter-spacing: 0.04em;
            font-weight: 800;
            color: #39FF14 !important;
            text-shadow: 0 0 10px rgba(57, 255, 20, 0.48);
            text-transform: none;
          }
          .meta-token-side {
            margin-top: auto;
            margin-bottom: 0.45rem;
          }
          .meta-token-box {
            background: rgba(255, 255, 255, 0.95);
            border: 1px solid #CBD5E1;
            border-radius: 12px;
            padding: 0.44rem 0.58rem;
            text-align: center;
            box-shadow: 0 6px 14px rgba(2, 6, 23, 0.2);
          }
          .meta-token-box .mth-title {
            color: #334155 !important;
            font-size: 0.65rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            margin-bottom: 0.18rem;
          }
          .meta-token-box .mth-value {
            color: #0F172A !important;
            font-size: 0.93rem;
            font-weight: 700;
            line-height: 1.1;
          }
          .meta-token-box * { color: #0F172A !important; }
          .app-filter-title {
            margin-top: 0.15rem;
            margin-bottom: 0.25rem;
            font-size: 0.72rem;
            letter-spacing: 0.09em;
            text-transform: uppercase;
            font-weight: 800;
            color: #4B5563;
          }
          .app-filter-helper {
            margin-top: 0.08rem;
            margin-bottom: 0.35rem;
            font-size: 0.8rem;
            color: #6B7280;
          }
          .filter-chip-row {
            display: flex;
            gap: 0.35rem;
            flex-wrap: wrap;
            margin-bottom: 0.45rem;
          }
          .filter-chip {
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            padding: 0.3rem 0.55rem;
            border-radius: 999px;
            border: 1px solid rgba(203, 213, 225, 0.95);
            background: rgba(255, 255, 255, 0.86);
            color: #334155;
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            box-shadow: 0 4px 10px rgba(15, 23, 42, 0.08);
          }
          .filter-chip .k {
            font-size: 0.66rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: #64748B;
            font-weight: 800;
          }
          .app-filter-divider {
            height: 1px;
            margin: 0.2rem 0 0.65rem 0;
            background: linear-gradient(90deg, rgba(148,163,184,0.45), rgba(148,163,184,0.05));
          }
          [data-testid="stMain"] .stDateInput > div > div,
          [data-testid="stMain"] .stSelectbox > div > div {
            border-radius: 12px !important;
            border: 1px solid rgba(203, 213, 225, 0.9) !important;
            background: rgba(255, 255, 255, 0.92) !important;
            box-shadow: 0 5px 14px rgba(15, 23, 42, 0.06);
          }
          [data-testid="stMain"] .stDateInput label,
          [data-testid="stMain"] .stSelectbox label {
            font-size: 0.72rem !important;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: #64748B !important;
            font-weight: 700 !important;
          }
          [data-testid="stMain"] .stDateInput input {
            font-size: 0.88rem !important;
            color: #0F172A !important;
          }
          [data-testid="stMain"] .stSelectbox [data-baseweb="select"] > div {
            min-height: 2.55rem !important;
          }
          [data-testid="stMain"] .stDateInput [data-baseweb="input"] {
            min-height: 2.55rem !important;
          }
          [data-testid="stMain"] div[data-testid="stPlotlyChart"] {
            border: 1px solid rgba(226, 232, 240, 0.95);
            border-radius: 14px;
            overflow: hidden;
            background: rgba(255,255,255,0.88);
            box-shadow: 0 8px 20px rgba(15,23,42,0.06);
          }
          [data-testid="stSidebar"] .stDateInput,
          [data-testid="stSidebar"] .stSelectbox > div > div {
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.14);
            border-radius: 12px;
          }
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
            color: #F8FAFC !important;
          }
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] svg {
            fill: #E2E8F0 !important;
          }
          [data-testid="stSidebar"] .stDateInput input {
            background: #4B5563 !important;
            color: #FFFFFF !important;
            border-radius: 10px !important;
          }
          [data-baseweb="popover"] [role="listbox"] {
            background: #FFFFFF !important;
            color: #0F172A !important;
            border: 1px solid #CBD5E1 !important;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.14) !important;
          }
          [data-baseweb="popover"] [role="option"] {
            color: #0F172A !important;
            background: #FFFFFF !important;
          }
          [data-baseweb="popover"] [role="option"]:hover {
            color: #0F172A !important;
            background: #EEF2FF !important;
          }
          [data-baseweb="popover"] [role="option"][aria-selected="true"],
          [data-baseweb="popover"] [aria-selected="true"] {
            background: #0F172A !important;
            color: #FFFFFF !important;
          }
          [data-testid="stSidebar"] [data-baseweb="calendar"] [aria-selected="true"] {
            background: #6B7280 !important;
            color: #FFFFFF !important;
          }
          [data-testid="stSidebar"] [data-baseweb="calendar"] [aria-label*="selected"] {
            background: #6B7280 !important;
            color: #FFFFFF !important;
          }
          .hero {
            background: rgba(255,255,255,0.66);
            border: 1px solid rgba(255,255,255,0.85);
            border-radius: 22px;
            padding: 1.1rem 1.2rem;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            margin-bottom: 0.75rem;
          }
          .hero-kicker {
            color: #6E6E73;
            text-transform: uppercase;
            letter-spacing: .08em;
            font-size: .72rem;
            font-weight: 700;
          }
          .hero-title {
            color: #111114;
            font-size: 1.9rem;
            font-weight: 700;
            margin-top: .12rem;
            line-height: 1.15;
          }
          .hero-sub {
            color: #4F5563;
            font-size: .92rem;
            margin-top: .25rem;
          }
          div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.74);
            border: 1px solid rgba(255,255,255,0.9);
            border-radius: 18px;
            padding: 12px 10px;
            box-shadow: 0 8px 22px rgba(15,23,42,0.07);
            backdrop-filter: blur(7px);
            -webkit-backdrop-filter: blur(7px);
          }
          div[data-testid="stMetricLabel"] { color: #6E6E73 !important; font-weight: 600; }
          div[data-testid="stMetricValue"] { color: #141416 !important; font-weight: 700; }
          .card {
            background: rgba(255,255,255,0.74);
            border: 1px solid rgba(255,255,255,0.9);
            border-radius: 20px;
            padding: 16px;
            box-shadow: 0 8px 22px rgba(15,23,42,0.07);
            backdrop-filter: blur(7px);
            -webkit-backdrop-filter: blur(7px);
          }
          .kicker {
            color: #6E6E73;
            text-transform: uppercase;
            letter-spacing: .06em;
            font-size: .77rem;
            font-weight: 700;
          }
          .kpis li { margin-bottom:.32rem; }
          .section-title {
            margin-top: 1.22rem;
            margin-bottom: .58rem;
            padding: .22rem .12rem .5rem .12rem;
            border-bottom: 1px solid #DEE1E8;
            font-size: 1.03rem;
            font-weight: 700;
            color: #1D1D1F;
            letter-spacing: .01em;
          }
          div[data-testid="stDataFrame"] {
            border: 1px solid rgba(255,255,255,0.95);
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 8px 22px rgba(15,23,42,0.07);
            background: rgba(255,255,255,0.78);
          }
          [data-baseweb="tab-list"] {
            gap: .55rem;
            background: rgba(15, 23, 42, 0.06);
            border: 1px solid rgba(255,255,255,0.96);
            padding: .45rem;
            border-radius: 14px;
            box-shadow: 0 8px 20px rgba(15,23,42,0.06);
          }
          [data-baseweb="tab"] {
            min-height: 2.6rem;
            border-radius: 11px;
            color: #596173;
            font-weight: 800;
            font-size: 1rem;
            text-transform: uppercase;
            letter-spacing: .06em;
          }
          [aria-selected="true"][data-baseweb="tab"] {
            background: #0F172A !important;
            color: #FFFFFF !important;
            box-shadow: 0 5px 12px rgba(15,23,42,0.20);
          }
          .daily-fact {
            background: linear-gradient(90deg, rgba(15,23,42,0.96) 0%, rgba(30,41,59,0.95) 100%);
            color: #E2E8F0;
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 14px;
            padding: 0.7rem 0.85rem;
            margin: 0.1rem 0 0.75rem 0;
            box-shadow: 0 8px 18px rgba(15,23,42,0.18);
          }
          .daily-fact .title {
            font-size: 0.74rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            color: #93C5FD;
          }
          .daily-fact .body {
            margin-top: 0.18rem;
            font-size: 0.94rem;
            font-weight: 600;
            color: #F8FAFC;
            line-height: 1.25;
          }
          @media (max-width: 960px) {
            .block-container {
              max-width: 100%;
              padding-top: 0.55rem;
              padding-bottom: 1.4rem;
              padding-left: 0.6rem;
              padding-right: 0.6rem;
            }
            [data-testid="stSidebar"] {
              display: none !important;
            }
            [data-testid="collapsedControl"] {
              display: none !important;
            }
            [data-testid="stSidebarCollapseButton"] {
              display: none !important;
            }
            [data-testid="collapsedControl"] button,
            [data-testid="stSidebarCollapseButton"] button {
              width: 2.5rem !important;
              height: 2.5rem !important;
              border-radius: 12px !important;
            }
            .hero {
              border-radius: 18px;
              padding: 0.9rem 0.85rem;
              margin-top: 0.3rem;
            }
            .hero-kicker {
              font-size: 0.62rem;
              letter-spacing: 0.07em;
            }
            .hero-title {
              font-size: 1.35rem;
              line-height: 1.2;
            }
            .hero-sub {
              font-size: 0.82rem;
              line-height: 1.35;
            }
            [data-baseweb="tab-list"] {
              gap: 0.35rem;
              padding: 0.26rem;
              border-radius: 12px;
              overflow-x: auto;
              overflow-y: hidden;
              -webkit-overflow-scrolling: touch;
            }
            [data-baseweb="tab"] {
              min-height: 2.15rem;
              font-size: 0.7rem;
              letter-spacing: 0.03em;
              padding: 0.34rem 0.5rem !important;
              text-align: center;
              white-space: nowrap !important;
              min-width: max-content;
            }
            .app-filter-title {
              font-size: 0.64rem;
              margin-top: 0.05rem;
              margin-bottom: 0.12rem;
            }
            .app-filter-helper {
              font-size: 0.72rem;
              margin-bottom: 0.24rem;
            }
            .filter-chip-row {
              margin-bottom: 0.32rem;
              gap: 0.26rem;
            }
            .filter-chip {
              font-size: 0.67rem;
              padding: 0.22rem 0.44rem;
            }
            .filter-chip .k {
              font-size: 0.6rem;
            }
            .section-title {
              margin-top: 0.95rem;
              margin-bottom: 0.45rem;
              padding-bottom: 0.35rem;
              font-size: 0.92rem;
            }
            div[data-testid="stMetric"] {
              border-radius: 14px;
              padding: 10px 9px;
            }
            div[data-testid="stMetricLabel"] p {
              font-size: 0.74rem !important;
            }
            div[data-testid="stMetricValue"] {
              font-size: 1.18rem !important;
              line-height: 1.1 !important;
            }
            .daily-fact {
              border-radius: 12px;
              padding: 0.65rem 0.72rem;
            }
            .daily-fact .body {
              font-size: 0.86rem;
            }
            [data-testid="stMain"] [data-testid="stHorizontalBlock"] {
              flex-wrap: wrap !important;
              gap: 0.55rem !important;
            }
            [data-testid="stMain"] [data-testid="column"] {
              min-width: 100% !important;
              flex: 1 1 100% !important;
            }
            [data-testid="stMain"] .stDateInput [data-baseweb="input"],
            [data-testid="stMain"] .stSelectbox [data-baseweb="select"] > div {
              min-height: 2.35rem !important;
            }
            [data-testid="stMain"] div[data-testid="stPlotlyChart"] {
              border-radius: 12px;
            }
            div[data-testid="stDataFrame"] {
              border-radius: 12px;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def section_title(text: str) -> None:
    st.markdown(f"<div class='section-title'>{text}</div>", unsafe_allow_html=True)


def pbi_layout(
    fig: go.Figure,
    *,
    yaxis_title: str | None = None,
    xaxis_title: str | None = None,
    legend_h: bool = True,
    y2_title: str | None = None,
) -> go.Figure:
    layout: dict[str, Any] = {
        "template": "plotly_white",
        "paper_bgcolor": C_PANEL_BG,
        "plot_bgcolor": C_PANEL_BG,
        "margin": {"l": 10, "r": 10, "t": 14, "b": 10},
        "font": {"family": "SF Pro Display, SF Pro Text, Avenir Next, Helvetica Neue, sans-serif", "color": C_TEXT, "size": 12},
        "hoverlabel": {"bgcolor": "#FFFFFF", "bordercolor": C_PANEL_BORDER, "font": {"color": C_TEXT}},
        "xaxis": {
            "showgrid": False,
            "linecolor": C_PANEL_BORDER,
            "tickfont": {"color": C_MUTE},
            "title": {"text": xaxis_title or ""},
        },
        "yaxis": {
            "showgrid": True,
            "gridcolor": C_GRID,
            "zeroline": False,
            "linecolor": C_PANEL_BORDER,
            "tickfont": {"color": C_MUTE},
            "title": {"text": yaxis_title or ""},
        },
    }
    if legend_h:
        layout["legend"] = {"orientation": "h", "x": 0.0, "y": 1.1, "bgcolor": "rgba(255,255,255,0.7)"}
    if y2_title:
        layout["yaxis2"] = {
            "title": {"text": y2_title},
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "tickfont": {"color": C_MUTE},
        }
    fig.update_layout(**layout)
    return fig


def render_hero(s, e, platform: str) -> None:
    st.markdown(
        f"""
        <div class='hero'>
          <div class='hero-kicker'>Executive Marketing Command Center</div>
          <div class='hero-title'>YAP Marketing Performance</div>
          <div class='hero-sub'>Rango activo: {s.isoformat()} a {e.isoformat()} | Plataforma: {platform}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_daily_fact(df: pd.DataFrame, platform: str) -> None:
    if df.empty:
        return
    uniq_days = sorted(d for d in df["date"].dropna().unique())
    if len(uniq_days) < 2:
        return
    day_cur = uniq_days[-1]
    day_prev = uniq_days[-2]
    c = metric_cols(platform)

    cur_row = df[df["date"] == day_cur]
    prev_row = df[df["date"] == day_prev]
    if cur_row.empty or prev_row.empty:
        return

    cur_spend = float(cur_row[c["spend"]].sum())
    cur_impr = float(cur_row[c["impr"]].sum())
    cur_conv = float(cur_row[c["conv"]].sum())
    prev_spend = float(prev_row[c["spend"]].sum())
    prev_impr = float(prev_row[c["impr"]].sum())
    prev_conv = float(prev_row[c["conv"]].sum())
    cur_cpl = sdiv(cur_spend, cur_conv)
    prev_cpl = sdiv(prev_spend, prev_conv)

    d_impr = pct_delta(cur_impr, prev_impr)
    d_conv = pct_delta(cur_conv, prev_conv)
    d_cpl = pct_delta(cur_cpl, prev_cpl)

    if (d_conv or 0) >= 8 and (d_cpl or 0) <= -5:
        body = (
            f"Ayer ({day_cur.isoformat()}) tuvimos un buen día: las conversiones subieron "
            f"{fmt_delta(d_conv)} y el CPL mejoró {fmt_delta(d_cpl)}. Vale la pena mantener este enfoque."
        )
    elif (d_impr or 0) > 12 and (d_conv or 0) < 0:
        body = (
            f"Te comparto un hallazgo de ayer ({day_cur.isoformat()}): las impresiones crecieron "
            f"{fmt_delta(d_impr)}, pero las conversiones bajaron {fmt_delta(d_conv)}. "
            "Recomiendo revisar segmentación y creativos hoy mismo."
        )
    else:
        body = (
            f"Resumen de ayer ({day_cur.isoformat()}): impresiones {fmt_delta(d_impr)}, "
            f"conversiones {fmt_delta(d_conv)} y CPL {fmt_delta(d_cpl)}."
        )

    dismiss_key = f"dismiss_fact_{platform}_{day_cur.isoformat()}"
    if st.session_state.get(dismiss_key, False):
        return

    a, b = st.columns([12, 1], gap="small")
    with a:
        st.markdown(
            f"""
            <div class="daily-fact">
              <div class="title">Insight Diario</div>
              <div class="body">{body}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with b:
        if st.button("Cerrar", key=f"close_fact_btn_{dismiss_key}", use_container_width=True):
            st.session_state[dismiss_key] = True
            st.rerun()


def load_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"No se encontro: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ga4_source_platform(source_medium: Any) -> str:
    s = str(source_medium or "").lower()
    if any(k in s for k in ("facebook", "instagram", "meta /", "fb /", "facebook /", "instagram /")):
        return "Meta"
    if any(
        k in s
        for k in (
            "google /",
            "adwords",
            "search,adwords",
            "googleads",
            "google / cpc",
            "google / paid",
        )
    ):
        return "Google"
    return "Other"


def _coerce_date_value(value: Any, min_d: date, max_d: date) -> date:
    if isinstance(value, pd.Timestamp):
        value = value.date()
    elif hasattr(value, "date") and not isinstance(value, date):
        try:
            value = value.date()
        except Exception:
            pass

    if isinstance(value, date):
        d = value
    else:
        d = max_d
    if d < min_d:
        return min_d
    if d > max_d:
        return max_d
    return d


def _normalize_date_range(sel: Any, min_d: date, max_d: date) -> tuple[date, date]:
    if isinstance(sel, (tuple, list)):
        vals = [v for v in sel if v is not None]
        if len(vals) >= 2:
            s = _coerce_date_value(vals[0], min_d, max_d)
            e = _coerce_date_value(vals[1], min_d, max_d)
        elif len(vals) == 1:
            s = e = _coerce_date_value(vals[0], min_d, max_d)
        else:
            s = e = max_d
    else:
        s = e = _coerce_date_value(sel, min_d, max_d)
    if s > e:
        s, e = e, s
    return s, e


def daily_df(report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in report.get("daily", []):
        m, g, ga4, n = r.get("meta", {}), r.get("google_ads", {}), r.get("ga4", {}), r.get("normalization", {})
        ms, gs = sf(m.get("spend")), sf(g.get("cost"))
        mc, gc = sf(m.get("clicks")), sf(g.get("clicks"))
        mv, gv = sf(m.get("conversions")), sf(g.get("conversions"))
        mi, gi = sf(m.get("impressions")), sf(g.get("impressions"))
        rows.append(
            {
                "date": r.get("date"),
                "meta_spend": ms, "google_spend": gs, "total_spend": sf(n.get("total_spend")) or (ms + gs),
                "meta_clicks": mc, "google_clicks": gc, "total_clicks": sf(n.get("total_clicks")) or (mc + gc),
                "meta_conv": mv, "google_conv": gv, "total_conv": sf(n.get("total_conversions")) or (mv + gv),
                "meta_impr": mi, "google_impr": gi, "total_impr": sf(n.get("total_impressions")) or (mi + gi),
                "ga4_sessions": sf(ga4.get("sessions")), "ga4_users": sf(ga4.get("totalUsers")),
                "ga4_avg_sess": sf(ga4.get("averageSessionDuration")), "ga4_bounce": sf(ga4.get("bounceRate")),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    num_cols = [c for c in df.columns if c != "date"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def acq_df(report: dict[str, Any], key: str) -> pd.DataFrame:
    rows = report.get("traffic_acquisition", {}).get(key, [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for c in df.columns:
        if c != "date" and df[c].dtype == object:
            try:
                df[c] = pd.to_numeric(df[c])
            except Exception:
                pass
    return df.dropna(subset=["date"]).reset_index(drop=True) if "date" in df.columns else df


def paid_device_df(report: dict[str, Any]) -> pd.DataFrame:
    df = acq_df(report, "paid_device_daily")
    if df.empty:
        return df
    required = ["platform", "device", "spend", "impressions", "clicks", "conversions"]
    for col in required:
        if col not in df.columns:
            df[col] = "" if col in ("platform", "device") else 0.0
    for col in ("spend", "impressions", "clicks", "conversions"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["device"] = (
        df["device"]
        .astype(str)
        .str.strip()
        .replace({"desktop": "Desktop", "mobile": "Mobile", "other": "Other"})
    )
    return df


def metric_cols(platform: str) -> dict[str, str]:
    if platform == "Google":
        return {"spend": "google_spend", "clicks": "google_clicks", "conv": "google_conv", "impr": "google_impr"}
    if platform == "Meta":
        return {"spend": "meta_spend", "clicks": "meta_clicks", "conv": "meta_conv", "impr": "meta_impr"}
    return {"spend": "total_spend", "clicks": "total_clicks", "conv": "total_conv", "impr": "total_impr"}


def summary(df: pd.DataFrame, platform: str) -> dict[str, float | None]:
    c = metric_cols(platform)
    spend = float(df[c["spend"]].sum()) if not df.empty else 0.0
    clicks = float(df[c["clicks"]].sum()) if not df.empty else 0.0
    conv = float(df[c["conv"]].sum()) if not df.empty else 0.0
    impr = float(df[c["impr"]].sum()) if not df.empty else 0.0
    sessions_total = float(df["ga4_sessions"].sum()) if not df.empty else 0.0
    avg_sess_weighted = (
        float((df["ga4_avg_sess"] * df["ga4_sessions"]).sum()) / sessions_total
        if sessions_total > 0
        else 0.0
    )
    return {
        "spend": spend,
        "clicks": clicks,
        "conv": conv,
        "impr": impr,
        "cpl": sdiv(spend, conv),
        "ctr": sdiv(clicks, impr),
        "sessions": sessions_total,
        "users": float(df["ga4_users"].sum()) if not df.empty else 0.0,
        # GA4-consistent: weighted by sessions across the selected period.
        "avg_sess": avg_sess_weighted,
        "bounce": float(df["ga4_bounce"].mean()) if not df.empty else 0.0,
    }

def render_sidebar(report: dict[str, Any]) -> None:
    st.sidebar.markdown("##")
    a, b, c = st.sidebar.columns([1, 4, 1])
    with b:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), use_container_width=True)
        else:
            st.image(LOGO_PLACEHOLDER, use_container_width=True)
        st.markdown("<div class='logo-subtitle'>IA Analítica</div>", unsafe_allow_html=True)
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Panel")
    st.sidebar.caption("Usa los filtros en la parte superior del dashboard.")
    t = report.get("metadata", {}).get("meta_token_status", {})
    days = t.get("days_left")
    status = "No disponible" if days is None else f"{int(float(days))} días"
    st.sidebar.markdown(
        f"""
        <div class="meta-token-side">
          <div class="meta-token-box">
            <div class="mth-title">Meta Token Health</div>
            <div class="mth-value">{status}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_top_filters(min_d: date, max_d: date) -> tuple[date, date, str]:
    st.markdown("<div class='app-filter-title'>Filtros del panel</div>", unsafe_allow_html=True)
    f1, f2 = st.columns([2.2, 1.0], gap="small")
    with f1:
        sel = st.date_input(
            "Rango de fechas",
            value=(max(min_d, max_d - timedelta(days=29)), max_d),
            min_value=min_d,
            max_value=max_d,
            key="top_date_range",
        )
    with f2:
        platform = st.selectbox(
            "Plataforma",
            ["All", "Google", "Meta"],
            index=0,
            key="top_platform",
        )
    st.markdown(
        "<div class='app-filter-helper'>Tip: en móvil este menú reemplaza la barra lateral para evitar superposiciones.</div>",
        unsafe_allow_html=True,
    )
    s, e = _normalize_date_range(sel, min_d, max_d)
    st.markdown(
        f"""
        <div class="filter-chip-row">
          <div class="filter-chip"><span class="k">Periodo</span> {s.isoformat()} a {e.isoformat()}</div>
          <div class="filter-chip"><span class="k">Plataforma</span> {platform}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='app-filter-divider'></div>", unsafe_allow_html=True)
    return s, e, platform


def render_exec(
    df_sel: pd.DataFrame,
    df_prev: pd.DataFrame,
    platform: str,
    paid_dev_df: pd.DataFrame,
    camp_df: pd.DataFrame,
    ga4_event_df: pd.DataFrame,
    s,
    e,
    prev_s,
    prev_e,
):
    cur, prev = summary(df_sel, platform), summary(df_prev, platform)
    cur_days, prev_days = max(len(df_sel), 1), len(df_prev)
    d_sp = pct_delta(sdiv(sf(cur["spend"]), float(cur_days)), sdiv(sf(prev["spend"]), float(prev_days)) if prev_days else None)
    d_cv = pct_delta(sdiv(sf(cur["conv"]), float(cur_days)), sdiv(sf(prev["conv"]), float(prev_days)) if prev_days else None)
    d_cpl = pct_delta(cur["cpl"], prev["cpl"])
    d_ctr = pct_delta(cur["ctr"], prev["ctr"])
    ga4_conv_cur: float | None = None
    ga4_conv_prev: float | None = None
    if not ga4_event_df.empty and {"date", "eventName", "eventCount"}.issubset(set(ga4_event_df.columns)):
        ev = ga4_event_df[ga4_event_df["eventName"].astype(str) == GA4_GTC_SOLICITAR_CODIGO_EVENT].copy()
        if "platform" not in ev.columns and "sessionSourceMedium" in ev.columns:
            ev["platform"] = ev["sessionSourceMedium"].map(_ga4_source_platform)
        if platform in ("Google", "Meta") and "platform" in ev.columns:
            ev = ev[ev["platform"] == platform]
        ev_cur = ev[(ev["date"] >= s) & (ev["date"] <= e)].copy()
        ev_prev = ev[(ev["date"] >= prev_s) & (ev["date"] <= prev_e)].copy()
        ga4_conv_cur = float(pd.to_numeric(ev_cur["eventCount"], errors="coerce").fillna(0.0).sum())
        ga4_conv_prev = float(pd.to_numeric(ev_prev["eventCount"], errors="coerce").fillna(0.0).sum())
    ga4_conv_delta = pct_delta(ga4_conv_cur, ga4_conv_prev) if ga4_conv_cur is not None and ga4_conv_prev is not None else None

    section_title("1) North Star Metrics")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto Total", fmt_money(cur["spend"]), fmt_delta(d_sp))
    c2.metric("Conversiones", f"{cur['conv']:,.0f}", fmt_delta(d_cv))
    c3.metric("CPL Promedio", fmt_money(cur["cpl"]), fmt_delta(d_cpl), delta_color="inverse")
    c4.metric("CTR", fmt_pct(cur["ctr"]), fmt_delta(d_ctr))

    section_title("2) Cross-Channel Performance")
    ld = df_sel.sort_values("date")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=ld["date"], y=ld["google_spend"], mode="lines+markers", name="Google", line={"color": C_GOOGLE, "width": 3}))
    fig.add_trace(go.Scatter(x=ld["date"], y=ld["meta_spend"], mode="lines+markers", name="Meta", line={"color": C_META, "width": 3}))
    pbi_layout(fig, yaxis_title="Gasto", xaxis_title="Fecha")
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    section_title("3) El Embudo de Conversion")
    c = metric_cols(platform)
    impr = float(df_sel[c["impr"]].sum())
    clicks = float(df_sel[c["clicks"]].sum())
    conv = float(df_sel[c["conv"]].sum())
    sess_total = float(df_sel["ga4_sessions"].sum())
    sess = sess_total if platform == "All" else sess_total * (sdiv(clicks, float(df_sel["total_clicks"].sum())) or 0.0)
    if impr <= 0 and clicks > 0:
        impr = clicks / 0.03
    stages = ["Impresiones", "Clics", "Sesiones (GA4)", "Conversiones"]
    stages_display = ["", "Clics", "Sesiones (GA4)", "Conversiones"]
    vals = [max(impr, 0.0), max(min(clicks, impr), 0.0), max(min(sess, clicks), 0.0), max(min(conv, sess), 0.0)]
    emb_col, ga4_col = st.columns([4.2, 1.5], gap="medium")
    with emb_col:
        st.markdown(
            f"<div style='text-align:center;font-size:1.08rem;font-weight:700;color:#1D1D1F;margin:0 0 8px 0;'>"
            f"Impresiones: {vals[0]:,.0f}</div>",
            unsafe_allow_html=True,
        )
        stage_colors = ["#5AC8FA", "#0A84FF", "#30B0C7", "#FF375F"]
        stage_texts = ["", f"{vals[1]:,.0f}", f"{vals[2]:,.0f}", f"{vals[3]:,.0f}"]
        ff = go.Figure(
                go.Funnel(
                y=stages_display,
                x=vals,
                text=stage_texts,
                textposition="outside",
                texttemplate="<b>%{label}</b><br>%{text}",
                insidetextfont={
                    "size": 15,
                    "family": "SF Pro Display, SF Pro Text, Avenir Next, Helvetica Neue, sans-serif",
                    "color": "#FFFFFF",
                },
                outsidetextfont={
                    "size": 16,
                    "family": "SF Pro Display, SF Pro Text, Avenir Next, Helvetica Neue, sans-serif",
                    "color": "#1D1D1F",
                },
                marker={
                    "color": stage_colors,
                    "line": {"color": "#FFFFFF", "width": 1.4},
                },
                connector={"line": {"color": "rgba(110,110,115,0.22)", "width": 1.0}},
                hovertemplate="%{label}: %{value:,.0f}<extra></extra>",
                opacity=0.98,
            )
        )
        pbi_layout(ff, xaxis_title="Volumen", yaxis_title="", legend_h=False)
        ff.update_layout(showlegend=False, margin={"l": 8, "r": 8, "t": 8, "b": 8}, height=430)
        ff.update_yaxes(categoryorder="array", categoryarray=stages_display, autorange="reversed")
        ff.update_yaxes(tickfont={"size": 15, "color": "#3A3D46"})
        ff.update_xaxes(tickfont={"size": 13, "color": "#5A5F6E"}, tickformat="~s")
        st.plotly_chart(ff, use_container_width=True)
    with ga4_col:
        st.markdown("<div class='kicker'>GA4</div>", unsafe_allow_html=True)
        if ga4_conv_cur is None:
            st.metric("Conversiones GA4", "N/A", "N/A")
            st.caption("Sin serie GA4 por evento en JSON.")
        else:
            st.metric("Conversiones GA4", f"{ga4_conv_cur:,.0f}", fmt_delta(ga4_conv_delta))
            ga4_cpl = sdiv(float(cur["spend"]), ga4_conv_cur)
            st.metric("CPL (GA4)", fmt_money(ga4_cpl))
            st.caption("Evento: form_gtc_otp_solicitar_codigo")

    section_title("4) Dispositivos de Pauta (Desktop / Mobile / Other)")
    pcur = paid_dev_df[
        (paid_dev_df["date"] >= s) & (paid_dev_df["date"] <= e)
    ].copy() if not paid_dev_df.empty else pd.DataFrame()
    pprev = paid_dev_df[
        (paid_dev_df["date"] >= prev_s) & (paid_dev_df["date"] <= prev_e)
    ].copy() if not paid_dev_df.empty else pd.DataFrame()
    if platform in ("Google", "Meta"):
        pcur = pcur[pcur["platform"] == platform]
        pprev = pprev[pprev["platform"] == platform]

    if pcur.empty:
        st.info("No hay datos de dispositivo de pauta para el rango seleccionado.")
    else:
        cur_roll = (
            pcur.groupby("device", as_index=False)
            .agg(
                spend=("spend", "sum"),
                impressions=("impressions", "sum"),
                clicks=("clicks", "sum"),
                conversions=("conversions", "sum"),
            )
        )
        prev_roll = (
            pprev.groupby("device", as_index=False)
            .agg(impressions_prev=("impressions", "sum"))
            if not pprev.empty
            else pd.DataFrame(columns=["device", "impressions_prev"])
        )
        cur_roll = cur_roll.merge(prev_roll, on="device", how="left").fillna({"impressions_prev": 0.0})
        cur_roll["ctr"] = cur_roll.apply(
            lambda r: sdiv(float(r["clicks"]), float(r["impressions"])),
            axis=1,
        )
        cur_roll["cpl"] = cur_roll.apply(
            lambda r: sdiv(float(r["spend"]), float(r["conversions"])),
            axis=1,
        )
        cur_roll["delta_impressions"] = cur_roll.apply(
            lambda r: pct_delta(float(r["impressions"]), float(r["impressions_prev"])),
            axis=1,
        )
        order = ["Desktop", "Mobile", "Other"]
        roll = pd.DataFrame({"device": order}).merge(cur_roll, on="device", how="left").fillna(0.0)

        mcols = st.columns(3)
        for idx, dname in enumerate(order):
            row = roll[roll["device"] == dname].iloc[0]
            mcols[idx].metric(
                f"{dname} Impresiones",
                f"{float(row['impressions']):,.0f}",
                fmt_delta(row["delta_impressions"]),
            )

        bar = go.Figure(
            go.Bar(
                x=roll["device"],
                y=roll["impressions"],
                marker={"color": [C_GOOGLE, C_META, "#6E6E73"]},
                text=[f"{v:,.0f}" for v in roll["impressions"]],
                textposition="outside",
            )
        )
        pbi_layout(bar, xaxis_title="Dispositivo", yaxis_title="Impresiones", legend_h=False)
        st.plotly_chart(bar, use_container_width=True)

        table = roll.rename(
            columns={
                "device": "Device",
                "spend": "Spend",
                "impressions": "Impressions",
                "clicks": "Clicks",
                "conversions": "Conversions",
                "ctr": "CTR",
                "cpl": "CPL",
            }
        )[
            ["Device", "Spend", "Impressions", "Clicks", "Conversions", "CTR", "CPL"]
        ]
        st.dataframe(
            table.style.format(
                {
                    "Spend": lambda v: fmt_money(float(v)),
                    "Impressions": "{:.0f}",
                    "Clicks": "{:.0f}",
                    "Conversions": "{:.2f}",
                    "CTR": lambda v: fmt_pct(v if pd.notna(v) else None),
                    "CPL": lambda v: fmt_money(v if pd.notna(v) else None),
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    section_title("5) Tabla Maestra de Auditoria")
    c = metric_cols(platform)
    t = df_sel[["date", "meta_spend", "google_spend", c["spend"], c["clicks"], c["impr"], c["conv"], "ga4_sessions", "ga4_avg_sess", "ga4_bounce"]].copy()
    t.columns = ["Date", "Meta Spend", "Google Spend", "Spend", "Clicks", "Impressions", "Conversions", "Sessions", "Avg Session (s)", "Bounce Rate"]
    t["CPL"] = t.apply(lambda r: sdiv(float(r["Spend"]), float(r["Conversions"])), axis=1)
    t["CTR"] = t.apply(lambda r: sdiv(float(r["Clicks"]), float(r["Impressions"])), axis=1)
    t = t.sort_values("Date", ascending=False)
    sty = (
        t.style.format({
            "Date": lambda v: v.isoformat() if hasattr(v, "isoformat") else str(v),
            "Meta Spend": lambda v: fmt_money(float(v)),
            "Google Spend": lambda v: fmt_money(float(v)),
            "Spend": lambda v: fmt_money(float(v)),
            "Clicks": "{:.0f}", "Impressions": "{:.0f}", "Conversions": "{:.2f}", "Sessions": "{:.0f}", "Avg Session (s)": "{:.1f}",
            "Bounce Rate": lambda v: fmt_pct(float(v)), "CPL": lambda v: fmt_money(v if pd.notna(v) else None), "CTR": lambda v: fmt_pct(v if pd.notna(v) else None),
        })
        .background_gradient(subset=["Spend"], cmap="Blues")
        .background_gradient(subset=["Conversions"], cmap="Greens")
        .background_gradient(subset=["CPL"], cmap="RdYlGn_r")
        .background_gradient(subset=["CTR"], cmap="PuBu")
    )
    st.dataframe(sty, use_container_width=True, hide_index=True)
    render_top_pieces_month(camp_df=camp_df, platform=platform, month_ref=e)


def render_top_pieces_month(camp_df: pd.DataFrame, platform: str, month_ref):
    section_title("Top 10 Piezas Del Mes")
    if camp_df.empty:
        st.info("No hay datos de piezas/campanas para construir el top 10.")
        return

    cp = camp_df.copy()
    if "date" not in cp.columns:
        st.info("El dataset de piezas no contiene columna de fecha.")
        return

    cp["date"] = pd.to_datetime(cp["date"], errors="coerce").dt.date
    cp = cp.dropna(subset=["date"])
    month_start = month_ref.replace(day=1)
    cp = cp[(cp["date"] >= month_start) & (cp["date"] <= month_ref)]

    if platform in ("Google", "Meta") and "platform" in cp.columns:
        cp = cp[cp["platform"] == platform]

    required_defaults: dict[str, Any] = {
        "platform": "",
        "campaign_id": "",
        "campaign_name": "",
        "spend": 0.0,
        "impressions": 0.0,
        "clicks": 0.0,
        "conversions": 0.0,
    }
    for col, default in required_defaults.items():
        if col not in cp.columns:
            cp[col] = default

    if cp.empty:
        st.info("No hay piezas/campanas para el mes seleccionado.")
        return

    top = (
        cp.groupby(["platform", "campaign_id", "campaign_name"], as_index=False)
        .agg(
            inversion=("spend", "sum"),
            impresiones=("impressions", "sum"),
            clics=("clicks", "sum"),
            conversiones=("conversions", "sum"),
        )
    )
    top["costo_conversion"] = top.apply(
        lambda r: sdiv(float(r["inversion"]), float(r["conversiones"])), axis=1
    )
    top["__cost_sort"] = top["costo_conversion"].fillna(float("inf"))
    top = top.sort_values(
        ["conversiones", "__cost_sort", "clics"],
        ascending=[False, True, False],
    ).head(10)

    def _piece_link(row: pd.Series) -> str:
        plat = str(row.get("platform", ""))
        campaign_id = str(row.get("campaign_id", "")).strip()
        if plat == "Meta":
            if campaign_id:
                return (
                    "https://adsmanager.facebook.com/adsmanager/manage/campaigns?"
                    f"act={META_ACCOUNT_ID}&selected_campaign_ids={campaign_id}"
                )
            return f"https://adsmanager.facebook.com/adsmanager/manage/campaigns?act={META_ACCOUNT_ID}"
        if plat == "Google":
            if campaign_id:
                return (
                    "https://ads.google.com/aw/campaigns?"
                    f"ocid={GOOGLE_CUSTOMER_ID}&campaignId={campaign_id}"
                )
            return f"https://ads.google.com/aw/campaigns?ocid={GOOGLE_CUSTOMER_ID}"
        return ""

    top["Ver Pieza"] = top.apply(_piece_link, axis=1)
    top["Pieza"] = top["campaign_name"].astype(str)

    out = top[
        [
            "Ver Pieza",
            "platform",
            "Pieza",
            "inversion",
            "impresiones",
            "clics",
            "conversiones",
            "costo_conversion",
        ]
    ].rename(
        columns={
            "platform": "Plataforma",
            "inversion": "Inversion",
            "impresiones": "Impresiones",
            "clics": "Clics",
            "conversiones": "Conversiones",
            "costo_conversion": "Costo Conversion",
        }
    )

    st.dataframe(
        out,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Ver Pieza": st.column_config.LinkColumn("Ver Pieza", display_text="Abrir"),
            "Inversion": st.column_config.NumberColumn("Inversion", format="$%.2f"),
            "Impresiones": st.column_config.NumberColumn("Impresiones", format="%d"),
            "Clics": st.column_config.NumberColumn("Clics", format="%d"),
            "Conversiones": st.column_config.NumberColumn("Conversiones", format="%.2f"),
            "Costo Conversion": st.column_config.NumberColumn("Costo Conversion", format="$%.2f"),
        },
    )
    st.caption(
        f"Mes analizado: {month_start.isoformat()} a {month_ref.isoformat()}. "
        "Enlace directo a la pieza/campana en la plataforma correspondiente."
    )


def render_traffic(df_sel, df_prev, ch_df, pg_df, camp_df, platform, s, e):
    section_title("03 > Rendimiento de Trafico y Adquisicion")
    sm = summary(df_sel, platform)
    sm_prev = summary(df_prev, platform)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sesiones", f"{sm['sessions']:,.0f}", fmt_delta(pct_delta(sm["sessions"], sm_prev["sessions"])))
    k2.metric("Usuarios", f"{sm['users']:,.0f}", fmt_delta(pct_delta(sm["users"], sm_prev["users"])))
    k3.metric(
        "Tiempo Promedio de Interaccion",
        fmt_duration(sm["avg_sess"]),
        fmt_delta(pct_delta(sm["avg_sess"], sm_prev["avg_sess"])),
    )
    k4.metric(
        "Tasa de Rebote",
        fmt_pct(sm["bounce"]),
        fmt_delta(pct_delta(sm["bounce"], sm_prev["bounce"])),
        delta_color="inverse",
    )

    c1, c2 = st.columns(2)
    with c1:
        section_title("Canales / Adquisicion")
        if ch_df.empty:
            st.info("No hay datos de canales en JSON.")
        else:
            ch = ch_df[(ch_df["date"] >= s) & (ch_df["date"] <= e)].copy()
            if ch.empty:
                st.info("Sin datos para el rango seleccionado.")
            else:
                b = ch.groupby("sessionDefaultChannelGroup", as_index=False).agg(sessions=("sessions", "sum"), conversions=("conversions", "sum")).sort_values("sessions", ascending=False).head(10)
                fig = go.Figure()
                fig.add_trace(go.Bar(x=b["sessionDefaultChannelGroup"], y=b["sessions"], name="Sessions", marker={"color": C_ACCENT}))
                fig.add_trace(go.Scatter(x=b["sessionDefaultChannelGroup"], y=b["conversions"], name="Conversions", mode="lines+markers", line={"color": C_META, "width": 2}, yaxis="y2"))
                pbi_layout(fig, yaxis_title="Sessions", xaxis_title="Canal", y2_title="Conversions")
                st.plotly_chart(fig, use_container_width=True)

    with c2:
        section_title("Paginas Mas Visitadas")
        if pg_df.empty:
            st.info("No hay datos de paginas en JSON.")
        else:
            pg = pg_df[(pg_df["date"] >= s) & (pg_df["date"] <= e)].copy()
            if pg.empty:
                st.info("Sin datos para el rango seleccionado.")
            else:
                top_p = pg.groupby(["pagePath", "pageTitle"], as_index=False).agg(views=("screenPageViews", "sum"), sessions=("sessions", "sum"), avg_session=("averageSessionDuration", "mean")).sort_values("views", ascending=False).head(10)
                st.dataframe(top_p, use_container_width=True, hide_index=True)

    section_title("Rendimiento de Campanas (Paid Media)")
    if camp_df.empty:
        st.info("No hay datos de campanas en JSON.")
    else:
        cp = camp_df[(camp_df["date"] >= s) & (camp_df["date"] <= e)].copy()
        if platform in ("Google", "Meta"):
            cp = cp[cp["platform"] == platform]
        required_defaults: dict[str, Any] = {
            "platform": "",
            "campaign_id": "",
            "campaign_name": "",
            "spend": 0.0,
            "impressions": 0.0,
            "clicks": 0.0,
            "conversions": 0.0,
            "ctr": 0.0,
            "cpc": 0.0,
            "reach": 0.0,
            "frequency": 0.0,
        }
        for col, default in required_defaults.items():
            if col not in cp.columns:
                cp[col] = default
        if cp.empty:
            st.info("Sin datos de campanas para filtros actuales.")
        else:
            roll = cp.groupby(["platform", "campaign_id", "campaign_name"], as_index=False).agg(spend=("spend", "sum"), impressions=("impressions", "sum"), clicks=("clicks", "sum"), conversions=("conversions", "sum"), ctr=("ctr", "mean"), cpc=("cpc", "mean"), reach=("reach", "max"), frequency=("frequency", "mean")).sort_values("spend", ascending=False)
            roll["cpl"] = roll.apply(lambda r: sdiv(float(r["spend"]), float(r["conversions"])), axis=1)
            st.dataframe(roll.head(20), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(
        page_title="YAP Executive Marketing Command Center",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    apply_theme()

    report = load_report(REPORT_PATH)
    df = daily_df(report)
    if df.empty:
        st.warning("No hay datos diarios en el JSON.")
        st.stop()

    ch = acq_df(report, "ga4_channel_daily")
    ga4_event_daily = acq_df(report, "ga4_event_daily")
    pg = acq_df(report, "ga4_top_pages_daily")
    paid_dev = paid_device_df(report)
    camp = acq_df(report, "meta_campaign_daily")
    if not camp.empty:
        camp["platform"] = "Meta"
    gcamp = acq_df(report, "google_campaign_daily")
    if not gcamp.empty:
        gcamp["platform"] = "Google"
        if "cost" in gcamp.columns:
            gcamp["spend"] = pd.to_numeric(gcamp["cost"], errors="coerce").fillna(0.0)
    if camp.empty and gcamp.empty:
        camp_all = pd.DataFrame()
    elif camp.empty:
        camp_all = gcamp
    elif gcamp.empty:
        camp_all = camp
    else:
        camp_all = pd.concat([camp, gcamp], ignore_index=True)

    min_d, max_d = df["date"].min(), df["date"].max()
    render_sidebar(report)
    s, e, platform = render_top_filters(min_d, max_d)

    df_sel = df[(df["date"] >= s) & (df["date"] <= e)].copy()
    period_days = max((e - s).days + 1, 1)
    prev_e = s - timedelta(days=1)
    prev_s = prev_e - timedelta(days=period_days - 1)
    df_prev = df[(df["date"] >= prev_s) & (df["date"] <= prev_e)].copy()

    render_hero(s, e, platform)
    render_daily_fact(df_sel, platform)

    t1, t2 = st.tabs(["EXECUTIVE OVERVIEW", "TRAFICO Y ADQUISICION"])
    with t1:
        render_exec(
            df_sel,
            df_prev,
            platform,
            paid_dev,
            camp_all,
            ga4_event_daily,
            s,
            e,
            prev_s,
            prev_e,
        )
    with t2:
        render_traffic(df_sel, df_prev, ch, pg, camp_all, platform, s, e)

    st.caption(f"Fuente: {REPORT_PATH.name} | Datos: {min_d.isoformat()} a {max_d.isoformat()}")


if __name__ == "__main__":
    main()
