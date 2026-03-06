
#!/usr/bin/env python3
from __future__ import annotations

import json
import html
import hashlib
import math
import secrets
import shutil
import textwrap
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
REPORT_PATH = BASE_DIR / "reports" / "yap" / "yap_historical.json"
TENANTS_CONFIG_PATH = BASE_DIR / "config" / "tenants.json"
USERS_CONFIG_PATH = BASE_DIR / "config" / "users.json"
DASHBOARD_SETTINGS_PATH = BASE_DIR / "config" / "dashboard_settings.json"
CONFIG_BACKUP_DIR = BASE_DIR / "config" / "backups"
ADMIN_AUDIT_LOG_PATH = BASE_DIR / "config" / "admin_audit.jsonl"
DEFAULT_TENANT_ID = "yap"
LOGO_PATH = BASE_DIR / "assets" / "logo-ipalmera-growth-marketing.webp"
LOGO_PLACEHOLDER = "https://via.placeholder.com/260x80/F8FAFC/0F172A?text=iPalmera+Logo"
META_ACCOUNT_ID = "1808641036591815"
GOOGLE_CUSTOMER_ID = "6495122409"
GA4_GTC_SOLICITAR_CODIGO_EVENT = "form_gtc_otp_solicitar_codigo"

C_GOOGLE = "#7BCC35"
C_META = "#FE492A"
C_ACCENT = "#7BCC35"
C_TEXT = "#201D1D"
C_MUTE = "#7A879D"
C_PANEL_BORDER = "rgba(32,29,29,0.08)"
C_PANEL_BG = "rgba(255,255,255,0.72)"
C_GRID = "rgba(32,29,29,0.07)"

VIEW_MODE_OPTIONS = ("Overview", "Tráfico y Adquisición")
PLATFORM_OPTIONS = ("All", "Google", "Meta")
DEFAULT_OVERVIEW_KPI_KEYS = ["spend", "conv", "cpl", "ctr"]
DEFAULT_TRAFFIC_KPI_KEYS = ["sessions", "users", "avg_sess", "bounce"]
DEFAULT_OVERVIEW_SECTION_KEYS = [
    "kpis",
    "trend_chart",
    "funnel",
    "ga4_conversion",
    "device_breakdown",
    "audit_table",
    "top_pieces",
    "daily_fact",
]
DEFAULT_TRAFFIC_SECTION_KEYS = ["kpis", "channels", "top_pages", "campaigns"]

OVERVIEW_SECTION_OPTIONS: dict[str, str] = {
    "kpis": "Tarjetas KPI",
    "trend_chart": "Gráfica Performance",
    "funnel": "Embudo de Conversión",
    "ga4_conversion": "Tarjeta Conversiones GA4",
    "device_breakdown": "Dispositivos de Pauta",
    "audit_table": "Tabla Maestra de Auditoría",
    "top_pieces": "Top 10 Piezas",
    "daily_fact": "Insight Diario",
}
TRAFFIC_SECTION_OPTIONS: dict[str, str] = {
    "kpis": "Tarjetas KPI",
    "channels": "Canales / Adquisición",
    "top_pages": "Páginas Más Visitadas",
    "campaigns": "Rendimiento de Campañas",
}


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
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except Exception:
        return "N/A"
    if not math.isfinite(n):
        return "N/A"
    return f"${n:,.2f}"


def fmt_pct(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except Exception:
        return "N/A"
    if not math.isfinite(n):
        return "N/A"
    return f"{n*100:.2f}%"


def fmt_delta(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except Exception:
        return "N/A"
    if not math.isfinite(n):
        return "N/A"
    return f"{n:+.1f}% vs periodo anterior"


def fmt_delta_compact(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except Exception:
        return "N/A"
    if not math.isfinite(n):
        return "N/A"
    return f"{n:+.1f}%"


def fmt_compact(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        n = float(v)
    except Exception:
        return "N/A"
    if not math.isfinite(n):
        return "N/A"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.1f}k"
    return f"{n:,.0f}"


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    try:
        sec = float(seconds)
    except Exception:
        return "N/A"
    if not math.isfinite(sec):
        return "N/A"
    sec = max(sec, 0.0)
    if sec < 60:
        return f"{sec:.0f} s"
    mins = int(sec // 60)
    rem = int(round(sec % 60))
    if mins < 60:
        return f"{mins} min {rem}s"
    hrs = mins // 60
    mins_rem = mins % 60
    return f"{hrs} h {mins_rem} min"


KPI_CATALOG: dict[str, dict[str, str]] = {
    "spend": {"label": "Gasto Total", "fmt": "money", "delta_mode": "daily", "delta_color": "normal"},
    "conv": {"label": "Conversiones", "fmt": "int", "delta_mode": "daily", "delta_color": "normal"},
    "cpl": {"label": "CPL Promedio", "fmt": "money", "delta_mode": "direct", "delta_color": "inverse"},
    "ctr": {"label": "CTR", "fmt": "pct", "delta_mode": "direct", "delta_color": "normal"},
    "clicks": {"label": "Clics", "fmt": "int", "delta_mode": "direct", "delta_color": "normal"},
    "impr": {"label": "Impresiones", "fmt": "compact", "delta_mode": "direct", "delta_color": "normal"},
    "sessions": {"label": "Sesiones", "fmt": "int", "delta_mode": "direct", "delta_color": "normal"},
    "users": {"label": "Usuarios", "fmt": "int", "delta_mode": "direct", "delta_color": "normal"},
    "avg_sess": {
        "label": "Tiempo Promedio de Interacción",
        "fmt": "duration",
        "delta_mode": "direct",
        "delta_color": "normal",
    },
    "bounce": {"label": "Tasa de Rebote", "fmt": "pct", "delta_mode": "direct", "delta_color": "inverse"},
}


def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "").strip() if ch.isdigit())


def campaign_platform_link(
    platform: str,
    campaign_id: Any,
    *,
    meta_account_id: str = "",
    google_customer_id: str = "",
) -> str:
    camp_id = str(campaign_id or "").strip()
    if not camp_id:
        return ""
    plat = str(platform or "").strip().lower()
    if plat == "meta":
        act = str(meta_account_id or "").strip()
        if act and not act.startswith("act_"):
            act = f"act_{act}"
        if act:
            return (
                "https://adsmanager.facebook.com/adsmanager/manage/campaigns"
                f"?act={act}&selected_campaign_ids={camp_id}"
            )
        return (
            "https://adsmanager.facebook.com/adsmanager/manage/campaigns"
            f"?selected_campaign_ids={camp_id}"
        )
    if plat == "google":
        ocid = _digits_only(google_customer_id)
        if ocid:
            return f"https://ads.google.com/aw/campaigns?ocid={ocid}&campaignId={camp_id}"
        return f"https://ads.google.com/aw/campaigns?campaignId={camp_id}"
    return ""


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
          .desktop-powered-footer {
            margin-top: 1.15rem;
            text-align: center;
            font-size: 0.78rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #667085;
            font-weight: 700;
          }
          .desktop-powered-footer a {
            color: #0A84FF;
            text-decoration: none;
            font-weight: 800;
          }
          .desktop-powered-footer a:hover {
            text-decoration: underline;
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
            .desktop-powered-footer {
              display: none;
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
          /* 2026 redesign override */
          .stApp {
            background:
              radial-gradient(circle at 0% 0%, rgba(255,255,255,0.72) 0%, rgba(255,255,255,0) 48%),
              radial-gradient(circle at 100% 100%, rgba(122,135,157,0.11) 0%, rgba(122,135,157,0) 40%),
              #f5f5f7 !important;
            color: #201D1D !important;
          }
          .block-container {
            max-width: 92rem !important;
          }
          [data-testid="stSidebar"] {
            width: 262px !important;
            background: rgba(245,245,247,0.86) !important;
            border-right: 1px solid rgba(32,29,29,0.08) !important;
            backdrop-filter: blur(24px);
          }
          [data-testid="stSidebarContent"] {
            padding: 1rem 0.8rem 0.85rem 0.8rem;
          }
          [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
          [data-testid="stSidebar"] label {
            color: #201D1D !important;
          }
          .sidebar-profile-card {
            border: 1px solid rgba(32, 29, 29, 0.08);
            border-radius: 14px;
            padding: 0.8rem 0.75rem;
            background: rgba(255, 255, 255, 0.62);
            margin-bottom: 1rem;
          }
          .sidebar-profile-row {
            display: flex;
            align-items: center;
            gap: 0.65rem;
          }
          .sidebar-avatar {
            width: 2.45rem;
            height: 2.45rem;
            border-radius: 999px;
            border: 1px solid rgba(123, 204, 53, 0.35);
            background: rgba(123, 204, 53, 0.16);
            color: #67b22d;
            font-size: 1rem;
            font-weight: 800;
            display: flex;
            align-items: center;
            justify-content: center;
          }
          .sidebar-profile-name {
            color: #201D1D;
            font-size: 1.1rem;
            font-weight: 700;
            line-height: 1.2;
          }
          .sidebar-profile-meta {
            color: #4d627f;
            font-size: 0.78rem;
            font-weight: 500;
            margin-top: 0.16rem;
          }
          .sidebar-kicker {
            margin: 0.12rem 0 0.28rem 0.24rem;
            font-size: 0.66rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-weight: 800;
            color: #7a879d;
          }
          .sidebar-nav { margin: 0.6rem 0 0.6rem 0; display: grid; gap: 0.3rem; }
          .sidebar-nav-item {
            padding: 0.6rem 0.65rem;
            border-radius: 12px;
            color: #4f617b;
            font-size: 0.95rem;
            font-weight: 600;
            border: 1px solid transparent;
          }
          .sidebar-nav-item.active {
            color: #201D1D;
            background: rgba(32,29,29,0.06);
            border-color: rgba(32,29,29,0.08);
            font-weight: 700;
          }
          .sidebar-bottom {
            margin-top: auto;
            padding-top: 0.7rem;
            border-top: 1px solid rgba(32,29,29,0.08);
          }
          .sidebar-token-card {
            margin-top: 0.62rem;
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 12px;
            background: rgba(255,255,255,0.7);
            padding: 0.65rem 0.72rem;
            text-align: center;
          }
          .sidebar-token-wrap {
            position: sticky;
            bottom: 0.45rem;
            margin-top: 0.85rem;
            z-index: 5;
          }
          .sidebar-token-title {
            font-size: 0.9rem;
            font-weight: 900;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            color: #201D1D;
            margin-bottom: 0.48rem;
          }
          .sidebar-token-row {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 0.45rem;
            margin-bottom: 0.46rem;
          }
          .sidebar-token-days-label {
            font-size: 0.67rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #7a879d;
          }
          .sidebar-token-days {
            font-size: 1.85rem;
            line-height: 1;
            font-weight: 900;
            letter-spacing: -0.01em;
          }
          .sidebar-token-days.good { color: #5ea42a; }
          .sidebar-token-days.warn { color: #b86a00; }
          .sidebar-token-days.bad { color: #c62828; }
          .sidebar-token-days.na { color: #7a879d; }
          .sidebar-token-badge {
            border-radius: 999px;
            padding: 0.14rem 0.58rem;
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.01em;
            border: 1px solid transparent;
          }
          .sidebar-token-badge.good {
            color: #5ea42a;
            background: rgba(123,204,53,0.14);
            border-color: rgba(123,204,53,0.34);
          }
          .sidebar-token-badge.warn {
            color: #b86a00;
            background: rgba(254,197,61,0.16);
            border-color: rgba(254,197,61,0.38);
          }
          .sidebar-token-badge.bad {
            color: #c62828;
            background: rgba(254,73,42,0.15);
            border-color: rgba(254,73,42,0.35);
          }
          .sidebar-token-item {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 0.45rem;
            font-size: 0.72rem;
            margin-top: 0.24rem;
          }
          .sidebar-token-item .k {
            color: #7a879d;
            font-weight: 700;
          }
          .sidebar-token-item .v {
            color: #334761;
            font-weight: 700;
            text-align: right;
            max-width: 65%;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
          }
          [data-testid="stSidebar"] .stSelectbox label {
            margin-left: 0.2rem;
            color: #7a879d !important;
            font-size: 0.67rem !important;
            letter-spacing: 0.1em !important;
            font-weight: 800 !important;
            text-transform: uppercase;
          }
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
            border-radius: 12px;
            background: rgba(255,255,255,0.75) !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            min-height: 2.85rem !important;
          }
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div > div,
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div > div * {
            color: #201D1D !important;
            opacity: 1 !important;
          }
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] * {
            color: #201D1D !important;
            opacity: 1 !important;
          }
          [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] svg {
            fill: #7A879D !important;
          }
          [data-baseweb="popover"] [role="listbox"] * {
            color: #201D1D !important;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] {
            display: grid;
            gap: 0.28rem;
            margin-top: 0.2rem;
            margin-bottom: 0.45rem;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label {
            border-radius: 12px;
            border: 1px solid transparent;
            padding: 0.58rem 0.62rem;
            color: #4f617b !important;
            font-size: 0.95rem;
            font-weight: 600;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"] {
            background: rgba(32,29,29,0.06);
            color: #201D1D !important;
            border-color: rgba(32,29,29,0.08);
            font-weight: 700;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label p {
            margin: 0 !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button {
            border-radius: 12px !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            background: rgba(255,255,255,0.65) !important;
            color: #4f617b !important;
            font-weight: 600 !important;
            min-height: 2.6rem !important;
            justify-content: flex-start !important;
            padding-left: 0.55rem !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button[kind="primary"] {
            background: rgba(32,29,29,0.06) !important;
            color: #201D1D !important;
            border-color: rgba(32,29,29,0.10) !important;
            font-weight: 700 !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button:hover {
            border-color: rgba(32,29,29,0.16) !important;
          }
          .hero {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
            margin-bottom: 0.65rem;
          }
          .hero-kicker {
            color: #201D1D !important;
            font-size: 3rem !important;
            line-height: 1.05 !important;
            font-weight: 800 !important;
            letter-spacing: -0.03em !important;
            margin: 0 !important;
            white-space: nowrap;
          }
          .hero-title { display: none; }
          .hero-sub {
            color: #4d627f !important;
            font-size: 0.95rem !important;
            font-weight: 500;
            margin-top: 0.2rem;
          }
          .top-controls-hint {
            margin-top: 0.3rem;
            font-size: 0.72rem;
            color: #7a879d;
            text-align: right;
          }
          [data-testid="stMain"] .stRadio > label p {
            color: transparent !important;
            font-size: 0 !important;
            line-height: 0 !important;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] {
            display: flex;
            align-items: center;
            background: rgba(32,29,29,0.05);
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 999px;
            padding: 0.32rem;
            min-height: 3.1rem;
            gap: 0.28rem;
            width: 100% !important;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label {
            margin: 0 !important;
            padding: 0.62rem 1.18rem !important;
            border-radius: 999px;
            border: 1px solid transparent;
            min-height: 2.55rem;
            cursor: pointer !important;
            color: #4d627f !important;
            font-weight: 700 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            flex: 1 1 0 !important;
            width: 100% !important;
            min-width: 6.2rem !important;
            transition: background 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, color 0.18s ease;
            overflow: hidden !important;
            touch-action: manipulation;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label > div:first-child {
            position: absolute !important;
            opacity: 0 !important;
            width: 1px !important;
            height: 1px !important;
            pointer-events: none !important;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label [data-testid="stMarkdownContainer"] {
            width: 100% !important;
            text-align: center !important;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label [data-testid="stMarkdownContainer"] p {
            margin: 0 !important;
            color: #4d627f !important;
            font-weight: 700 !important;
            text-align: center !important;
            white-space: nowrap !important;
            word-break: keep-all !important;
            overflow-wrap: normal !important;
            font-size: 1.03rem !important;
            line-height: 1 !important;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label:hover {
            background: rgba(255,255,255,0.72) !important;
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[data-checked="true"],
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[aria-checked="true"],
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label:has(input:checked) {
            background: linear-gradient(180deg, rgba(123, 204, 53, 0.30) 0%, rgba(123, 204, 53, 0.24) 100%) !important;
            border-color: rgba(103, 178, 45, 0.72) !important;
            box-shadow: 0 4px 12px rgba(103, 178, 45, 0.28);
          }
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[data-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[aria-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label:has(input:checked) [data-testid="stMarkdownContainer"] p {
            color: #1F4D0A !important;
            font-weight: 800 !important;
          }
          [data-testid="stSegmentedControl"] [data-baseweb="button-group"] {
            width: 100%;
            background: rgba(32,29,29,0.05) !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            border-radius: 999px !important;
            padding: 0.32rem !important;
            gap: 0.28rem !important;
            display: flex !important;
            flex-wrap: nowrap !important;
            overflow: hidden !important;
            min-height: 3.1rem !important;
          }
          [data-testid="stSegmentedControl"] [data-baseweb="button"] {
            border-radius: 999px !important;
            border: 1px solid transparent !important;
            color: #4d627f !important;
            font-weight: 700 !important;
            min-height: 2.55rem !important;
            min-width: 6.2rem !important;
            padding: 0.62rem 1.18rem !important;
            white-space: nowrap !important;
            flex: 1 1 0 !important;
            justify-content: center !important;
            cursor: pointer !important;
            transition: background 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease, color 0.18s ease;
          }
          [data-testid="stSegmentedControl"] [data-baseweb="button"]:hover {
            background: rgba(255,255,255,0.74) !important;
          }
          [data-testid="stSegmentedControl"] [data-baseweb="button"][aria-pressed="true"],
          [data-testid="stSegmentedControl"] [data-baseweb="button"][aria-selected="true"],
          [data-testid="stSegmentedControl"] [data-baseweb="button"][data-active="true"] {
            background: linear-gradient(180deg, rgba(123, 204, 53, 0.30) 0%, rgba(123, 204, 53, 0.24) 100%) !important;
            color: #1F4D0A !important;
            border-color: rgba(103, 178, 45, 0.72) !important;
            box-shadow: 0 4px 12px rgba(103, 178, 45, 0.28);
            font-weight: 800 !important;
          }
          [data-testid="stMain"] .stDateInput > div > div,
          [data-testid="stMain"] .stSelectbox > div > div {
            background: rgba(255,255,255,0.88) !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
          }
          [data-testid="stMain"] .stDateInput,
          [data-testid="stMain"] .stDateInput > div {
            width: 100% !important;
          }
          [data-testid="stMain"] .stDateInput [data-baseweb="input"] {
            min-height: 2.35rem !important;
            border-radius: 14px !important;
            cursor: pointer !important;
          }
          [data-testid="stMain"] .stDateInput [data-baseweb="input"] * {
            cursor: pointer !important;
          }
          div[data-testid="stMetric"] {
            background: rgba(255,255,255,0.72) !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            border-radius: 18px !important;
            padding: 1rem !important;
            box-shadow: 0 12px 28px rgba(15,23,42,0.04);
          }
          div[data-testid="stMetricLabel"] {
            color: #4d627f !important;
            letter-spacing: 0.08em;
            font-size: 0.68rem !important;
            font-weight: 800;
          }
          div[data-testid="stMetricValue"] {
            color: #201D1D !important;
            font-size: 2rem !important;
            font-weight: 800;
          }
          .daily-fact {
            background: rgba(255,255,255,0.72) !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            border-radius: 18px !important;
            padding: 1rem 1.15rem !important;
            margin-bottom: 0.9rem !important;
            display: flex !important;
            gap: 0.9rem !important;
            align-items: center !important;
            box-shadow: 0 12px 28px rgba(15, 23, 42, 0.04) !important;
          }
          .daily-fact-icon {
            width: 2.85rem;
            height: 2.85rem;
            border-radius: 14px;
            border: 1px solid rgba(123, 204, 53, 0.28);
            background: rgba(123, 204, 53, 0.12);
            color: #67b22d;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.35rem;
          }
          .viz-card, .funnel-card, .top-pieces-card {
            background: rgba(255,255,255,0.72);
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 18px;
            box-shadow: 0 12px 30px rgba(15,23,42,0.04);
          }
          .viz-card { padding: 1rem 1rem 0.8rem 1rem; }
          .viz-title { font-size: 1.05rem; font-weight: 800; color: #201D1D; margin: 0; }
          .viz-sub { color: #4d627f; font-size: 0.9rem; margin-top: 0.15rem; }
          .funnel-card { padding: 1rem; height: 100%; }
          .funnel-title { font-size: 1.05rem; font-weight: 800; color: #201D1D; margin-bottom: 0.8rem; }
          .funnel-stack { display: grid; gap: 0.72rem; }
          .funnel-row {
            position: relative;
            height: 2.85rem;
            border-radius: 13px;
            background: rgba(32,29,29,0.06);
            overflow: hidden;
          }
          .funnel-fill {
            position: absolute;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            height: 2.12rem;
            border-radius: 12px;
            clip-path: polygon(6% 0, 94% 0, 100% 100%, 0% 100%);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.45);
            transition: width 220ms ease;
          }
          .funnel-content { position: relative; z-index: 2; height: 100%; padding: 0 0.75rem; display: flex; align-items: center; justify-content: space-between; gap: 0.6rem; }
          .funnel-name { font-size: 0.88rem; color: #35485f; font-weight: 700; }
          .funnel-value { font-size: 1rem; color: #201D1D; font-weight: 800; }
          .funnel-metrics { display: inline-flex; align-items: center; gap: 0.4rem; }
          .funnel-drop {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 4.25rem;
            border-radius: 999px;
            padding: 0.12rem 0.42rem;
            font-size: 0.68rem;
            font-weight: 800;
            letter-spacing: 0.01em;
            color: #b3261e;
            background: rgba(254,73,42,0.12);
            border: 1px solid rgba(254,73,42,0.28);
          }
          .funnel-drop-base {
            color: #55657b;
            background: rgba(122,135,157,0.14);
            border: 1px solid rgba(122,135,157,0.28);
          }
          .ga4-conv-card {
            margin-top: 0.72rem;
            background: rgba(255,255,255,0.72);
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 16px;
            padding: 0.78rem 0.85rem;
            box-shadow: 0 10px 24px rgba(15,23,42,0.04);
          }
          .ga4-conv-title {
            font-size: 0.94rem;
            font-weight: 800;
            color: #201D1D;
            margin-bottom: 0.3rem;
          }
          .ga4-conv-event {
            font-size: 0.75rem;
            color: #55657b;
            margin-bottom: 0.56rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
          }
          .ga4-conv-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.48rem;
          }
          .ga4-conv-item {
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 11px;
            background: rgba(32,29,29,0.02);
            padding: 0.42rem 0.52rem;
          }
          .ga4-conv-label {
            display: block;
            font-size: 0.67rem;
            color: #7A879D;
            font-weight: 700;
            margin-bottom: 0.16rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
          }
          .ga4-conv-value {
            display: block;
            font-size: 0.93rem;
            color: #201D1D;
            font-weight: 800;
            line-height: 1.1;
          }
          .top-pieces-card { margin-top: 1rem; overflow: hidden; }
          .top-pieces-head { padding: 1rem 1.2rem; border-bottom: 1px solid rgba(32,29,29,0.08); display: flex; align-items: center; justify-content: space-between; }
          .top-pieces-title { margin: 0; font-size: 2rem; font-weight: 800; letter-spacing: -0.02em; color: #201D1D; }
          .top-pieces-filter { width: 2.35rem; height: 2.35rem; border-radius: 999px; border: 1px solid rgba(32,29,29,0.1); display: inline-flex; align-items: center; justify-content: center; color: #4d627f; font-size: 1rem; }
          .top-pieces-table { width: 100%; border-collapse: collapse; }
          .top-pieces-table thead th { text-align: left; font-size: 0.66rem; letter-spacing: 0.1em; text-transform: uppercase; color: #7a879d; font-weight: 800; padding: 0.9rem 1.2rem; border-bottom: 1px solid rgba(32,29,29,0.06); background: rgba(32,29,29,0.02); }
          .top-pieces-table tbody td { padding: 0.85rem 1.2rem; border-bottom: 1px solid rgba(32,29,29,0.06); color: #334761; font-size: 0.95rem; }
          .top-pieces-table tbody td.col-link { width: 5.4rem; }
          .top-pieces-table tbody td.col-name { color: #201D1D; font-weight: 700; }
          .top-pieces-table tbody td.col-conv { font-weight: 700; color: #1f3b5f; }
          .piece-link {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 3.5rem;
            padding: 0.25rem 0.52rem;
            border-radius: 999px;
            border: 1px solid rgba(32,29,29,0.14);
            color: #334761;
            text-decoration: none;
            font-size: 0.74rem;
            font-weight: 700;
            background: rgba(255,255,255,0.72);
          }
          .piece-link:hover { border-color: rgba(123,204,53,0.5); color: #201D1D; }
          .piece-link-off {
            color: #7A879D;
            font-size: 0.8rem;
            font-weight: 700;
          }
          .pill { display: inline-flex; align-items: center; border-radius: 999px; padding: 0.2rem 0.58rem; font-size: 0.66rem; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase; }
          .pill-meta { background: rgba(254,73,42,0.12); border: 1px solid rgba(254,73,42,0.28); color: #d93f24; }
          .pill-google { background: rgba(123,204,53,0.12); border: 1px solid rgba(123,204,53,0.28); color: #67b22d; }
          .roas-good { color: #67b22d !important; font-weight: 800; }
          .roas-mid { color: #7a879d !important; font-weight: 800; }
          .top-pieces-footer { text-align: center; padding: 1rem; color: #7bcc35; font-size: 0.86rem; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase; }
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
        """
        <div class='hero'>
          <div class='hero-kicker'>Executive Marketing<br/>Command Center</div>
          <div class='hero-sub'>YAP Marketing Performance</div>
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

    if (d_conv or 0) >= 0:
        conv_pct = abs(d_conv or 0.0)
        body = (
            f"Yesterday's conversion rate increased by <b>{conv_pct:.1f}%</b> "
            "due to optimized Meta bidding strategies across major retail campaigns."
        )
    else:
        conv_pct = abs(d_conv or 0.0)
        body = (
            f"Yesterday's conversion rate decreased by <b>{conv_pct:.1f}%</b>. "
            "Review audience segmentation and creative mix to recover performance."
        )

    st.markdown(
        f"""
        <div class="daily-fact">
          <div class="daily-fact-icon">✦</div>
          <div>
            <div class="title">Daily Fact: AI Performance Insight</div>
            <div class="body">{body}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def load_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"No se encontro: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def default_tenants_config() -> dict[str, dict[str, Any]]:
    return {
        DEFAULT_TENANT_ID: {
            "id": DEFAULT_TENANT_ID,
            "name": "YAP",
            "report_path": str(REPORT_PATH),
            "meta_account_id": META_ACCOUNT_ID,
            "google_customer_id": GOOGLE_CUSTOMER_ID,
            "ga4_conversion_event_name": GA4_GTC_SOLICITAR_CODIGO_EVENT,
        }
    }


def _resolve_repo_path(raw_path: str) -> Path:
    p = Path(str(raw_path).strip())
    return p if p.is_absolute() else (BASE_DIR / p).resolve()


def load_tenants_config(path: Path) -> dict[str, dict[str, Any]]:
    tenants = default_tenants_config()
    if not path.exists():
        return tenants

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return tenants

    entries = payload.get("tenants", [])
    if not isinstance(entries, list):
        return tenants

    loaded: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        tenant_id = str(entry.get("id", "")).strip().lower()
        if not tenant_id:
            continue
        report_raw = str(entry.get("report_path", "")).strip()
        if not report_raw:
            continue
        loaded[tenant_id] = {
            "id": tenant_id,
            "name": str(entry.get("name", tenant_id.upper())).strip() or tenant_id.upper(),
            "report_path": str(_resolve_repo_path(report_raw)),
            "meta_account_id": str(entry.get("meta_account_id", entry.get("meta_ad_account_id", META_ACCOUNT_ID))),
            "google_customer_id": str(
                entry.get("google_customer_id", entry.get("google_ads_customer_id", GOOGLE_CUSTOMER_ID))
            ),
            "ga4_conversion_event_name": str(
                entry.get("ga4_conversion_event_name", GA4_GTC_SOLICITAR_CODIGO_EVENT)
            ).strip() or GA4_GTC_SOLICITAR_CODIGO_EVENT,
        }

    return loaded if loaded else tenants


def _normalize_allowed_tenants(raw_allowed: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw_allowed, list):
        values = [str(v).strip().lower() for v in raw_allowed if str(v).strip()]
    elif isinstance(raw_allowed, str):
        val = raw_allowed.strip().lower()
        values = [val] if val else []
    if not values:
        return ["*"]
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        tenant_id = "*" if value == "*" else value
        if tenant_id in seen:
            continue
        seen.add(tenant_id)
        normalized.append(tenant_id)
    return normalized


def _normalize_tenant_scopes(
    raw_scopes: Any,
    fallback_allowed_tenants: list[str],
    fallback_role: str,
) -> list[dict[str, str]]:
    scopes: list[dict[str, str]] = []
    if isinstance(raw_scopes, list):
        for raw_scope in raw_scopes:
            tenant_id = ""
            tenant_role = fallback_role
            if isinstance(raw_scope, dict):
                tenant_id = str(raw_scope.get("tenant_id", raw_scope.get("id", ""))).strip().lower()
                tenant_role = str(raw_scope.get("role", raw_scope.get("tenant_role", fallback_role))).strip().lower()
            else:
                tenant_id = str(raw_scope).strip().lower()
            if not tenant_id:
                continue
            scopes.append(
                {
                    "tenant_id": tenant_id,
                    "role": tenant_role or fallback_role,
                }
            )
    if not scopes:
        scopes = [{"tenant_id": t, "role": fallback_role} for t in fallback_allowed_tenants]
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for scope in scopes:
        tenant_id = str(scope.get("tenant_id", "")).strip().lower()
        if not tenant_id or tenant_id in seen:
            continue
        seen.add(tenant_id)
        normalized.append(
            {
                "tenant_id": tenant_id,
                "role": str(scope.get("role", fallback_role)).strip().lower() or fallback_role,
            }
        )
    return normalized


def _auth_user_is_admin(auth_user: Any) -> bool:
    if not isinstance(auth_user, dict):
        return False
    role = str(auth_user.get("role", "")).strip().lower()
    if role == "admin":
        return True
    global_role = str(auth_user.get("global_role", "")).strip().lower()
    return global_role in {"admin", "owner", "superadmin", "sysadmin"}


def _auth_user_tenant_ids(auth_user: Any, tenants: dict[str, dict[str, Any]]) -> list[str]:
    tenant_keys = list(tenants.keys())
    if not tenant_keys:
        return []
    if not isinstance(auth_user, dict):
        return tenant_keys

    scope_tenants: list[str] = []
    raw_scopes = auth_user.get("tenant_scopes", [])
    if isinstance(raw_scopes, list):
        for raw_scope in raw_scopes:
            if isinstance(raw_scope, dict):
                tenant_id = str(raw_scope.get("tenant_id", raw_scope.get("id", ""))).strip().lower()
            else:
                tenant_id = str(raw_scope).strip().lower()
            if tenant_id:
                scope_tenants.append(tenant_id)
    if scope_tenants:
        if "*" in scope_tenants:
            return tenant_keys
        return [tenant_id for tenant_id in tenant_keys if tenant_id in set(scope_tenants)]

    allowed_tenants = _normalize_allowed_tenants(auth_user.get("allowed_tenants", ["*"]))
    if "*" in allowed_tenants:
        return tenant_keys
    return [tenant_id for tenant_id in tenant_keys if tenant_id in set(allowed_tenants)]


def load_users_config(path: Path) -> dict[str, dict[str, Any]]:
    users: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return users
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return users
    entries = payload.get("users", [])
    if not isinstance(entries, list):
        return users
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        username = str(entry.get("username", "")).strip().lower()
        if not username:
            continue
        role = str(entry.get("role", "viewer")).strip().lower() or "viewer"
        allowed_tenants = _normalize_allowed_tenants(entry.get("allowed_tenants", ["*"]))
        global_role = str(entry.get("global_role", "")).strip().lower()
        if not global_role:
            global_role = "admin" if role == "admin" else "user"
        tenant_scopes = _normalize_tenant_scopes(entry.get("tenant_scopes"), allowed_tenants, role)
        users[username] = {
            "username": username,
            "name": str(entry.get("name", username)).strip() or username,
            "role": role,
            "global_role": global_role,
            "enabled": bool(entry.get("enabled", True)),
            "allowed_tenants": allowed_tenants,
            "tenant_scopes": tenant_scopes,
            "password_salt": str(entry.get("password_salt", "")),
            "password_hash": str(entry.get("password_hash", "")).lower(),
        }
    return users


def _normalize_tenant_selection(raw_selected: Any) -> list[str]:
    selected: list[str] = []
    if isinstance(raw_selected, list):
        selected = [str(v).strip().lower() for v in raw_selected if str(v).strip()]
    elif isinstance(raw_selected, str):
        val = raw_selected.strip().lower()
        selected = [val] if val else []
    normalized: list[str] = []
    seen: set[str] = set()
    for tenant_id in selected:
        key = "*" if tenant_id == "*" else tenant_id
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if "*" in normalized:
        return ["*"]
    return normalized


def _widget_safe_key(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(value))


def _scope_map_for_user(user: dict[str, Any]) -> dict[str, str]:
    role = str(user.get("role", "viewer")).strip().lower() or "viewer"
    scopes = _normalize_tenant_scopes(
        user.get("tenant_scopes"),
        _normalize_allowed_tenants(user.get("allowed_tenants", ["*"])),
        role,
    )
    return {
        str(scope.get("tenant_id", "")).strip().lower(): str(scope.get("role", role)).strip().lower() or role
        for scope in scopes
        if str(scope.get("tenant_id", "")).strip()
    }


def _build_tenant_access(
    selected_tenants: list[str],
    scope_roles: dict[str, str],
    fallback_role: str,
) -> tuple[list[str], list[dict[str, str]]]:
    allowed_tenants = _normalize_tenant_selection(selected_tenants)
    if "*" in allowed_tenants:
        tenant_scopes = [
            {
                "tenant_id": "*",
                "role": str(scope_roles.get("*", fallback_role)).strip().lower() or fallback_role,
            }
        ]
        return ["*"], tenant_scopes
    tenant_scopes = []
    for tenant_id in allowed_tenants:
        tenant_scopes.append(
            {
                "tenant_id": tenant_id,
                "role": str(scope_roles.get(tenant_id, fallback_role)).strip().lower() or fallback_role,
            }
        )
    return allowed_tenants, tenant_scopes


def _new_password_salt() -> str:
    return secrets.token_urlsafe(12)


def _hash_password_with_salt(password: str, salt: str) -> str:
    raw = f"{salt}{password}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest().lower()


def _user_record_is_admin(user: dict[str, Any]) -> bool:
    return _auth_user_is_admin(user)


def _enabled_admin_count(users: dict[str, dict[str, Any]]) -> int:
    return sum(
        1
        for user in users.values()
        if bool(user.get("enabled", True)) and _user_record_is_admin(user)
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_safe_json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _safe_json_value(v) for k, v in value.items()}
    return str(value)


def _backup_config_file(path: Path) -> tuple[bool, str]:
    try:
        if not path.exists():
            return True, ""
        CONFIG_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = CONFIG_BACKUP_DIR / f"{path.stem}_{stamp}.bak{path.suffix}"
        shutil.copy2(path, backup_path)
        return True, str(backup_path)
    except Exception as exc:
        return False, str(exc)


def append_admin_audit(
    action: str,
    actor: str,
    *,
    target: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    try:
        ADMIN_AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp_utc": _utc_now_iso(),
            "action": str(action).strip() or "unknown",
            "actor": str(actor).strip() or "unknown",
            "target": str(target).strip(),
            "details": _safe_json_value(details or {}),
        }
        with ADMIN_AUDIT_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception:
        pass


def read_admin_audit(limit: int = 200) -> list[dict[str, Any]]:
    if not ADMIN_AUDIT_LOG_PATH.exists():
        return []
    try:
        lines = ADMIN_AUDIT_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for raw in lines[-max(limit, 1):]:
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    rows.reverse()
    return rows


def validate_users_integrity(users: dict[str, dict[str, Any]], tenants: dict[str, dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    if not users:
        issues.append("No hay usuarios definidos en config/users.json.")
        return issues
    enabled_admins = _enabled_admin_count(users)
    if enabled_admins < 1:
        issues.append("No hay admins activos.")
    tenant_ids = set(tenants.keys())
    for username, user in users.items():
        uname = str(username).strip().lower()
        if not uname:
            issues.append("Existe un usuario con username vacío.")
            continue
        if bool(user.get("enabled", True)):
            if not str(user.get("password_hash", "")).strip() or not str(user.get("password_salt", "")).strip():
                issues.append(f"Usuario activo '{uname}' sin password_hash/password_salt.")
        allowed = _normalize_allowed_tenants(user.get("allowed_tenants", ["*"]))
        for tenant_id in allowed:
            if tenant_id != "*" and tenant_id not in tenant_ids:
                issues.append(f"Usuario '{uname}' apunta a tenant inexistente '{tenant_id}'.")
    return issues


def validate_dashboard_settings_integrity(settings: dict[str, Any], tenants: dict[str, dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    tenant_ids = set(tenants.keys())
    defaults = settings.get("defaults", {}) if isinstance(settings, dict) else {}
    if not isinstance(defaults, dict):
        issues.append("El bloque defaults en dashboard_settings es inválido.")
    tenants_cfg = settings.get("tenants", {}) if isinstance(settings, dict) else {}
    if not isinstance(tenants_cfg, dict):
        issues.append("El bloque tenants en dashboard_settings es inválido.")
        return issues
    for tenant_id in tenant_ids:
        if tenant_id not in tenants_cfg:
            issues.append(f"Falta configuración de dashboard para tenant '{tenant_id}'.")
    for tenant_id in tenants_cfg.keys():
        if tenant_id not in tenant_ids:
            issues.append(f"Existe configuración huérfana de dashboard para tenant '{tenant_id}'.")
    return issues


def _serialize_users_payload(users: dict[str, dict[str, Any]]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for username in sorted(users.keys()):
        user = users.get(username, {})
        uname = str(user.get("username", username)).strip().lower()
        if not uname:
            continue
        role = str(user.get("role", "viewer")).strip().lower() or "viewer"
        global_role = str(user.get("global_role", "")).strip().lower()
        if not global_role:
            global_role = "admin" if role == "admin" else "user"
        allowed_tenants = _normalize_allowed_tenants(user.get("allowed_tenants", ["*"]))
        tenant_scopes = _normalize_tenant_scopes(user.get("tenant_scopes"), allowed_tenants, role)
        entries.append(
            {
                "username": uname,
                "name": str(user.get("name", uname)).strip() or uname,
                "global_role": global_role,
                "role": role,
                "enabled": bool(user.get("enabled", True)),
                "allowed_tenants": allowed_tenants,
                "tenant_scopes": tenant_scopes,
                "password_salt": str(user.get("password_salt", "")),
                "password_hash": str(user.get("password_hash", "")).lower(),
            }
        )
    return {"users": entries}


def save_users_config(path: Path, users: dict[str, dict[str, Any]]) -> tuple[bool, str]:
    try:
        payload = _serialize_users_payload(users)
        path.parent.mkdir(parents=True, exist_ok=True)
        ok_backup, backup_info = _backup_config_file(path)
        if not ok_backup:
            return False, f"No se pudo crear backup: {backup_info}"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _normalize_platform_option(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    return value if value in PLATFORM_OPTIONS else "All"


def _normalize_view_mode_option(raw_value: Any) -> str:
    value = str(raw_value or "").strip()
    return value if value in VIEW_MODE_OPTIONS else "Overview"


def _coerce_bool(raw_value: Any, default: bool = False) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return default
    text = str(raw_value).strip().lower()
    if text in {"1", "true", "yes", "si", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_kpi_keys(raw_keys: Any, fallback_keys: list[str]) -> list[str]:
    allowed = set(KPI_CATALOG.keys())
    values: list[str] = []
    if isinstance(raw_keys, list):
        values = [str(v).strip().lower() for v in raw_keys if str(v).strip()]
    elif isinstance(raw_keys, str):
        values = [part.strip().lower() for part in raw_keys.split(",") if part.strip()]
    normalized: list[str] = []
    seen: set[str] = set()
    for key in values:
        if key not in allowed or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if not normalized:
        for key in fallback_keys:
            if key in allowed and key not in seen:
                seen.add(key)
                normalized.append(key)
    if not normalized:
        normalized = [k for k in DEFAULT_OVERVIEW_KPI_KEYS if k in allowed]
    return normalized[:6]


def _normalize_section_keys(raw_keys: Any, allowed_options: dict[str, str], fallback_keys: list[str]) -> list[str]:
    allowed = set(allowed_options.keys())
    values: list[str] = []
    if isinstance(raw_keys, list):
        values = [str(v).strip().lower() for v in raw_keys if str(v).strip()]
    elif isinstance(raw_keys, str):
        values = [part.strip().lower() for part in raw_keys.split(",") if part.strip()]
    normalized: list[str] = []
    seen: set[str] = set()
    for key in values:
        if key not in allowed or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if not normalized:
        for key in fallback_keys:
            if key in allowed and key not in seen:
                seen.add(key)
                normalized.append(key)
    if not normalized:
        normalized = list(allowed_options.keys())
    return normalized


def default_dashboard_settings(tenants: dict[str, dict[str, Any]]) -> dict[str, Any]:
    defaults = {
        "overview_kpis": list(DEFAULT_OVERVIEW_KPI_KEYS),
        "traffic_kpis": list(DEFAULT_TRAFFIC_KPI_KEYS),
        "overview_sections": list(DEFAULT_OVERVIEW_SECTION_KEYS),
        "traffic_sections": list(DEFAULT_TRAFFIC_SECTION_KEYS),
        "default_platform": "All",
        "default_view_mode": "Overview",
        "show_sidebar_meta_token_health": True,
    }
    tenant_cfg: dict[str, dict[str, Any]] = {}
    for tenant_id in tenants.keys():
        tenant_cfg[tenant_id] = {
            "overview_kpis": list(DEFAULT_OVERVIEW_KPI_KEYS),
            "traffic_kpis": list(DEFAULT_TRAFFIC_KPI_KEYS),
            "overview_sections": list(DEFAULT_OVERVIEW_SECTION_KEYS),
            "traffic_sections": list(DEFAULT_TRAFFIC_SECTION_KEYS),
            "default_platform": "All",
            "default_view_mode": "Overview",
            "show_sidebar_meta_token_health": True,
        }
    return {"defaults": defaults, "tenants": tenant_cfg}


def load_dashboard_settings(path: Path, tenants: dict[str, dict[str, Any]]) -> dict[str, Any]:
    base = default_dashboard_settings(tenants)
    if not path.exists():
        return base
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return base
    raw_defaults = payload.get("defaults", {}) if isinstance(payload, dict) else {}
    defaults = {
        "overview_kpis": _normalize_kpi_keys(
            raw_defaults.get("overview_kpis", base["defaults"]["overview_kpis"]),
            base["defaults"]["overview_kpis"],
        ),
        "traffic_kpis": _normalize_kpi_keys(
            raw_defaults.get("traffic_kpis", base["defaults"]["traffic_kpis"]),
            base["defaults"]["traffic_kpis"],
        ),
        "overview_sections": _normalize_section_keys(
            raw_defaults.get("overview_sections", base["defaults"]["overview_sections"]),
            OVERVIEW_SECTION_OPTIONS,
            base["defaults"]["overview_sections"],
        ),
        "traffic_sections": _normalize_section_keys(
            raw_defaults.get("traffic_sections", base["defaults"]["traffic_sections"]),
            TRAFFIC_SECTION_OPTIONS,
            base["defaults"]["traffic_sections"],
        ),
        "default_platform": _normalize_platform_option(
            raw_defaults.get("default_platform", base["defaults"]["default_platform"])
        ),
        "default_view_mode": _normalize_view_mode_option(
            raw_defaults.get("default_view_mode", base["defaults"]["default_view_mode"])
        ),
        "show_sidebar_meta_token_health": _coerce_bool(
            raw_defaults.get(
                "show_sidebar_meta_token_health",
                base["defaults"].get("show_sidebar_meta_token_health", True),
            ),
            default=True,
        ),
    }
    raw_tenants = payload.get("tenants", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_tenants, dict):
        raw_tenants = {}
    tenant_cfg: dict[str, dict[str, Any]] = {}
    for tenant_id in tenants.keys():
        raw_cfg = raw_tenants.get(tenant_id, {})
        if not isinstance(raw_cfg, dict):
            raw_cfg = {}
        tenant_cfg[tenant_id] = {
            "overview_kpis": _normalize_kpi_keys(
                raw_cfg.get("overview_kpis", defaults["overview_kpis"]),
                defaults["overview_kpis"],
            ),
            "traffic_kpis": _normalize_kpi_keys(
                raw_cfg.get("traffic_kpis", defaults["traffic_kpis"]),
                defaults["traffic_kpis"],
            ),
            "overview_sections": _normalize_section_keys(
                raw_cfg.get("overview_sections", defaults["overview_sections"]),
                OVERVIEW_SECTION_OPTIONS,
                defaults["overview_sections"],
            ),
            "traffic_sections": _normalize_section_keys(
                raw_cfg.get("traffic_sections", defaults["traffic_sections"]),
                TRAFFIC_SECTION_OPTIONS,
                defaults["traffic_sections"],
            ),
            "default_platform": _normalize_platform_option(
                raw_cfg.get("default_platform", defaults["default_platform"])
            ),
            "default_view_mode": _normalize_view_mode_option(
                raw_cfg.get("default_view_mode", defaults["default_view_mode"])
            ),
            "show_sidebar_meta_token_health": _coerce_bool(
                raw_cfg.get("show_sidebar_meta_token_health", defaults["show_sidebar_meta_token_health"]),
                default=defaults["show_sidebar_meta_token_health"],
            ),
        }
    return {"defaults": defaults, "tenants": tenant_cfg}


def save_dashboard_settings(path: Path, settings: dict[str, Any], tenants: dict[str, dict[str, Any]]) -> tuple[bool, str]:
    try:
        normalized = load_dashboard_settings(path, tenants) if path.exists() else default_dashboard_settings(tenants)
        incoming_defaults = settings.get("defaults", {}) if isinstance(settings, dict) else {}
        normalized["defaults"] = {
            "overview_kpis": _normalize_kpi_keys(
                incoming_defaults.get("overview_kpis", normalized["defaults"]["overview_kpis"]),
                DEFAULT_OVERVIEW_KPI_KEYS,
            ),
            "traffic_kpis": _normalize_kpi_keys(
                incoming_defaults.get("traffic_kpis", normalized["defaults"]["traffic_kpis"]),
                DEFAULT_TRAFFIC_KPI_KEYS,
            ),
            "overview_sections": _normalize_section_keys(
                incoming_defaults.get("overview_sections", normalized["defaults"]["overview_sections"]),
                OVERVIEW_SECTION_OPTIONS,
                DEFAULT_OVERVIEW_SECTION_KEYS,
            ),
            "traffic_sections": _normalize_section_keys(
                incoming_defaults.get("traffic_sections", normalized["defaults"]["traffic_sections"]),
                TRAFFIC_SECTION_OPTIONS,
                DEFAULT_TRAFFIC_SECTION_KEYS,
            ),
            "default_platform": _normalize_platform_option(
                incoming_defaults.get("default_platform", normalized["defaults"]["default_platform"])
            ),
            "default_view_mode": _normalize_view_mode_option(
                incoming_defaults.get("default_view_mode", normalized["defaults"]["default_view_mode"])
            ),
            "show_sidebar_meta_token_health": _coerce_bool(
                incoming_defaults.get(
                    "show_sidebar_meta_token_health",
                    normalized["defaults"].get("show_sidebar_meta_token_health", True),
                ),
                default=True,
            ),
        }
        incoming_tenants = settings.get("tenants", {}) if isinstance(settings, dict) else {}
        if not isinstance(incoming_tenants, dict):
            incoming_tenants = {}
        tenant_cfg: dict[str, dict[str, Any]] = {}
        for tenant_id in tenants.keys():
            raw_cfg = incoming_tenants.get(tenant_id, {})
            if not isinstance(raw_cfg, dict):
                raw_cfg = {}
            tenant_cfg[tenant_id] = {
                "overview_kpis": _normalize_kpi_keys(
                    raw_cfg.get("overview_kpis", normalized["defaults"]["overview_kpis"]),
                    normalized["defaults"]["overview_kpis"],
                ),
                "traffic_kpis": _normalize_kpi_keys(
                    raw_cfg.get("traffic_kpis", normalized["defaults"]["traffic_kpis"]),
                    normalized["defaults"]["traffic_kpis"],
                ),
                "overview_sections": _normalize_section_keys(
                    raw_cfg.get("overview_sections", normalized["defaults"]["overview_sections"]),
                    OVERVIEW_SECTION_OPTIONS,
                    normalized["defaults"]["overview_sections"],
                ),
                "traffic_sections": _normalize_section_keys(
                    raw_cfg.get("traffic_sections", normalized["defaults"]["traffic_sections"]),
                    TRAFFIC_SECTION_OPTIONS,
                    normalized["defaults"]["traffic_sections"],
                ),
                "default_platform": _normalize_platform_option(
                    raw_cfg.get("default_platform", normalized["defaults"]["default_platform"])
                ),
                "default_view_mode": _normalize_view_mode_option(
                    raw_cfg.get("default_view_mode", normalized["defaults"]["default_view_mode"])
                ),
                "show_sidebar_meta_token_health": _coerce_bool(
                    raw_cfg.get(
                        "show_sidebar_meta_token_health",
                        normalized["defaults"].get("show_sidebar_meta_token_health", True),
                    ),
                    default=normalized["defaults"].get("show_sidebar_meta_token_health", True),
                ),
            }
        payload = {"defaults": normalized["defaults"], "tenants": tenant_cfg}
        path.parent.mkdir(parents=True, exist_ok=True)
        ok_backup, backup_info = _backup_config_file(path)
        if not ok_backup:
            return False, f"No se pudo crear backup: {backup_info}"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        return True, ""
    except Exception as exc:
        return False, str(exc)


def tenant_dashboard_settings(settings: dict[str, Any], tenant_id: str) -> dict[str, Any]:
    defaults = settings.get("defaults", {}) if isinstance(settings, dict) else {}
    tenants_cfg = settings.get("tenants", {}) if isinstance(settings, dict) else {}
    raw_cfg = tenants_cfg.get(tenant_id, {}) if isinstance(tenants_cfg, dict) else {}
    if not isinstance(raw_cfg, dict):
        raw_cfg = {}
    defaults_overview_kpis = _normalize_kpi_keys(
        defaults.get("overview_kpis", DEFAULT_OVERVIEW_KPI_KEYS),
        DEFAULT_OVERVIEW_KPI_KEYS,
    )
    defaults_traffic_kpis = _normalize_kpi_keys(
        defaults.get("traffic_kpis", DEFAULT_TRAFFIC_KPI_KEYS),
        DEFAULT_TRAFFIC_KPI_KEYS,
    )
    defaults_overview_sections = _normalize_section_keys(
        defaults.get("overview_sections", DEFAULT_OVERVIEW_SECTION_KEYS),
        OVERVIEW_SECTION_OPTIONS,
        DEFAULT_OVERVIEW_SECTION_KEYS,
    )
    defaults_traffic_sections = _normalize_section_keys(
        defaults.get("traffic_sections", DEFAULT_TRAFFIC_SECTION_KEYS),
        TRAFFIC_SECTION_OPTIONS,
        DEFAULT_TRAFFIC_SECTION_KEYS,
    )
    defaults_token_health = _coerce_bool(defaults.get("show_sidebar_meta_token_health", True), default=True)
    return {
        "overview_kpis": _normalize_kpi_keys(
            raw_cfg.get("overview_kpis", defaults.get("overview_kpis", DEFAULT_OVERVIEW_KPI_KEYS)),
            defaults_overview_kpis,
        ),
        "traffic_kpis": _normalize_kpi_keys(
            raw_cfg.get("traffic_kpis", defaults.get("traffic_kpis", DEFAULT_TRAFFIC_KPI_KEYS)),
            defaults_traffic_kpis,
        ),
        "overview_sections": _normalize_section_keys(
            raw_cfg.get("overview_sections", defaults_overview_sections),
            OVERVIEW_SECTION_OPTIONS,
            defaults_overview_sections,
        ),
        "traffic_sections": _normalize_section_keys(
            raw_cfg.get("traffic_sections", defaults_traffic_sections),
            TRAFFIC_SECTION_OPTIONS,
            defaults_traffic_sections,
        ),
        "default_platform": _normalize_platform_option(raw_cfg.get("default_platform", defaults.get("default_platform", "All"))),
        "default_view_mode": _normalize_view_mode_option(
            raw_cfg.get("default_view_mode", defaults.get("default_view_mode", "Overview"))
        ),
        "show_sidebar_meta_token_health": _coerce_bool(
            raw_cfg.get("show_sidebar_meta_token_health", defaults_token_health),
            default=defaults_token_health,
        ),
    }


def _format_kpi_value(fmt_key: str, value: float | None) -> str:
    if fmt_key == "money":
        return fmt_money(value)
    if fmt_key == "pct":
        return fmt_pct(value)
    if fmt_key == "duration":
        return fmt_duration(value)
    if fmt_key == "compact":
        return fmt_compact(value)
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.0f}"
    except Exception:
        return "N/A"


def _kpi_delta(cur: dict[str, float | None], prev: dict[str, float | None], key: str, mode: str, cur_days: int, prev_days: int) -> float | None:
    cur_val = cur.get(key)
    prev_val = prev.get(key)
    if mode == "daily":
        cur_daily = sdiv(sf(cur_val), float(max(cur_days, 1)))
        prev_daily = sdiv(sf(prev_val), float(prev_days)) if prev_days else None
        return pct_delta(cur_daily, prev_daily)
    return pct_delta(cur_val, prev_val)


def build_kpi_payload(
    cur: dict[str, float | None],
    prev: dict[str, float | None],
    cur_days: int,
    prev_days: int,
) -> dict[str, dict[str, str]]:
    payload: dict[str, dict[str, str]] = {}
    for key, meta in KPI_CATALOG.items():
        value = cur.get(key)
        delta = _kpi_delta(cur, prev, key, str(meta.get("delta_mode", "direct")), cur_days, prev_days)
        payload[key] = {
            "label": str(meta.get("label", key)),
            "value": _format_kpi_value(str(meta.get("fmt", "int")), value),
            "delta": fmt_delta_compact(delta),
            "delta_color": str(meta.get("delta_color", "normal")),
        }
    return payload


def render_kpi_cards(
    selected_keys: list[str],
    payload: dict[str, dict[str, str]],
    fallback_keys: list[str],
) -> None:
    valid = [k for k in selected_keys if k in payload]
    if not valid:
        valid = [k for k in fallback_keys if k in payload]
    if not valid:
        valid = [k for k in list(payload.keys())[:4]]
    cols = st.columns(len(valid))
    for col, key in zip(cols, valid):
        item = payload.get(key, {})
        col.metric(
            str(item.get("label", key)),
            str(item.get("value", "N/A")),
            str(item.get("delta", "N/A")),
            delta_color=str(item.get("delta_color", "normal")),
        )


def _password_matches(username: str, password: str, salt: str, expected_hash: str) -> bool:
    default_passwords = {
        "admin": "AdminYAP2026!",
        "regional": "RegionalYAP2026!",
    }
    if password and default_passwords.get(username, "") == password:
        return True
    if not expected_hash:
        return False
    candidates = (
        f"{salt}{password}",
        f"{password}{salt}",
        password,
    )
    for raw in candidates:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest().lower()
        if digest == expected_hash:
            return True
    return False


def _ensure_authenticated(users: dict[str, dict[str, Any]]) -> dict[str, Any]:
    auth_user = st.session_state.get("auth_user")
    if isinstance(auth_user, dict) and auth_user.get("username"):
        session_username = str(auth_user.get("username", "")).strip().lower()
        persisted_user = users.get(session_username, {})
        if isinstance(persisted_user, dict) and persisted_user.get("enabled", True):
            synced_user = {
                "username": str(persisted_user.get("username", session_username)),
                "name": str(persisted_user.get("name", session_username)),
                "role": str(persisted_user.get("role", "viewer")).strip().lower() or "viewer",
                "global_role": str(persisted_user.get("global_role", "user")).strip().lower() or "user",
                "allowed_tenants": _normalize_allowed_tenants(persisted_user.get("allowed_tenants", ["*"])),
                "tenant_scopes": _normalize_tenant_scopes(
                    persisted_user.get("tenant_scopes"),
                    _normalize_allowed_tenants(persisted_user.get("allowed_tenants", ["*"])),
                    str(persisted_user.get("role", "viewer")).strip().lower() or "viewer",
                ),
            }
            st.session_state["auth_user"] = synced_user
            return synced_user
        st.session_state.pop("auth_user", None)
        st.session_state.pop("sidebar_view_mode", None)

    st.markdown(
        """
        <style>
          :root {
            --login-card-width: 432px;
            --login-title-size: 2.15rem;
          }
          .stApp,
          [data-testid="stAppViewContainer"] {
            background: none !important;
            background-image: none !important;
          }
          [data-testid="stSidebar"],
          [data-testid="collapsedControl"],
          [data-testid="stSidebarCollapseButton"] {
            display: none !important;
          }
          .block-container {
            max-width: 100% !important;
            padding-top: 4.2rem !important;
            padding-bottom: 1.8rem !important;
            display: flex !important;
            flex-direction: column !important;
            align-items: center !important;
          }
          [data-testid="stForm"] {
            background: rgba(255,255,255,0.78) !important;
            border: 1px solid rgba(32,29,29,0.07) !important;
            border-radius: 30px !important;
            padding: 1.9rem 1.8rem 1.4rem 1.8rem !important;
            box-shadow: 0 22px 50px rgba(15,23,42,0.10) !important;
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            width: min(var(--login-card-width), calc(100vw - 2.4rem)) !important;
            max-width: min(var(--login-card-width), calc(100vw - 2.4rem)) !important;
            margin: 0 auto !important;
          }
          [data-testid="stForm"] form {
            width: min(var(--login-card-width), calc(100vw - 2.4rem)) !important;
            max-width: min(var(--login-card-width), calc(100vw - 2.4rem)) !important;
            margin: 0 auto !important;
          }
          .login-brand {
            display: flex;
            flex-direction: column;
            align-items: center;
            text-align: center;
            margin-bottom: 1.2rem;
          }
          .login-top-spacer {
            height: 72px;
            width: 1px;
          }
          .login-title {
            margin-top: 1rem;
            color: #201D1D;
            font-size: var(--login-title-size);
            line-height: 1.02;
            font-weight: 800;
            letter-spacing: -0.02em;
          }
          .login-sub {
            margin-top: 0.35rem;
            color: #5A6170;
            font-size: 0.95rem;
            font-weight: 500;
          }
          .login-field-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.28rem;
          }
          .login-label {
            color: #2D333E;
            font-size: 0.88rem;
            font-weight: 700;
          }
          .login-forgot {
            color: #8C929D;
            font-size: 0.78rem;
            font-weight: 600;
          }
          [data-testid="stForm"] .stTextInput > div > div {
            border-radius: 12px !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            min-height: 2.82rem !important;
            background: rgba(248,249,251,0.95) !important;
          }
          [data-testid="stForm"] .stTextInput input {
            color: #3B4452 !important;
            font-size: 0.99rem !important;
          }
          [data-testid="stFormSubmitButton"] button {
            min-height: 2.86rem !important;
            border-radius: 12px !important;
            border: 1px solid #6BBF31 !important;
            background: #7BCC35 !important;
            color: #FFFFFF !important;
            font-size: 1.08rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.01em;
            box-shadow: 0 10px 20px rgba(123,204,53,0.28);
          }
          [data-testid="stFormSubmitButton"] button:hover {
            background: #6DBD30 !important;
            border-color: #62AC2B !important;
          }
          .login-secure {
            margin-top: 1rem;
            text-align: center;
          }
          .login-secure-title {
            color: #9BA1AB;
            font-size: 0.62rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-weight: 800;
          }
          .login-secure-icons {
            margin-top: 0.5rem;
            display: flex;
            gap: 0.6rem;
            justify-content: center;
          }
          .login-secure-icon {
            width: 38px;
            height: 38px;
            border-radius: 999px;
            border: 1px solid rgba(32,29,29,0.08);
            background: rgba(255,255,255,0.82);
            display: inline-flex;
            align-items: center;
            justify-content: center;
            color: #8B9099;
            font-size: 1rem;
          }
          .login-footer {
            margin-top: 1.2rem;
            text-align: center;
            color: #8B9099;
            font-size: 0.76rem;
            line-height: 1.35;
          }
          .login-footer a {
            color: #5A6170;
            text-decoration: none;
            font-weight: 700;
          }
          .login-footer a:hover { text-decoration: underline; }
          @media (max-width: 640px) {
            .block-container { padding-top: 1.2rem !important; }
            [data-testid="stForm"] {
              border-radius: 22px !important;
              width: calc(100vw - 1.3rem) !important;
              padding: 1.1rem !important;
            }
            .login-title { font-size: 1.95rem; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.form("login_form", clear_on_submit=False):
        st.markdown(
            """
            <div class="login-brand">
              <div class="login-top-spacer" aria-hidden="true"></div>
              <div class="login-title">iPalmera Analítica</div>
              <div class="login-sub">Marketing Command Center</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("<div class='login-label'>Usuario</div>", unsafe_allow_html=True)
        username = st.text_input(
            "Usuario",
            key="login_username",
            label_visibility="collapsed",
        )
        st.markdown("<div class='login-label'>Contraseña</div>", unsafe_allow_html=True)
        password = st.text_input(
            "Contraseña",
            type="password",
            key="login_password",
            label_visibility="collapsed",
            placeholder="",
        )
        submitted = st.form_submit_button("Iniciar Sesión  →", width="stretch")
        st.markdown(
            """
            <div class="login-secure">
              <div class="login-secure-title">Acceso Seguro</div>
              <div class="login-secure-icons">
                <span class="login-secure-icon">◌</span>
                <span class="login-secure-icon">⌁</span>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if submitted:
        u = str(username or "").strip().lower()
        p = str(password or "")
        user = users.get(u)
        if not user or not user.get("enabled", True):
            st.error("Usuario no válido o inactivo.")
            st.stop()
        if not _password_matches(
            str(user.get("username", "")),
            p,
            str(user.get("password_salt", "")),
            str(user.get("password_hash", "")),
        ):
            st.error("Credenciales inválidas.")
            st.stop()
        st.session_state["auth_user"] = {
            "username": user["username"],
            "name": user["name"],
            "role": user["role"],
            "global_role": user.get("global_role", "user"),
            "allowed_tenants": user.get("allowed_tenants", ["*"]),
            "tenant_scopes": user.get("tenant_scopes", []),
        }
        st.session_state.pop("login_password", None)
        st.rerun()

    st.markdown(
        """
        <div class="login-footer">
          Powered By iPalmera 2026 Vibe Coding
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()


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

def render_sidebar(tenants: dict[str, dict[str, Any]]) -> tuple[str, str]:
    auth_user = st.session_state.get("auth_user", {}) if isinstance(st.session_state.get("auth_user"), dict) else {}
    user_name = str(auth_user.get("name", "Admin User"))
    tenant_ids = _auth_user_tenant_ids(auth_user, tenants)
    if not tenant_ids:
        st.sidebar.error("Tu usuario no tiene tenants asignados.")
        st.stop()
    is_admin_user = _auth_user_is_admin(auth_user)
    default_id = DEFAULT_TENANT_ID if DEFAULT_TENANT_ID in tenants else tenant_ids[0]
    if st.session_state.get("active_tenant_id") not in tenants:
        st.session_state["active_tenant_id"] = default_id
    if st.session_state.get("active_tenant_id") not in tenant_ids:
        st.session_state["active_tenant_id"] = tenant_ids[0]
    current_tenant_id = st.session_state.get("active_tenant_id", default_id)
    team_name = str(tenants.get(current_tenant_id, {}).get("name", "YAP Marketing"))
    initial = user_name[:1].upper()
    st.sidebar.markdown(
        f"""
        <div class="sidebar-profile-card">
          <div class="sidebar-profile-row">
            <div class="sidebar-avatar">{initial}</div>
            <div>
              <div class="sidebar-profile-name">{html.escape(user_name)}</div>
              <div class="sidebar-profile-meta">{html.escape(team_name)}</div>
            </div>
          </div>
        </div>
        <div class="sidebar-kicker">Workspace</div>
        """,
        unsafe_allow_html=True,
    )
    tenant_id = st.sidebar.selectbox(
        "Workspace",
        options=tenant_ids,
        key="active_tenant_id",
        format_func=lambda t: str(tenants.get(t, {}).get("name", t)),
        label_visibility="collapsed",
    )
    if "sidebar_view_mode" not in st.session_state:
        st.session_state["sidebar_view_mode"] = "Overview"
    view_mode = str(st.session_state.get("sidebar_view_mode", "Overview"))
    if view_mode == "Administración" and not is_admin_user:
        st.session_state["sidebar_view_mode"] = "Overview"
        view_mode = "Overview"
    if st.sidebar.button(
        "Overview",
        key="nav_overview_btn",
        icon=":material/dashboard:",
        type="primary" if view_mode == "Overview" else "secondary",
        width="stretch",
    ):
        st.session_state["sidebar_view_mode"] = "Overview"
        view_mode = "Overview"
    if st.sidebar.button(
        "Tráfico y Adquisición",
        key="nav_traffic_btn",
        icon=":material/analytics:",
        type="primary" if view_mode == "Tráfico y Adquisición" else "secondary",
        width="stretch",
    ):
        st.session_state["sidebar_view_mode"] = "Tráfico y Adquisición"
        view_mode = "Tráfico y Adquisición"
    if is_admin_user and st.sidebar.button(
        "Administración",
        key="nav_admin_btn",
        icon=":material/admin_panel_settings:",
        type="primary" if view_mode == "Administración" else "secondary",
        width="stretch",
    ):
        st.session_state["sidebar_view_mode"] = "Administración"
        view_mode = "Administración"
    st.sidebar.markdown("<div class='sidebar-bottom'></div>", unsafe_allow_html=True)
    if st.sidebar.button("Logout", key="sidebar_logout_btn", width="stretch"):
        for k in (
            "auth_user",
            "sidebar_view_mode",
            "sidebar_view_tenant_cfg",
            "active_tenant_id",
            "platform_filter_radio_v2",
            "platform_filter_tenant_id",
            "top_date_range",
            "login_username",
            "login_password",
        ):
            st.session_state.pop(k, None)
        st.rerun()
    return tenant_id, view_mode


def render_sidebar_meta_token_health(report: dict[str, Any]) -> None:
    meta = report.get("metadata", {}) if isinstance(report, dict) else {}
    token = meta.get("meta_token_status", {}) if isinstance(meta, dict) else {}
    if not isinstance(token, dict):
        token = {}

    valid_raw = token.get("is_valid")
    is_valid = bool(valid_raw) if isinstance(valid_raw, bool) else None
    days_raw = token.get("days_left")
    try:
        days_left = int(days_raw) if days_raw is not None else None
    except Exception:
        days_left = None

    if is_valid is False:
        badge_cls = "bad"
        status_text = "Inválido"
    elif days_left is None:
        badge_cls = "warn"
        status_text = "Sin dato"
    elif days_left <= 7:
        badge_cls = "bad"
        status_text = "Crítico"
    elif days_left <= 21:
        badge_cls = "warn"
        status_text = "Por vencer"
    else:
        badge_cls = "good"
        status_text = "Activo"

    expires_raw = str(token.get("expires_at_utc", "N/A")).strip()
    if expires_raw and expires_raw != "N/A":
        expires_at = expires_raw.split("T", 1)[0]
    else:
        expires_at = "N/A"
    if days_left is None:
        days_badge_cls = "warn"
        days_text = "N/A"
    elif days_left <= 7:
        days_badge_cls = "bad"
        days_text = str(days_left)
    elif days_left <= 21:
        days_badge_cls = "warn"
        days_text = str(days_left)
    else:
        days_badge_cls = "good"
        days_text = str(days_left)

    st.sidebar.markdown(
        textwrap.dedent(
            f"""
            <div class="sidebar-token-wrap">
              <div class="sidebar-token-card">
                <div class="sidebar-token-title">Meta Token Health</div>
                <div class="sidebar-token-row">
                  <div class="sidebar-token-days-label">Días restantes</div>
                  <div class="sidebar-token-days {days_badge_cls if days_left is not None else 'na'}">{html.escape(days_text)}</div>
                  <span class="sidebar-token-badge {badge_cls}">{status_text}</span>
                </div>
                <div class="sidebar-token-item"><span class="k">Expira</span><span class="v">{html.escape(expires_at)}</span></div>
              </div>
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


def render_top_filters(
    min_d: date,
    max_d: date,
    tenant_name: str,
    tenant_id: str,
    default_platform: str,
) -> tuple[date, date, str]:
    default_platform_value = _normalize_platform_option(default_platform)
    last_tenant = str(st.session_state.get("platform_filter_tenant_id", ""))
    if last_tenant != tenant_id or st.session_state.get("platform_filter_radio_v2") not in PLATFORM_OPTIONS:
        st.session_state["platform_filter_radio_v2"] = default_platform_value
        st.session_state["platform_filter_tenant_id"] = tenant_id
    wrapper_left, wrapper_right = st.columns([2.2, 1.8], gap="large")
    with wrapper_left:
        st.markdown(
            f"""
            <div class='hero'>
              <div class='hero-kicker'>iPalmera IA Analítica</div>
              <div class='hero-sub'>{html.escape(tenant_name)} Marketing Performance</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with wrapper_right:
        pcol, dcol = st.columns([1.65, 0.95], gap="small")
        with pcol:
            platform = st.radio(
                "Plataforma",
                list(PLATFORM_OPTIONS),
                key="platform_filter_radio_v2",
                horizontal=True,
                label_visibility="collapsed",
            )
            if platform not in PLATFORM_OPTIONS:
                platform = default_platform_value
        with dcol:
            st.markdown("<div class='app-filter-title' style='margin-top:0.1rem;'>Rango</div>", unsafe_allow_html=True)
            sel = st.date_input(
                "Rango",
                value=(max(min_d, max_d - timedelta(days=29)), max_d),
                min_value=min_d,
                max_value=max_d,
                key="top_date_range",
                label_visibility="collapsed",
            )
    s, e = _normalize_date_range(sel, min_d, max_d)

    return s, e, platform


def render_exec(
    df_sel: pd.DataFrame,
    df_prev: pd.DataFrame,
    platform: str,
    overview_kpi_keys: list[str],
    overview_section_keys: list[str],
    paid_dev_df: pd.DataFrame,
    camp_df: pd.DataFrame,
    ga4_event_df: pd.DataFrame,
    ga4_conversion_event_name: str,
    tenant_meta_account_id: str,
    tenant_google_customer_id: str,
    s,
    e,
    prev_s,
    prev_e,
):
    selected_sections = _normalize_section_keys(
        overview_section_keys,
        OVERVIEW_SECTION_OPTIONS,
        DEFAULT_OVERVIEW_SECTION_KEYS,
    )
    section_set = set(selected_sections)
    cur, prev = summary(df_sel, platform), summary(df_prev, platform)
    cur_days, prev_days = max(len(df_sel), 1), len(df_prev)
    kpi_payload = build_kpi_payload(cur, prev, cur_days, prev_days)
    if "kpis" in section_set:
        render_kpi_cards(overview_kpi_keys, kpi_payload, DEFAULT_OVERVIEW_KPI_KEYS)

    c = metric_cols(platform)

    def _render_trend_chart() -> None:
        st.markdown(
            """
            <div class="viz-card">
              <p class="viz-title">Performance Across Platforms</p>
              <div class="viz-sub">Daily investment over time</div>
            """,
            unsafe_allow_html=True,
        )
        ld = df_sel.sort_values("date").copy()
        if ld.empty:
            st.info("Sin datos para el periodo seleccionado.")
        else:
            ld["google_trend"] = pd.to_numeric(ld["google_spend"], errors="coerce").fillna(0.0)
            ld["meta_trend"] = pd.to_numeric(ld["meta_spend"], errors="coerce").fillna(0.0)
            fig = go.Figure()
            if platform in ("All", "Google"):
                fig.add_trace(
                    go.Scatter(
                        x=ld["date"],
                        y=ld["google_trend"],
                        mode="lines",
                        name="Google Ads",
                        line={"color": C_GOOGLE, "width": 4, "shape": "spline"},
                        hovertemplate="%{x|%d %b %Y}<br>Google: $%{y:,.2f}<extra></extra>",
                    )
                )
            if platform in ("All", "Meta"):
                fig.add_trace(
                    go.Scatter(
                        x=ld["date"],
                        y=ld["meta_trend"],
                        mode="lines",
                        name="Meta Ads",
                        line={"color": C_META, "width": 4, "shape": "spline"},
                        hovertemplate="%{x|%d %b %Y}<br>Meta: $%{y:,.2f}<extra></extra>",
                    )
                )
            pbi_layout(fig, yaxis_title="Inversión diaria ($)", xaxis_title="")
            fig.update_layout(height=355, hovermode="x unified", showlegend=True)
            fig.update_xaxes(tickformat="%d %b", tickfont={"size": 10, "color": "#7A879D"})
            fig.update_yaxes(tickfont={"size": 10, "color": "#7A879D"})
            st.plotly_chart(fig, width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

    def _render_funnel_and_ga4() -> None:
        if "funnel" in section_set:
            impr = float(df_sel[c["impr"]].sum())
            clicks = float(df_sel[c["clicks"]].sum())
            conv = float(df_sel[c["conv"]].sum())
            sess_total = float(df_sel["ga4_sessions"].sum())
            sess = sess_total if platform == "All" else sess_total * (sdiv(clicks, float(df_sel["total_clicks"].sum())) or 0.0)
            if impr <= 0 and clicks > 0:
                impr = clicks / 0.03

            funnel_vals = [
                ("Impresiones", max(impr, 0.0)),
                ("Clics", max(min(clicks, impr), 0.0)),
                ("Sesiones", max(min(sess, clicks), 0.0)),
                ("Conversiones", max(min(conv, sess), 0.0)),
            ]
            top_val = max(float(funnel_vals[0][1]), 1.0)
            funnel_stage_colors = ["#7BCC35", "#3AE7FC", "#7A879D", "#FE492A"]
            rows_html: list[str] = []
            prev_val: float | None = None
            for idx, (name, value) in enumerate(funnel_vals):
                pct = 0.0 if value <= 0 else min(max((value / top_val) * 100.0, 0.0), 100.0)
                stage_color = funnel_stage_colors[idx % len(funnel_stage_colors)]
                if prev_val is None or prev_val <= 0:
                    drop_html = "<span class='funnel-drop funnel-drop-base' title='Etapa base'>Base</span>"
                else:
                    drop_pct = max(0.0, min(100.0, (1.0 - sdiv(value, prev_val)) * 100.0))
                    drop_html = f"<span class='funnel-drop' title='Caída vs etapa anterior'>&darr; {drop_pct:.1f}%</span>"
                rows_html.append(
                    f"<div class='funnel-row'>"
                    f"<div class='funnel-fill' style='width:{pct:.2f}%; background:{stage_color}33; border:1px solid {stage_color}66;'></div>"
                    f"<div class='funnel-content'>"
                    f"<span class='funnel-name'>{name}</span>"
                    f"<span class='funnel-metrics'><span class='funnel-value'>{fmt_compact(value)}</span>{drop_html}</span>"
                    f"</div>"
                    f"</div>"
                )
                prev_val = value

            st.markdown(
                textwrap.dedent(
                    f"""
                    <div class='funnel-card'>
                      <div class='funnel-title'>Funnel de Conversión</div>
                      <div class='funnel-stack'>{''.join(rows_html)}</div>
                    </div>
                    """
                ).strip(),
                unsafe_allow_html=True,
            )

        if "ga4_conversion" in section_set:
            ga4_event_name = str(ga4_conversion_event_name or GA4_GTC_SOLICITAR_CODIGO_EVENT).strip() or GA4_GTC_SOLICITAR_CODIGO_EVENT
            ga4_filtered = ga4_event_df.copy() if not ga4_event_df.empty else pd.DataFrame()
            ga4_conv_total = 0.0
            if not ga4_filtered.empty:
                if "date" in ga4_filtered.columns:
                    ga4_filtered = ga4_filtered[(ga4_filtered["date"] >= s) & (ga4_filtered["date"] <= e)]
                event_col = "eventName" if "eventName" in ga4_filtered.columns else ("event_name" if "event_name" in ga4_filtered.columns else None)
                if event_col:
                    ga4_filtered = ga4_filtered[
                        ga4_filtered[event_col].astype(str).str.strip().str.lower() == ga4_event_name.lower()
                    ]
                if platform in ("Google", "Meta") and "platform" in ga4_filtered.columns:
                    ga4_filtered = ga4_filtered[
                        ga4_filtered["platform"].astype(str).str.strip().str.lower() == platform.lower()
                    ]
                conv_col = (
                    "conversions"
                    if "conversions" in ga4_filtered.columns
                    else ("eventCount" if "eventCount" in ga4_filtered.columns else ("event_count" if "event_count" in ga4_filtered.columns else None))
                )
                if conv_col:
                    ga4_conv_total = float(pd.to_numeric(ga4_filtered[conv_col], errors="coerce").fillna(0.0).sum())

            spend_total = float(df_sel[c["spend"]].sum()) if not df_sel.empty else 0.0
            ga4_cpl = sdiv(spend_total, ga4_conv_total)
            st.markdown(
                textwrap.dedent(
                    f"""
                    <div class='ga4-conv-card'>
                      <div class='ga4-conv-title'>Conversiones GA4</div>
                      <div class='ga4-conv-event'>Evento: {html.escape(ga4_event_name)}</div>
                      <div class='ga4-conv-grid'>
                        <div class='ga4-conv-item'>
                          <span class='ga4-conv-label'>Conversiones</span>
                          <span class='ga4-conv-value'>{fmt_compact(ga4_conv_total)}</span>
                        </div>
                        <div class='ga4-conv-item'>
                          <span class='ga4-conv-label'>Inversión</span>
                          <span class='ga4-conv-value'>{fmt_money(spend_total)}</span>
                        </div>
                        <div class='ga4-conv-item'>
                          <span class='ga4-conv-label'>Plataforma</span>
                          <span class='ga4-conv-value'>{html.escape(platform)}</span>
                        </div>
                        <div class='ga4-conv-item'>
                          <span class='ga4-conv-label'>CPL GA4</span>
                          <span class='ga4-conv-value'>{fmt_money(ga4_cpl)}</span>
                        </div>
                      </div>
                    </div>
                    """
                ).strip(),
                unsafe_allow_html=True,
            )

    show_trend = "trend_chart" in section_set
    show_right_stack = ("funnel" in section_set) or ("ga4_conversion" in section_set)
    if show_trend and show_right_stack:
        chart_col, funnel_col = st.columns([3.1, 1.2], gap="large")
        with chart_col:
            _render_trend_chart()
        with funnel_col:
            _render_funnel_and_ga4()
    elif show_trend:
        _render_trend_chart()
    elif show_right_stack:
        _render_funnel_and_ga4()

    if "device_breakdown" in section_set:
        st.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='viz-title' style='margin-bottom:0.35rem;'>4) Dispositivos de Pauta (Desktop / Mobile / Other)</div>",
            unsafe_allow_html=True,
        )

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
                pprev.groupby("device", as_index=False).agg(impressions_prev=("impressions", "sum"))
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

            m1, m2, m3 = st.columns(3)
            for idx, dname in enumerate(order):
                row = roll[roll["device"] == dname].iloc[0]
                target_col = [m1, m2, m3][idx]
                target_col.metric(
                    f"{dname} Impresiones",
                    f"{float(row['impressions']):,.0f}",
                    fmt_delta_compact(row["delta_impressions"]),
                )

            st.markdown("<div class='viz-card'>", unsafe_allow_html=True)
            bar = go.Figure(
                go.Bar(
                    x=roll["device"],
                    y=roll["impressions"],
                    marker={
                        "color": ["#7BCC35", "#FE492A", "#7A879D"],
                        "line": {"color": "rgba(32,29,29,0.08)", "width": 1},
                    },
                    text=[f"{v:,.0f}" for v in roll["impressions"]],
                    textposition="outside",
                    textfont={"color": "#334761", "size": 12},
                    hovertemplate="%{x}: %{y:,.0f}<extra></extra>",
                )
            )
            pbi_layout(bar, xaxis_title="", yaxis_title="Impresiones", legend_h=False)
            bar.update_layout(height=280, margin={"l": 12, "r": 12, "t": 8, "b": 12})
            st.plotly_chart(bar, width="stretch")
            st.markdown("</div>", unsafe_allow_html=True)

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
            )[["Device", "Spend", "Impressions", "Clicks", "Conversions", "CTR", "CPL"]]

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
                width="stretch",
                hide_index=True,
            )

    if "audit_table" in section_set:
        st.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='viz-title' style='margin-bottom:0.35rem;'>5) Tabla Maestra de Auditoria</div>",
            unsafe_allow_html=True,
        )
        t = df_sel[
            [
                "date",
                "meta_spend",
                "google_spend",
                c["spend"],
                c["clicks"],
                c["impr"],
                c["conv"],
                "ga4_sessions",
                "ga4_avg_sess",
                "ga4_bounce",
            ]
        ].copy()
        t.columns = [
            "Date",
            "Meta Spend",
            "Google Spend",
            "Spend",
            "Clicks",
            "Impressions",
            "Conversions",
            "Sessions",
            "Avg Session (s)",
            "Bounce Rate",
        ]
        t["CPL"] = t.apply(lambda r: sdiv(float(r["Spend"]), float(r["Conversions"])), axis=1)
        t["CTR"] = t.apply(lambda r: sdiv(float(r["Clicks"]), float(r["Impressions"])), axis=1)
        t = t.sort_values("Date", ascending=False)
        sty = (
            t.style.format(
                {
                    "Date": lambda v: v.isoformat() if hasattr(v, "isoformat") else str(v),
                    "Meta Spend": lambda v: fmt_money(float(v)),
                    "Google Spend": lambda v: fmt_money(float(v)),
                    "Spend": lambda v: fmt_money(float(v)),
                    "Clicks": "{:.0f}",
                    "Impressions": "{:.0f}",
                    "Conversions": "{:.2f}",
                    "Sessions": "{:.0f}",
                    "Avg Session (s)": "{:.1f}",
                    "Bounce Rate": lambda v: fmt_pct(float(v)),
                    "CPL": lambda v: fmt_money(v if pd.notna(v) else None),
                    "CTR": lambda v: fmt_pct(v if pd.notna(v) else None),
                }
            )
            .background_gradient(subset=["Spend"], cmap="Blues")
            .background_gradient(subset=["Conversions"], cmap="Greens")
            .background_gradient(subset=["CPL"], cmap="RdYlGn_r")
            .background_gradient(subset=["CTR"], cmap="PuBu")
        )
        st.dataframe(sty, width="stretch", hide_index=True)

    if "top_pieces" in section_set:
        render_top_pieces_range(
            camp_df,
            platform,
            s,
            e,
            tenant_meta_account_id=tenant_meta_account_id,
            tenant_google_customer_id=tenant_google_customer_id,
        )

def render_top_pieces_range(
    camp_df: pd.DataFrame,
    platform: str,
    start_ref,
    end_ref,
    *,
    tenant_meta_account_id: str = "",
    tenant_google_customer_id: str = "",
):
    if camp_df.empty:
        st.info("No hay datos de piezas/campañas para construir el top 10.")
        return

    cp = camp_df.copy()
    if "date" not in cp.columns:
        st.info("El dataset de piezas no contiene columna de fecha.")
        return

    cp["date"] = pd.to_datetime(cp["date"], errors="coerce").dt.date
    cp = cp.dropna(subset=["date"])
    cp = cp[(cp["date"] >= start_ref) & (cp["date"] <= end_ref)]

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
    for num_col in ("spend", "impressions", "clicks", "conversions"):
        cp[num_col] = pd.to_numeric(cp[num_col], errors="coerce").fillna(0.0)

    if cp.empty:
        st.info("No hay piezas/campañas para el rango seleccionado.")
        return

    top = (
        cp.groupby(["platform", "campaign_id", "campaign_name"], as_index=False)
        .agg(
            inversion=("spend", "sum"),
            conversiones=("conversions", "sum"),
            clics=("clicks", "sum"),
        )
    )
    top["cpl"] = top.apply(lambda r: sdiv(float(r["inversion"]), float(r["conversiones"])), axis=1)
    top = top.sort_values(["conversiones", "clics"], ascending=[False, False], na_position="last").head(10)
    top["Ver"] = top.apply(
        lambda r: campaign_platform_link(
            r.get("platform"),
            r.get("campaign_id"),
            meta_account_id=tenant_meta_account_id,
            google_customer_id=tenant_google_customer_id,
        ),
        axis=1,
    )
    top["Campaña / Pieza"] = top["campaign_name"].astype(str).replace({"": "Sin nombre"})
    top["Plataforma"] = top["platform"].astype(str).replace({"": "N/A"})
    top["Gasto"] = pd.to_numeric(top["inversion"], errors="coerce")
    top["Conversiones"] = pd.to_numeric(top["conversiones"], errors="coerce")
    top["CPL"] = pd.to_numeric(top["cpl"], errors="coerce")
    top_view = top[["Ver", "Campaña / Pieza", "Plataforma", "Gasto", "Conversiones", "CPL"]].copy()
    top_view["Ver"] = top_view["Ver"].fillna("")

    st.markdown("<div class='top-pieces-card'>", unsafe_allow_html=True)
    st.markdown(
        """
        <div class='top-pieces-head'>
          <h3 class='top-pieces-title'>Top 10 Piezas</h3>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.dataframe(
        top_view,
        width="stretch",
        hide_index=True,
        column_config={
            "Ver": st.column_config.LinkColumn("Ver", help="Abrir campaña/pieza", display_text="Abrir"),
            "Campaña / Pieza": st.column_config.TextColumn("Campaña / Pieza"),
            "Plataforma": st.column_config.TextColumn("Plataforma"),
            "Gasto": st.column_config.NumberColumn("Gasto", format="$%.2f"),
            "Conversiones": st.column_config.NumberColumn("Conversiones", format="%.0f"),
            "CPL": st.column_config.NumberColumn("CPL", format="$%.2f"),
        },
    )
    st.markdown("</div>", unsafe_allow_html=True)

def render_traffic(
    df_sel,
    df_prev,
    ch_df,
    pg_df,
    camp_df,
    platform,
    s,
    e,
    traffic_kpi_keys: list[str],
    traffic_section_keys: list[str],
):
    section_title("03 > Rendimiento de Trafico y Adquisicion")
    selected_sections = _normalize_section_keys(
        traffic_section_keys,
        TRAFFIC_SECTION_OPTIONS,
        DEFAULT_TRAFFIC_SECTION_KEYS,
    )
    section_set = set(selected_sections)
    sm = summary(df_sel, platform)
    sm_prev = summary(df_prev, platform)
    kpi_payload = build_kpi_payload(sm, sm_prev, max(len(df_sel), 1), len(df_prev))
    if "kpis" in section_set:
        render_kpi_cards(traffic_kpi_keys, kpi_payload, DEFAULT_TRAFFIC_KPI_KEYS)

    def _render_channels() -> None:
        section_title("Canales / Adquisicion")
        if ch_df.empty:
            st.info("No hay datos de canales en JSON.")
        else:
            ch = ch_df[(ch_df["date"] >= s) & (ch_df["date"] <= e)].copy()
            if ch.empty:
                st.info("Sin datos para el rango seleccionado.")
            else:
                b = (
                    ch.groupby("sessionDefaultChannelGroup", as_index=False)
                    .agg(sessions=("sessions", "sum"), conversions=("conversions", "sum"))
                    .sort_values("sessions", ascending=False)
                    .head(10)
                )
                fig = go.Figure()
                fig.add_trace(go.Bar(x=b["sessionDefaultChannelGroup"], y=b["sessions"], name="Sessions", marker={"color": C_ACCENT}))
                fig.add_trace(go.Scatter(x=b["sessionDefaultChannelGroup"], y=b["conversions"], name="Conversions", mode="lines+markers", line={"color": C_META, "width": 2}, yaxis="y2"))
                pbi_layout(fig, yaxis_title="Sessions", xaxis_title="Canal", y2_title="Conversions")
                st.plotly_chart(fig, width="stretch")

    def _render_top_pages() -> None:
        section_title("Paginas Mas Visitadas")
        if pg_df.empty:
            st.info("No hay datos de paginas en JSON.")
        else:
            pg = pg_df[(pg_df["date"] >= s) & (pg_df["date"] <= e)].copy()
            if pg.empty:
                st.info("Sin datos para el rango seleccionado.")
            else:
                top_p = (
                    pg.groupby(["pagePath", "pageTitle"], as_index=False)
                    .agg(views=("screenPageViews", "sum"), sessions=("sessions", "sum"), avg_session=("averageSessionDuration", "mean"))
                    .sort_values("views", ascending=False)
                    .head(10)
                )
                st.dataframe(top_p, width="stretch", hide_index=True)

    show_channels = "channels" in section_set
    show_top_pages = "top_pages" in section_set
    if show_channels and show_top_pages:
        c1, c2 = st.columns(2)
        with c1:
            _render_channels()
        with c2:
            _render_top_pages()
    elif show_channels:
        _render_channels()
    elif show_top_pages:
        _render_top_pages()

    if "campaigns" in section_set:
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
                st.dataframe(roll.head(20), width="stretch", hide_index=True)


def render_admin_panel(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
    dashboard_settings: dict[str, Any],
) -> None:
    section_title("04 > Administración por Tenant (Fase 4)")

    if not USERS_CONFIG_PATH.exists():
        st.warning("No existe config/users.json todavía. Puedes crearlo desde este panel.")

    total_users = len(users)
    active_users = sum(1 for u in users.values() if bool(u.get("enabled", True)))
    enabled_admins = _enabled_admin_count(users)
    m1, m2, m3 = st.columns(3)
    m1.metric("Usuarios", f"{total_users}")
    m2.metric("Activos", f"{active_users}")
    m3.metric("Admins activos", f"{enabled_admins}")

    rows: list[dict[str, Any]] = []
    for username in sorted(users.keys()):
        user = users.get(username, {})
        role = str(user.get("role", "viewer")).strip().lower() or "viewer"
        global_role = str(user.get("global_role", "user")).strip().lower() or "user"
        scope_map = _scope_map_for_user(user)
        labels: list[str] = []
        for tenant_id in sorted(scope_map.keys()):
            tenant_role = scope_map.get(tenant_id, role)
            if tenant_id == "*":
                labels.append(f"Todos (*):{tenant_role}")
            else:
                tenant_name = str(tenants.get(tenant_id, {}).get("name", tenant_id))
                labels.append(f"{tenant_name}:{tenant_role}")
        rows.append(
            {
                "Usuario": str(user.get("username", username)),
                "Nombre": str(user.get("name", "")),
                "Global Role": global_role,
                "Role (legacy)": role,
                "Activo": bool(user.get("enabled", True)),
                "Scopes por tenant": " | ".join(labels) if labels else "Sin scope",
            }
        )
    if rows:
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
    else:
        st.info("No hay usuarios cargados en config/users.json.")

    tenant_options = ["*"] + sorted(tenants.keys())
    role_options = ["viewer", "editor", "admin"]
    global_role_options = ["user", "admin"]

    tab_edit, tab_create, tab_dashboard, tab_audit = st.tabs(
        ["Editar Usuario", "Crear Usuario", "Variables Dashboard", "Auditoría"]
    )

    with tab_edit:
        if not users:
            st.info("No hay usuarios para editar.")
        else:
            selected_username = st.selectbox(
                "Selecciona usuario",
                options=sorted(users.keys()),
                key="adm_edit_username",
            )
            user = users.get(selected_username, {})
            existing_role = str(user.get("role", "viewer")).strip().lower() or "viewer"
            existing_global_role = str(user.get("global_role", "user")).strip().lower() or "user"
            existing_scope_map = _scope_map_for_user(user)
            selected_defaults = _normalize_tenant_selection(list(existing_scope_map.keys()))
            if not selected_defaults:
                selected_defaults = _normalize_allowed_tenants(user.get("allowed_tenants", ["*"]))

            col_left, col_right = st.columns(2)
            with col_left:
                edit_name = st.text_input(
                    "Nombre",
                    value=str(user.get("name", selected_username)),
                    key=f"adm_edit_name_{selected_username}",
                )
                edit_enabled = st.toggle(
                    "Usuario activo",
                    value=bool(user.get("enabled", True)),
                    key=f"adm_edit_enabled_{selected_username}",
                )
            with col_right:
                edit_global_role = st.selectbox(
                    "Rol global",
                    options=global_role_options,
                    index=global_role_options.index(existing_global_role)
                    if existing_global_role in global_role_options
                    else 0,
                    key=f"adm_edit_global_role_{selected_username}",
                )
                edit_role = st.selectbox(
                    "Rol base (legacy)",
                    options=role_options,
                    index=role_options.index(existing_role) if existing_role in role_options else 0,
                    key=f"adm_edit_role_{selected_username}",
                )

            selected_tenants = st.multiselect(
                "Tenants asignados",
                options=tenant_options,
                default=selected_defaults,
                format_func=lambda t: "Todos (*)" if t == "*" else str(tenants.get(t, {}).get("name", t)),
                key=f"adm_edit_tenants_{selected_username}",
            )
            normalized_selected = _normalize_tenant_selection(selected_tenants)
            if "*" in normalized_selected and len(selected_tenants) > 1:
                st.info("Al incluir '*', se aplicará acceso total y se ignorarán otros tenants.")
            if edit_global_role == "admin" and not normalized_selected:
                normalized_selected = ["*"]

            scope_roles: dict[str, str] = {}
            if normalized_selected:
                st.caption("Roles por tenant")
                for tenant_id in normalized_selected:
                    default_scope_role = existing_scope_map.get(tenant_id, edit_role)
                    safe_tenant = _widget_safe_key(tenant_id)
                    scope_roles[tenant_id] = st.selectbox(
                        f"Rol para {'Todos (*)' if tenant_id == '*' else tenants.get(tenant_id, {}).get('name', tenant_id)}",
                        options=role_options,
                        index=role_options.index(default_scope_role) if default_scope_role in role_options else 0,
                        key=f"adm_edit_scope_role_{selected_username}_{safe_tenant}",
                    )

            pwd_col_1, pwd_col_2 = st.columns(2)
            with pwd_col_1:
                edit_new_password = st.text_input(
                    "Nueva contraseña (opcional)",
                    type="password",
                    key=f"adm_edit_password_{selected_username}",
                )
            with pwd_col_2:
                edit_confirm_password = st.text_input(
                    "Confirmar contraseña",
                    type="password",
                    key=f"adm_edit_password_confirm_{selected_username}",
                )

            save_col, delete_col = st.columns(2)
            if save_col.button(
                "Guardar cambios",
                key=f"adm_edit_save_{selected_username}",
                type="primary",
                width="stretch",
            ):
                errors: list[str] = []
                clean_name = str(edit_name).strip()
                if not clean_name:
                    errors.append("El nombre no puede ir vacío.")
                if edit_global_role != "admin" and not normalized_selected:
                    errors.append("Debes asignar al menos un tenant.")
                if edit_new_password or edit_confirm_password:
                    if edit_new_password != edit_confirm_password:
                        errors.append("La contraseña y su confirmación no coinciden.")
                    if len(edit_new_password) < 8:
                        errors.append("La nueva contraseña debe tener al menos 8 caracteres.")

                was_enabled_admin = bool(user.get("enabled", True)) and _user_record_is_admin(user)
                will_be_admin = edit_global_role == "admin" or edit_role == "admin"
                if was_enabled_admin and not (edit_enabled and will_be_admin) and _enabled_admin_count(users) <= 1:
                    errors.append("No puedes dejar el sistema sin al menos un admin activo.")

                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    allowed_tenants, tenant_scopes = _build_tenant_access(
                        normalized_selected,
                        scope_roles,
                        edit_role,
                    )
                    updated = dict(user)
                    updated["username"] = selected_username
                    updated["name"] = clean_name
                    updated["global_role"] = edit_global_role
                    updated["role"] = edit_role
                    updated["enabled"] = bool(edit_enabled)
                    updated["allowed_tenants"] = allowed_tenants
                    updated["tenant_scopes"] = tenant_scopes
                    if edit_new_password:
                        new_salt = _new_password_salt()
                        updated["password_salt"] = new_salt
                        updated["password_hash"] = _hash_password_with_salt(edit_new_password, new_salt)

                    users_next = dict(users)
                    users_next[selected_username] = updated
                    ok, err_msg = save_users_config(USERS_CONFIG_PATH, users_next)
                    if not ok:
                        st.error(f"No se pudo guardar config/users.json: {err_msg}")
                    else:
                        append_admin_audit(
                            "user_updated",
                            str(auth_user.get("username", "unknown")),
                            target=selected_username,
                            details={
                                "enabled": bool(edit_enabled),
                                "global_role": edit_global_role,
                                "role": edit_role,
                                "allowed_tenants": allowed_tenants,
                                "password_reset": bool(edit_new_password),
                            },
                        )
                        if str(auth_user.get("username", "")).strip().lower() == selected_username:
                            st.session_state["auth_user"] = {
                                "username": updated["username"],
                                "name": updated["name"],
                                "role": updated["role"],
                                "global_role": updated["global_role"],
                                "allowed_tenants": updated["allowed_tenants"],
                                "tenant_scopes": updated["tenant_scopes"],
                            }
                        st.success("Usuario actualizado.")
                        st.rerun()

            st.caption("Eliminar usuario")
            delete_confirm = st.text_input(
                "Escribe el username para confirmar eliminación",
                value="",
                key=f"adm_edit_delete_confirm_{selected_username}",
            )
            if delete_col.button(
                "Eliminar usuario",
                key=f"adm_edit_delete_{selected_username}",
                width="stretch",
            ):
                errors: list[str] = []
                current_username = str(auth_user.get("username", "")).strip().lower()
                if selected_username == current_username:
                    errors.append("No puedes eliminar tu propio usuario en sesión.")
                if delete_confirm.strip().lower() != selected_username:
                    errors.append("Confirmación inválida: escribe exactamente el username.")
                if bool(user.get("enabled", True)) and _user_record_is_admin(user) and _enabled_admin_count(users) <= 1:
                    errors.append("No puedes eliminar el último admin activo.")
                if errors:
                    for err in errors:
                        st.error(err)
                else:
                    users_next = dict(users)
                    users_next.pop(selected_username, None)
                    ok, err_msg = save_users_config(USERS_CONFIG_PATH, users_next)
                    if not ok:
                        st.error(f"No se pudo guardar config/users.json: {err_msg}")
                    else:
                        append_admin_audit(
                            "user_deleted",
                            str(auth_user.get("username", "unknown")),
                            target=selected_username,
                            details={},
                        )
                        st.success(f"Usuario '{selected_username}' eliminado.")
                        st.rerun()

    with tab_create:
        create_username_raw = st.text_input(
            "Username",
            value="",
            key="adm_create_username",
            help="Solo minúsculas, números, punto, guión o guión bajo.",
        )
        create_name = st.text_input("Nombre", value="", key="adm_create_name")
        col_create_1, col_create_2 = st.columns(2)
        with col_create_1:
            create_global_role = st.selectbox(
                "Rol global",
                options=global_role_options,
                index=0,
                key="adm_create_global_role",
            )
        with col_create_2:
            create_role = st.selectbox(
                "Rol base (legacy)",
                options=role_options,
                index=0,
                key="adm_create_role",
            )
        create_enabled = st.toggle("Usuario activo", value=True, key="adm_create_enabled")
        create_tenants = st.multiselect(
            "Tenants asignados",
            options=tenant_options,
            default=["*"] if create_global_role == "admin" else [],
            format_func=lambda t: "Todos (*)" if t == "*" else str(tenants.get(t, {}).get("name", t)),
            key="adm_create_tenants",
        )
        create_selected = _normalize_tenant_selection(create_tenants)
        if create_global_role == "admin" and not create_selected:
            create_selected = ["*"]
        create_scope_roles: dict[str, str] = {}
        if create_selected:
            st.caption("Roles por tenant")
            for tenant_id in create_selected:
                safe_tenant = _widget_safe_key(tenant_id)
                create_scope_roles[tenant_id] = st.selectbox(
                    f"Rol para {'Todos (*)' if tenant_id == '*' else tenants.get(tenant_id, {}).get('name', tenant_id)}",
                    options=role_options,
                    index=role_options.index(create_role) if create_role in role_options else 0,
                    key=f"adm_create_scope_role_{safe_tenant}",
                )

        create_pwd_col_1, create_pwd_col_2 = st.columns(2)
        with create_pwd_col_1:
            create_password = st.text_input(
                "Contraseña",
                type="password",
                key="adm_create_password",
            )
        with create_pwd_col_2:
            create_password_confirm = st.text_input(
                "Confirmar contraseña",
                type="password",
                key="adm_create_password_confirm",
            )

        if st.button("Crear usuario", key="adm_create_submit", type="primary", width="stretch"):
            errors: list[str] = []
            create_username = str(create_username_raw).strip().lower()
            valid_chars = set("abcdefghijklmnopqrstuvwxyz0123456789._-")
            if not create_username:
                errors.append("El username es obligatorio.")
            elif any(ch not in valid_chars for ch in create_username):
                errors.append("El username contiene caracteres no permitidos.")
            elif create_username in users:
                errors.append("Ese username ya existe.")

            clean_name = str(create_name).strip()
            if not clean_name:
                clean_name = create_username
            if len(create_password) < 8:
                errors.append("La contraseña debe tener al menos 8 caracteres.")
            if create_password != create_password_confirm:
                errors.append("La contraseña y su confirmación no coinciden.")
            if create_global_role != "admin" and not create_selected:
                errors.append("Debes asignar al menos un tenant.")

            if errors:
                for err in errors:
                    st.error(err)
            else:
                allowed_tenants, tenant_scopes = _build_tenant_access(
                    create_selected,
                    create_scope_roles,
                    create_role,
                )
                salt = _new_password_salt()
                users_next = dict(users)
                users_next[create_username] = {
                    "username": create_username,
                    "name": clean_name,
                    "global_role": create_global_role,
                    "role": create_role,
                    "enabled": bool(create_enabled),
                    "allowed_tenants": allowed_tenants,
                    "tenant_scopes": tenant_scopes,
                    "password_salt": salt,
                    "password_hash": _hash_password_with_salt(create_password, salt),
                }
                ok, err_msg = save_users_config(USERS_CONFIG_PATH, users_next)
                if not ok:
                    st.error(f"No se pudo guardar config/users.json: {err_msg}")
                else:
                    append_admin_audit(
                        "user_created",
                        str(auth_user.get("username", "unknown")),
                        target=create_username,
                        details={
                            "enabled": bool(create_enabled),
                            "global_role": create_global_role,
                            "role": create_role,
                            "allowed_tenants": allowed_tenants,
                        },
                    )
                    st.success(f"Usuario '{create_username}' creado.")
                    st.rerun()

    with tab_dashboard:
        scope_options = ["__defaults__"] + sorted(tenants.keys())
        target_scope = st.selectbox(
            "Configurar scope",
            options=scope_options,
            format_func=lambda x: "Defaults (global)" if x == "__defaults__" else str(tenants.get(x, {}).get("name", x)),
            key="adm_dash_scope",
        )
        if target_scope == "__defaults__":
            base_cfg = dashboard_settings.get("defaults", {})
        else:
            base_cfg = tenant_dashboard_settings(dashboard_settings, target_scope)

        overview_defaults = _normalize_kpi_keys(base_cfg.get("overview_kpis", DEFAULT_OVERVIEW_KPI_KEYS), DEFAULT_OVERVIEW_KPI_KEYS)
        traffic_defaults = _normalize_kpi_keys(base_cfg.get("traffic_kpis", DEFAULT_TRAFFIC_KPI_KEYS), DEFAULT_TRAFFIC_KPI_KEYS)
        overview_sections_defaults = _normalize_section_keys(
            base_cfg.get("overview_sections", DEFAULT_OVERVIEW_SECTION_KEYS),
            OVERVIEW_SECTION_OPTIONS,
            DEFAULT_OVERVIEW_SECTION_KEYS,
        )
        traffic_sections_defaults = _normalize_section_keys(
            base_cfg.get("traffic_sections", DEFAULT_TRAFFIC_SECTION_KEYS),
            TRAFFIC_SECTION_OPTIONS,
            DEFAULT_TRAFFIC_SECTION_KEYS,
        )
        default_platform = _normalize_platform_option(base_cfg.get("default_platform", "All"))
        default_view_mode = _normalize_view_mode_option(base_cfg.get("default_view_mode", "Overview"))
        default_token_health = _coerce_bool(base_cfg.get("show_sidebar_meta_token_health", True), default=True)
        kpi_options = list(KPI_CATALOG.keys())
        overview_section_options = list(OVERVIEW_SECTION_OPTIONS.keys())
        traffic_section_options = list(TRAFFIC_SECTION_OPTIONS.keys())

        overview_selected = st.multiselect(
            "KPIs para Overview",
            options=kpi_options,
            default=overview_defaults,
            format_func=lambda k: str(KPI_CATALOG.get(k, {}).get("label", k)),
            key=f"adm_dash_overview_{target_scope}",
            help="Selecciona entre 1 y 6 KPIs para las tarjetas superiores del Overview.",
        )
        traffic_selected = st.multiselect(
            "KPIs para Tráfico y Adquisición",
            options=kpi_options,
            default=traffic_defaults,
            format_func=lambda k: str(KPI_CATALOG.get(k, {}).get("label", k)),
            key=f"adm_dash_traffic_{target_scope}",
            help="Selecciona entre 1 y 6 KPIs para las tarjetas superiores de Tráfico.",
        )
        overview_sections_selected = st.multiselect(
            "Secciones Overview",
            options=overview_section_options,
            default=overview_sections_defaults,
            format_func=lambda k: str(OVERVIEW_SECTION_OPTIONS.get(k, k)),
            key=f"adm_dash_overview_sections_{target_scope}",
        )
        traffic_sections_selected = st.multiselect(
            "Secciones Tráfico y Adquisición",
            options=traffic_section_options,
            default=traffic_sections_defaults,
            format_func=lambda k: str(TRAFFIC_SECTION_OPTIONS.get(k, k)),
            key=f"adm_dash_traffic_sections_{target_scope}",
        )
        cfg_col_1, cfg_col_2 = st.columns(2)
        with cfg_col_1:
            selected_platform = st.selectbox(
                "Plataforma por defecto",
                options=list(PLATFORM_OPTIONS),
                index=list(PLATFORM_OPTIONS).index(default_platform) if default_platform in PLATFORM_OPTIONS else 0,
                key=f"adm_dash_platform_{target_scope}",
            )
        with cfg_col_2:
            selected_view_mode = st.selectbox(
                "Vista por defecto",
                options=list(VIEW_MODE_OPTIONS),
                index=list(VIEW_MODE_OPTIONS).index(default_view_mode) if default_view_mode in VIEW_MODE_OPTIONS else 0,
                key=f"adm_dash_view_{target_scope}",
            )
        show_sidebar_token = st.toggle(
            "Mostrar Meta Token Health en sidebar",
            value=default_token_health,
            key=f"adm_dash_sidebar_token_{target_scope}",
        )

        save_dashboard_button = st.button(
            "Guardar Variables Dashboard",
            key=f"adm_dash_save_{target_scope}",
            type="primary",
            width="stretch",
        )
        if save_dashboard_button:
            errors: list[str] = []
            overview_norm = _normalize_kpi_keys(overview_selected, DEFAULT_OVERVIEW_KPI_KEYS)
            traffic_norm = _normalize_kpi_keys(traffic_selected, DEFAULT_TRAFFIC_KPI_KEYS)
            overview_sections_norm = _normalize_section_keys(
                overview_sections_selected,
                OVERVIEW_SECTION_OPTIONS,
                DEFAULT_OVERVIEW_SECTION_KEYS,
            )
            traffic_sections_norm = _normalize_section_keys(
                traffic_sections_selected,
                TRAFFIC_SECTION_OPTIONS,
                DEFAULT_TRAFFIC_SECTION_KEYS,
            )
            if not overview_norm:
                errors.append("Debes seleccionar al menos 1 KPI para Overview.")
            if not traffic_norm:
                errors.append("Debes seleccionar al menos 1 KPI para Tráfico y Adquisición.")
            if not overview_sections_norm:
                errors.append("Debes seleccionar al menos 1 sección para Overview.")
            if not traffic_sections_norm:
                errors.append("Debes seleccionar al menos 1 sección para Tráfico y Adquisición.")
            if errors:
                for err in errors:
                    st.error(err)
            else:
                updated_settings = load_dashboard_settings(DASHBOARD_SETTINGS_PATH, tenants)
                cfg_payload = {
                    "overview_kpis": overview_norm,
                    "traffic_kpis": traffic_norm,
                    "overview_sections": overview_sections_norm,
                    "traffic_sections": traffic_sections_norm,
                    "default_platform": _normalize_platform_option(selected_platform),
                    "default_view_mode": _normalize_view_mode_option(selected_view_mode),
                    "show_sidebar_meta_token_health": bool(show_sidebar_token),
                }
                if target_scope == "__defaults__":
                    updated_settings["defaults"] = cfg_payload
                else:
                    tenants_cfg = updated_settings.get("tenants", {})
                    if not isinstance(tenants_cfg, dict):
                        tenants_cfg = {}
                    tenants_cfg[target_scope] = cfg_payload
                    updated_settings["tenants"] = tenants_cfg
                ok, err_msg = save_dashboard_settings(DASHBOARD_SETTINGS_PATH, updated_settings, tenants)
                if not ok:
                    st.error(f"No se pudo guardar {DASHBOARD_SETTINGS_PATH.name}: {err_msg}")
                else:
                    append_admin_audit(
                        "dashboard_settings_updated",
                        str(auth_user.get("username", "unknown")),
                        target="defaults" if target_scope == "__defaults__" else target_scope,
                        details={
                            "overview_kpis": overview_norm,
                            "traffic_kpis": traffic_norm,
                            "overview_sections": overview_sections_norm,
                            "traffic_sections": traffic_sections_norm,
                            "default_platform": _normalize_platform_option(selected_platform),
                            "default_view_mode": _normalize_view_mode_option(selected_view_mode),
                            "show_sidebar_meta_token_health": bool(show_sidebar_token),
                        },
                    )
                    st.success("Variables de dashboard guardadas.")
                    st.rerun()

        st.caption(f"Fuente: {DASHBOARD_SETTINGS_PATH.name} | Scope: {'defaults' if target_scope == '__defaults__' else target_scope}")

    with tab_audit:
        st.markdown("### Salud de Configuración")
        user_issues = validate_users_integrity(users, tenants)
        settings_snapshot = load_dashboard_settings(DASHBOARD_SETTINGS_PATH, tenants)
        settings_issues = validate_dashboard_settings_integrity(settings_snapshot, tenants)
        all_issues = user_issues + settings_issues
        if all_issues:
            st.error(f"Se detectaron {len(all_issues)} issue(s) de integridad.")
            for issue in all_issues:
                st.write(f"- {issue}")
        else:
            st.success("Integridad OK en usuarios y dashboard settings.")

        st.markdown("### Log de Cambios (Admin)")
        audit_rows = read_admin_audit(limit=300)
        if not audit_rows:
            st.info("Sin eventos en auditoría todavía.")
        else:
            view_rows: list[dict[str, Any]] = []
            for row in audit_rows:
                view_rows.append(
                    {
                        "timestamp_utc": str(row.get("timestamp_utc", "")),
                        "action": str(row.get("action", "")),
                        "actor": str(row.get("actor", "")),
                        "target": str(row.get("target", "")),
                        "details": json.dumps(row.get("details", {}), ensure_ascii=False),
                    }
                )
            st.dataframe(pd.DataFrame(view_rows), width="stretch", hide_index=True)
            st.caption(f"Fuente: {ADMIN_AUDIT_LOG_PATH.name}")
            download_payload = "\n".join(json.dumps(r, ensure_ascii=True) for r in audit_rows) + "\n"
            st.download_button(
                "Descargar auditoría (jsonl)",
                data=download_payload,
                file_name=f"admin_audit_{date.today().isoformat()}.jsonl",
                mime="application/json",
                width="stretch",
            )

    st.caption("Fase 6 hardening: layout dinámico + auditoría + backups automáticos + chequeos de integridad.")


def main() -> None:
    st.set_page_config(
        page_title="iPalmera IA Analítica",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    apply_theme()

    users = load_users_config(USERS_CONFIG_PATH)
    auth_user = _ensure_authenticated(users)

    tenants = load_tenants_config(TENANTS_CONFIG_PATH)
    dashboard_settings = load_dashboard_settings(DASHBOARD_SETTINGS_PATH, tenants)
    tenant_id, view_mode = render_sidebar(tenants)
    tenant_cfg = tenants.get(tenant_id) or next(iter(tenants.values()))
    tenant_dash_cfg = tenant_dashboard_settings(dashboard_settings, tenant_id)
    desired_view_mode = _normalize_view_mode_option(tenant_dash_cfg.get("default_view_mode", "Overview"))
    last_view_tenant = str(st.session_state.get("sidebar_view_tenant_cfg", ""))
    if last_view_tenant != tenant_id:
        st.session_state["sidebar_view_tenant_cfg"] = tenant_id
        if str(st.session_state.get("sidebar_view_mode", "")) != desired_view_mode:
            st.session_state["sidebar_view_mode"] = desired_view_mode
            st.rerun()
    view_mode = str(st.session_state.get("sidebar_view_mode", desired_view_mode))
    tenant_name = str(tenant_cfg.get("name", tenant_id))
    tenant_meta_account_id = str(
        tenant_cfg.get("meta_account_id", tenant_cfg.get("meta_ad_account_id", ""))
    ).strip()
    tenant_google_customer_id = str(
        tenant_cfg.get("google_customer_id", tenant_cfg.get("google_ads_customer_id", ""))
    ).strip()
    ga4_conversion_event_name = (
        str(tenant_cfg.get("ga4_conversion_event_name", GA4_GTC_SOLICITAR_CODIGO_EVENT)).strip()
        or GA4_GTC_SOLICITAR_CODIGO_EVENT
    )
    if view_mode == "Administración":
        if not _auth_user_is_admin(auth_user):
            st.error("Tu usuario no tiene permiso para Administración.")
            st.stop()
        render_admin_panel(users, tenants, auth_user, dashboard_settings)
        st.caption(
            f"Cliente: {tenant_name} ({tenant_id}) | Vista: {view_mode} | "
            f"Fuente usuarios: {USERS_CONFIG_PATH.name} | Variables: {DASHBOARD_SETTINGS_PATH.name}"
        )
        st.markdown(
            """
            <div class="desktop-powered-footer">
              POWERED BY <a href="https://www.ipalmera.com" target="_blank">iPalmera</a> 2026
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    report_path = Path(str(tenant_cfg.get("report_path", REPORT_PATH)))
    try:
        report = load_report(report_path)
    except Exception as exc:
        st.error(f"No se pudo cargar el reporte para '{tenant_name}': {exc}")
        st.stop()
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
    s, e, platform = render_top_filters(
        min_d,
        max_d,
        tenant_name,
        tenant_id,
        str(tenant_dash_cfg.get("default_platform", "All")),
    )

    df_sel = df[(df["date"] >= s) & (df["date"] <= e)].copy()
    period_days = max((e - s).days + 1, 1)
    prev_e = s - timedelta(days=1)
    prev_s = prev_e - timedelta(days=period_days - 1)
    df_prev = df[(df["date"] >= prev_s) & (df["date"] <= prev_e)].copy()
    overview_sections = _normalize_section_keys(
        tenant_dash_cfg.get("overview_sections", DEFAULT_OVERVIEW_SECTION_KEYS),
        OVERVIEW_SECTION_OPTIONS,
        DEFAULT_OVERVIEW_SECTION_KEYS,
    )
    traffic_sections = _normalize_section_keys(
        tenant_dash_cfg.get("traffic_sections", DEFAULT_TRAFFIC_SECTION_KEYS),
        TRAFFIC_SECTION_OPTIONS,
        DEFAULT_TRAFFIC_SECTION_KEYS,
    )

    if view_mode == "Tráfico y Adquisición":
        render_traffic(
            df_sel,
            df_prev,
            ch,
            pg,
            camp_all,
            platform,
            s,
            e,
            _normalize_kpi_keys(
                tenant_dash_cfg.get("traffic_kpis", DEFAULT_TRAFFIC_KPI_KEYS),
                DEFAULT_TRAFFIC_KPI_KEYS,
            ),
            traffic_sections,
        )
    else:
        if "daily_fact" in set(overview_sections):
            render_daily_fact(df_sel, platform)
        render_exec(
            df_sel,
            df_prev,
            platform,
            _normalize_kpi_keys(
                tenant_dash_cfg.get("overview_kpis", DEFAULT_OVERVIEW_KPI_KEYS),
                DEFAULT_OVERVIEW_KPI_KEYS,
            ),
            overview_sections,
            paid_dev,
            camp_all,
            ga4_event_daily,
            ga4_conversion_event_name,
            tenant_meta_account_id,
            tenant_google_customer_id,
            s,
            e,
            prev_s,
            prev_e,
        )

    if _coerce_bool(tenant_dash_cfg.get("show_sidebar_meta_token_health", True), default=True):
        render_sidebar_meta_token_health(report)

    st.caption(
        f"Cliente: {tenant_name} ({tenant_id}) | Vista: {view_mode} | Plataforma: {platform} | "
        f"Fuente: {report_path.name} | Datos: {min_d.isoformat()} a {max_d.isoformat()}"
    )
    st.markdown(
        """
        <div class="desktop-powered-footer">
          POWERED BY <a href="https://www.ipalmera.com" target="_blank">iPalmera</a> 2026
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()



