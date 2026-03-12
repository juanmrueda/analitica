
#!/usr/bin/env python3
from __future__ import annotations

import json
import html
import hashlib
import math
import calendar
import os
import re
import secrets
import shutil
import base64
import textwrap
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
import urllib.error
import urllib.request

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from PIL import Image
import dashboard_data
import dashboard_filters
from coco_agent import run_coco_agent_turn
from coco_agent import deterministic_resolvers as coco_det
from coco_agent import workflow as coco_workflow
from coco_agent import context_builder as coco_context

BASE_DIR = Path(__file__).resolve().parent
REPORT_PATH = BASE_DIR / "reports" / "yap" / "yap_historical.json"
TENANTS_CONFIG_PATH = BASE_DIR / "config" / "tenants.json"
USERS_CONFIG_PATH = BASE_DIR / "config" / "users.json"
DASHBOARD_SETTINGS_PATH = BASE_DIR / "config" / "dashboard_settings.json"
DASHBOARD_SETTINGS_TEMPLATE_PATH = BASE_DIR / "config" / "dashboard_settings.template.json"
CONFIG_BACKUP_DIR = BASE_DIR / "config" / "backups"
ADMIN_AUDIT_LOG_PATH = BASE_DIR / "config" / "admin_audit.jsonl"
AI_USAGE_LOG_PATH = BASE_DIR / "config" / "ai_usage.jsonl"
COCO_CHAT_MEMORY_LOG_PATH = BASE_DIR / "config" / "coco_chat_memory.jsonl"
COCO_CHAT_STATE_PATH = BASE_DIR / "config" / "coco_chat_state.json"
TENANT_LOGOS_DIR = BASE_DIR / "assets" / "logos"
DEFAULT_TENANT_ID = "yap"
LOGO_PATH = BASE_DIR / "assets" / "logo-ipalmera-growth-marketing.webp"
LOGO_PLACEHOLDER = "https://via.placeholder.com/260x80/F8FAFC/0F172A?text=iPalmera+Logo"
TENANT_LOGO_UPLOAD_WIDTH_PX = 520
TENANT_LOGO_UPLOAD_HEIGHT_PX = 160
SIDEBAR_LOGO_RENDER_WIDTH_PX = 228
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
THEME_COLOR_KEYS: tuple[str, ...] = ("google", "meta", "accent", "neutral", "success", "danger")
DEFAULT_THEME_COLORS: dict[str, str] = {
    "google": C_GOOGLE,
    "meta": C_META,
    "accent": C_ACCENT,
    "neutral": C_MUTE,
    "success": "#67B22D",
    "danger": "#D14343",
}
ACTIVE_THEME_COLORS: dict[str, str] = dict(DEFAULT_THEME_COLORS)

VIEW_MODE_OPTIONS = ("Overview", "Tráfico y Adquisición")
PLATFORM_OPTIONS = ("All", "Google", "Meta")
REPORT_PARQUET_DIRNAME = "dashboard"
PARQUET_DAILY_DATASET = "daily"
PARQUET_HOURLY_DATASET = "hourly"
PARQUET_ACQ_DATASETS: tuple[str, ...] = (
    "ga4_channel_daily",
    "ga4_top_pages_daily",
    "ga4_event_daily",
    "meta_campaign_daily",
    "google_campaign_daily",
    "paid_piece_daily",
    "paid_device_daily",
    "paid_lead_demographics_daily",
    "paid_lead_geo_daily",
)
PARQUET_CAMPAIGN_UNIFIED_DATASET = "campaign_unified_daily"
PARQUET_PIECE_ENRICHED_DATASET = "paid_piece_enriched_daily"
PARQUET_DERIVED_DATASETS: tuple[str, ...] = (
    PARQUET_CAMPAIGN_UNIFIED_DATASET,
    PARQUET_PIECE_ENRICHED_DATASET,
)
PARQUET_CORE_DATASETS: tuple[str, ...] = (
    PARQUET_DAILY_DATASET,
    PARQUET_HOURLY_DATASET,
    *PARQUET_ACQ_DATASETS,
    *PARQUET_DERIVED_DATASETS,
)
# Grace period between JSON mtime and parquet mtime before marking stale.
PARQUET_STALE_TOLERANCE_NS = int(5 * 60 * 1_000_000_000)
DATE_PRESET_OPTIONS = (
    "custom",
    "today",
    "yesterday",
    "this_week_to_date",
    "last_7_days",
    "last_30_days",
    "this_month_to_date",
    "last_month",
    "year_to_date",
    "last_calendar_year",
)
DATE_PRESET_LABELS: dict[str, str] = {
    "custom": "Personalizado",
    "today": "Hoy",
    "yesterday": "Ayer",
    "this_week_to_date": "Esta semana (De dom. a hoy)",
    "last_7_days": "Los ultimos 7 dias",
    "last_30_days": "Los ultimos 30 dias",
    "this_month_to_date": "Este mes",
    "last_month": "El mes pasado",
    "year_to_date": "Este año hasta hoy",
    "last_calendar_year": "El año pasado",
}
COMPARE_MODE_OPTIONS = ("previous_period", "year_over_year", "custom")
COMPARE_MODE_LABELS: dict[str, str] = {
    "previous_period": "Periodo anterior",
    "year_over_year": "Mismo periodo año anterior",
    "custom": "Personalizado",
}
DEFAULT_OVERVIEW_KPI_KEYS = ["spend", "conv", "cpl", "cvr", "cpm", "cpc"]
DEFAULT_TRAFFIC_KPI_KEYS = ["sessions", "users", "avg_sess", "bounce"]
DEFAULT_OVERVIEW_SECTION_KEYS = [
    "kpis",
    "trend_chart",
    "media_mix",
    "lead_demographics",
    "lead_geo_map",
    "funnel",
    "ga4_conversion",
    "device_breakdown",
    "audit_table",
    "top_pieces",
    "daily_fact",
]
DEFAULT_TRAFFIC_SECTION_KEYS = ["kpis", "channels", "top_pages", "campaigns"]
DEFAULT_CAMPAIGN_FILTER_KEYS: list[str] = []

OVERVIEW_SECTION_OPTIONS: dict[str, str] = {
    "kpis": "Tarjetas KPI",
    "trend_chart": "Gráfica Performance",
    "media_mix": "Mix y Eficiencia Paid",
    "lead_demographics": "Distribución Leads Paid: Edad y Género",
    "lead_geo_map": "Distribución Leads Paid: Mapa",
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
CAMPAIGN_FILTER_OPTIONS: dict[str, str] = {
    "advertising_channel_type": "Google: Channel Type",
    "advertising_channel_sub_type": "Google: Channel SubType",
    "bidding_strategy_type": "Google: Bidding Strategy",
}
ADMIN_SECTION_OPTIONS: dict[str, str] = {
    "users": "Usuarios",
    "coco_ia": "COCO IA",
    "dashboard": "Variables Dashboard",
    "audit": "Auditoría",
}
ADMIN_SECTION_MENU_LABELS: dict[str, str] = {
    "users": "Usuarios",
    "coco_ia": "COCO IA",
    "dashboard": "Variables Dashboard",
    "audit": "Auditoría",
}
AGE_BUCKET_ORDER = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+", "Unknown"]
GENDER_BUCKET_ORDER = ["Female", "Male", "Unknown"]
COUNTRY_CODE_TO_NAME: dict[str, str] = {
    "HN": "Honduras",
    "NI": "Nicaragua",
    "SV": "El Salvador",
    "GT": "Guatemala",
    "CR": "Costa Rica",
    "PA": "Panama",
    "CO": "Colombia",
    "MX": "Mexico",
    "US": "United States",
    "ES": "Spain",
}

COCO_DEFAULT_MODEL = "gpt-4o-mini"
COCO_DEFAULT_PROVIDER = "openai"
COCO_DEFAULT_MAX_DAILY_QUERIES = 20
# Editable from Admin > COCO IA. Kept as defaults in case config is missing.
COCO_DEFAULT_INPUT_COST_PER_1M = 0.15
COCO_DEFAULT_OUTPUT_COST_PER_1M = 0.60
COCO_DEFAULT_DAILY_BUDGET_USD = 0.0
COCO_DAILY_BUDGET_ALERT_RATIO = 0.80
COCO_DAILY_QUERY_ALERT_RATIO = 0.80
COCO_DEFAULT_SCOPE_MODE = "total"
COCO_HISTORY_PERSIST_LIMIT = 80
COCO_HISTORY_FOR_MODEL_LIMIT = 24
COCO_HISTORY_MSG_MAX_CHARS = 1200
COCO_MEMORY_SUMMARY_MAX_CHARS = 2200
COCO_CHAT_ICON = "\U0001F4AC"
SPANISH_MONTHS: dict[str, int] = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
WEEKDAY_NAMES_ES: dict[int, str] = {
    0: "lunes",
    1: "martes",
    2: "miercoles",
    3: "jueves",
    4: "viernes",
    5: "sabado",
    6: "domingo",
}
MONTH_NAMES_ES: dict[int, str] = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
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
    "cvr": {"label": "CVR", "fmt": "pct", "delta_mode": "direct", "delta_color": "normal"},
    "cpc": {"label": "CPC", "fmt": "money", "delta_mode": "direct", "delta_color": "inverse"},
    "cpm": {"label": "CPM", "fmt": "money", "delta_mode": "direct", "delta_color": "inverse"},
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


def piece_platform_link(
    platform: str,
    *,
    piece_id: Any,
    campaign_id: Any = "",
    meta_account_id: str = "",
    google_customer_id: str = "",
) -> str:
    piece = str(piece_id or "").strip()
    plat = str(platform or "").strip().lower()
    if plat == "meta" and piece:
        act = str(meta_account_id or "").strip()
        if act and not act.startswith("act_"):
            act = f"act_{act}"
        if act:
            return (
                "https://adsmanager.facebook.com/adsmanager/manage/ads"
                f"?act={act}&selected_ad_ids={piece}"
            )
        return f"https://adsmanager.facebook.com/adsmanager/manage/ads?selected_ad_ids={piece}"
    if str(campaign_id or "").strip():
        return campaign_platform_link(
            platform,
            campaign_id,
            meta_account_id=meta_account_id,
            google_customer_id=google_customer_id,
        )
    if plat == "google":
        ocid = _digits_only(google_customer_id)
        if ocid:
            return f"https://ads.google.com/aw/campaigns?ocid={ocid}"
        return "https://ads.google.com/aw/campaigns"
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
            border: 1px solid rgba(32,29,29,0.10) !important;
            background: rgba(255,255,255,0.86) !important;
            color: #4D627F !important;
            box-shadow: 0 8px 18px rgba(15,23,42,0.10) !important;
          }
          [data-testid="collapsedControl"] button:hover,
          [data-testid="stSidebarCollapseButton"] button:hover {
            background: linear-gradient(180deg, rgba(123,204,53,0.26) 0%, rgba(123,204,53,0.20) 100%) !important;
            border-color: rgba(103,178,45,0.58) !important;
            color: #1F4D0A !important;
          }
          [data-testid="collapsedControl"] button svg,
          [data-testid="stSidebarCollapseButton"] button svg {
            fill: #4D627F !important;
            stroke: #4D627F !important;
          }
          [data-testid="collapsedControl"] button:hover svg,
          [data-testid="stSidebarCollapseButton"] button:hover svg {
            fill: #1F4D0A !important;
            stroke: #1F4D0A !important;
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
          [data-testid="stMain"] [data-testid="stPopover"] button {
            white-space: nowrap !important;
            min-height: 2.55rem !important;
            border-radius: 14px !important;
            border: 1px solid rgba(32,29,29,0.12) !important;
            background: rgba(246,248,252,0.96) !important;
            color: #3d4f67 !important;
            font-size: 0.92rem !important;
            font-weight: 700 !important;
            box-shadow: 0 7px 18px rgba(15,23,42,0.06) !important;
          }
          [data-testid="stMain"] [data-testid="stPopover"] button p {
            white-space: nowrap !important;
            color: #3d4f67 !important;
            font-size: 0.92rem !important;
            font-weight: 700 !important;
          }
          [data-baseweb="popover"] [data-testid="stPopoverContent"] {
            border-radius: 24px !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            background: linear-gradient(180deg, #F7F8FB 0%, #F3F5F9 100%) !important;
            box-shadow: 0 26px 44px rgba(15,23,42,0.16) !important;
            padding: 0.95rem 1rem 1rem 1rem !important;
            min-width: min(360px, calc(100vw - 1.4rem)) !important;
          }
          [data-baseweb="popover"] .range-modal-header {
            margin-bottom: 0.45rem;
          }
          [data-baseweb="popover"] .range-modal-kicker {
            font-size: 0.62rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-weight: 800;
            color: #94a3b8;
            margin-bottom: 0.4rem;
          }
          [data-baseweb="popover"] .range-modal-selected {
            border: 1px solid rgba(32,29,29,0.10);
            border-radius: 13px;
            background: rgba(240,243,248,0.96);
            padding: 0.62rem 0.8rem;
            color: #344a64;
            font-size: 0.92rem;
            font-weight: 800;
            line-height: 1.25;
          }
          [data-baseweb="popover"] .range-modal-hint {
            margin-top: 0.42rem;
            color: #9ba8ba;
            font-size: 0.74rem;
            font-style: italic;
            font-weight: 600;
          }
          [data-baseweb="popover"] .stRadio > label {
            display: none !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] {
            display: grid !important;
            grid-template-columns: 1fr !important;
            gap: 0.14rem !important;
            background: transparent !important;
            border: none !important;
            border-radius: 0 !important;
            min-height: auto !important;
            padding: 0.08rem 0 0.36rem 0 !important;
            width: 100% !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label {
            margin: 0 !important;
            padding: 0.36rem 0.12rem !important;
            border: none !important;
            border-radius: 12px !important;
            min-height: 1.8rem !important;
            justify-content: flex-start !important;
            color: #5a6d86 !important;
            font-weight: 600 !important;
            background: transparent !important;
            display: flex !important;
            gap: 0.54rem !important;
            align-items: center !important;
            transition: background 0.18s ease !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label::before,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label::before {
            content: "";
            width: 1.03rem;
            height: 1.03rem;
            border-radius: 999px;
            border: 1.8px solid rgba(122,135,157,0.38);
            background: #f9fbff;
            box-sizing: border-box;
            flex: 0 0 auto;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label > div:first-child,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label > div:first-child {
            position: absolute !important;
            opacity: 0 !important;
            pointer-events: none !important;
            width: 1px !important;
            height: 1px !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label [data-testid="stMarkdownContainer"],
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label [data-testid="stMarkdownContainer"] {
            width: auto !important;
            text-align: left !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label [data-testid="stMarkdownContainer"] p,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label [data-testid="stMarkdownContainer"] p {
            margin: 0 !important;
            color: #5a6d86 !important;
            font-size: 0.9rem !important;
            font-weight: 600 !important;
            line-height: 1.25 !important;
            text-align: left !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label[data-checked="true"],
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label[aria-checked="true"],
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label:has(input:checked),
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label[data-checked="true"],
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label[aria-checked="true"],
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label:has(input:checked) {
            background: rgba(123,204,53,0.10) !important;
            color: #1f4d0a !important;
            font-weight: 800 !important;
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label[data-checked="true"]::before,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label[aria-checked="true"]::before,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label:has(input:checked)::before,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label[data-checked="true"]::before,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label[aria-checked="true"]::before,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label:has(input:checked)::before {
            border-color: rgba(103,178,45,0.95) !important;
            background: radial-gradient(circle at center, #7bcc35 46%, #ffffff 48%);
          }
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label[data-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label[aria-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > label:has(input:checked) [data-testid="stMarkdownContainer"] p,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label[data-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label[aria-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-baseweb="popover"] .stRadio [role="radiogroup"] > div > label:has(input:checked) [data-testid="stMarkdownContainer"] p {
            color: #2a3a52 !important;
            font-weight: 800 !important;
          }
          [data-baseweb="popover"] .stDateInput > label,
          [data-baseweb="popover"] .stSelectbox > label {
            font-size: 0.66rem !important;
            letter-spacing: 0.11em !important;
            text-transform: uppercase !important;
            font-weight: 800 !important;
            color: #9aa9be !important;
            margin-top: 0.38rem !important;
            margin-bottom: 0.24rem !important;
          }
          [data-baseweb="popover"] .stDateInput [data-baseweb="input"],
          [data-baseweb="popover"] .stSelectbox [data-baseweb="select"] > div {
            min-height: 2.55rem !important;
            border-radius: 13px !important;
            border: 1px solid rgba(32,29,29,0.10) !important;
            background: rgba(240,243,248,0.98) !important;
            box-shadow: none !important;
          }
          [data-baseweb="popover"] .stDateInput input,
          [data-baseweb="popover"] .stSelectbox [data-baseweb="select"] span {
            color: #3e526d !important;
            font-size: 0.92rem !important;
            font-weight: 700 !important;
          }
          [data-baseweb="popover"] hr {
            margin: 0.72rem 0 0.58rem 0 !important;
            border-top: 1px solid rgba(122,135,157,0.20) !important;
          }
          [data-baseweb="popover"] [data-testid="stButton"] button {
            min-height: 2.65rem !important;
            border-radius: 14px !important;
            border: 1px solid rgba(32,29,29,0.10) !important;
            background: rgba(255,255,255,0.86) !important;
            color: #6b7d96 !important;
            font-weight: 800 !important;
          }
          [data-baseweb="popover"] [data-testid="stButton"] button[kind="primary"] {
            border-color: rgba(103,178,45,0.72) !important;
            background: linear-gradient(180deg, #7bcc35 0%, #67b22d 100%) !important;
            color: #ffffff !important;
            box-shadow: 0 9px 18px rgba(103,178,45,0.34) !important;
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
            padding: 0.18rem 0.8rem 0.85rem 0.8rem;
          }
          [data-testid="stSidebar"] [data-testid="stImage"] {
            display: flex;
            justify-content: center;
            margin: -0.22rem 0 0.62rem 0;
          }
          [data-testid="stSidebar"] [data-testid="stImage"] img {
            width: 228px !important;
            height: 70px !important;
            object-fit: contain !important;
            image-rendering: auto;
          }
          @keyframes sidebarAdminMenuIn {
            from { opacity: 0; transform: translateY(-5px); }
            to { opacity: 1; transform: translateY(0); }
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
          .sidebar-tenant-logo {
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 14px;
            padding: 0.45rem 0.5rem;
            background: rgba(255,255,255,0.72);
            margin-bottom: 0.78rem;
            box-shadow: 0 8px 18px rgba(15,23,42,0.04);
          }
          .sidebar-tenant-logo img {
            width: 100%;
            border-radius: 10px;
            object-fit: contain;
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
            gap: 0.08rem;
            margin-top: 0.02rem;
            margin-bottom: 0.52rem;
            padding-left: 0.22rem;
            animation: sidebarAdminMenuIn 0.24s ease;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label {
            border-radius: 9px;
            border: none !important;
            background: transparent !important;
            padding: 0.24rem 0.28rem !important;
            color: #4f617b !important;
            font-size: 0.9rem;
            font-weight: 600;
            display: flex !important;
            align-items: center !important;
            gap: 0.42rem;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label > div:first-child {
            display: none !important;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label::before {
            content: "";
            width: 0.92rem;
            height: 0.92rem;
            border-radius: 0.26rem;
            border: 1px solid rgba(77,98,127,0.46);
            background: rgba(77,98,127,0.08);
            flex: 0 0 auto;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.62rem;
            font-weight: 800;
            color: #4d627f;
            line-height: 1;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(1)::before { content: "U"; }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(2)::before { content: "D"; }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(3)::before { content: "A"; }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"]::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"]::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked)::before {
            color: #1F4D0A;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label p {
            margin: 0 !important;
            text-align: left !important;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label [data-testid="stMarkdownContainer"] {
            width: 100%;
            text-align: left !important;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked) {
            color: #1F4D0A !important;
            font-weight: 800 !important;
            background: rgba(123,204,53,0.16) !important;
            border-radius: 8px !important;
            box-shadow: inset 2px 0 0 rgba(103,178,45,0.72);
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"] [data-testid="stMarkdownContainer"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"] [data-testid="stMarkdownContainer"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked) [data-testid="stMarkdownContainer"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"] [data-testid="stMarkdownContainer"] *,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"] [data-testid="stMarkdownContainer"] *,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked) [data-testid="stMarkdownContainer"] * {
            font-weight: 800 !important;
            color: #1F4D0A !important;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"]::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"]::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked)::before {
            border-color: rgba(103,178,45,0.78);
            background: linear-gradient(180deg, rgba(123,204,53,0.33) 0%, rgba(123,204,53,0.24) 100%);
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:hover {
            background: rgba(123,204,53,0.12) !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button {
            border-radius: 12px !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            background: rgba(255,255,255,0.65) !important;
            color: #4f617b !important;
            font-weight: 600 !important;
            min-height: 2.9rem !important;
            justify-content: flex-start !important;
            padding-left: 0.78rem !important;
            padding-right: 0.78rem !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button > div {
            width: 100%;
            display: flex !important;
            align-items: center !important;
            justify-content: flex-start !important;
            gap: 0.48rem !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button p {
            margin: 0 !important;
            width: 100%;
            text-align: left !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button svg {
            flex: 0 0 auto;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button[kind="primary"] {
            background: linear-gradient(180deg, rgba(123,204,53,0.30) 0%, rgba(123,204,53,0.24) 100%) !important;
            color: #1F4D0A !important;
            border-color: rgba(103,178,45,0.70) !important;
            box-shadow: 0 4px 12px rgba(103,178,45,0.24) !important;
            font-weight: 800 !important;
          }
          [data-testid="stSidebar"] [data-testid="stButton"] button:hover {
            border-color: rgba(103,178,45,0.40) !important;
            background: rgba(123,204,53,0.16) !important;
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
            text-transform: none !important;
          }
          .hero-title { display: none; }
          .hero-sub {
            color: #4d627f !important;
            font-size: 0.95rem !important;
            font-weight: 600;
            margin-top: 0.2rem;
          }
          .hero-tenant-name {
            color: #2E7D1D !important;
            font-weight: 800 !important;
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
            background: linear-gradient(135deg, #1F2937 0%, #2C3A4E 100%) !important;
            border: 1px solid rgba(148,163,184,0.32) !important;
            border-radius: 24px !important;
            padding: 0.95rem 1.05rem !important;
            margin: 0.12rem 0 0.95rem 0 !important;
            display: flex !important;
            gap: 0.82rem !important;
            align-items: center !important;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.18) !important;
          }
          .daily-fact-icon {
            width: 2.6rem;
            height: 2.6rem;
            border-radius: 14px;
            border: 1px solid rgba(123, 204, 53, 0.28);
            background: rgba(123, 204, 53, 0.16);
            color: #67b22d;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.15rem;
            flex: 0 0 auto;
          }
          .daily-fact .title {
            font-size: 1.2rem;
            line-height: 1.14;
            font-weight: 800;
            letter-spacing: -0.01em;
            color: #F8FAFC;
            text-transform: none;
            margin: 0;
          }
          .daily-fact .body {
            margin-top: 0.16rem;
            font-size: 0.94rem;
            line-height: 1.35;
            font-weight: 500;
            color: #D3DCE8;
          }
          .daily-fact-highlight {
            color: #67b22d;
            font-weight: 800;
          }
          .daily-fact-highlight.neg {
            color: #D14343;
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
          @media (max-width: 960px) {
            .daily-fact {
              border-radius: 16px !important;
              padding: 0.72rem 0.75rem !important;
              gap: 0.58rem !important;
            }
            .daily-fact-icon {
              width: 2.15rem;
              height: 2.15rem;
              border-radius: 12px;
              font-size: 0.95rem;
            }
            .daily-fact .title {
              font-size: 0.95rem;
            }
            .daily-fact .body {
              font-size: 0.79rem;
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
    cur_clicks = float(cur_row[c["clicks"]].sum())
    cur_conv = float(cur_row[c["conv"]].sum())
    prev_spend = float(prev_row[c["spend"]].sum())
    prev_impr = float(prev_row[c["impr"]].sum())
    prev_clicks = float(prev_row[c["clicks"]].sum())
    prev_conv = float(prev_row[c["conv"]].sum())
    cur_cpl = sdiv(cur_spend, cur_conv)
    prev_cpl = sdiv(prev_spend, prev_conv)
    cur_cvr = sdiv(cur_conv, cur_clicks)
    prev_cvr = sdiv(prev_conv, prev_clicks)
    cur_ctr = sdiv(cur_clicks, cur_impr)
    prev_ctr = sdiv(prev_clicks, prev_impr)

    d_impr = pct_delta(cur_impr, prev_impr)
    d_clicks = pct_delta(cur_clicks, prev_clicks)
    d_conv = pct_delta(cur_conv, prev_conv)
    d_cpl = pct_delta(cur_cpl, prev_cpl)
    d_cvr = pct_delta(cur_cvr, prev_cvr)
    d_ctr = pct_delta(cur_ctr, prev_ctr)

    def _pct_html(value: float | None, *, positive_good: bool = True) -> str:
        v = float(value or 0.0)
        cls = "daily-fact-highlight" if (v >= 0) == positive_good else "daily-fact-highlight neg"
        sign = "+" if v >= 0 else ""
        return f"<span class='{cls}'>{sign}{v:.1f}%</span>"

    if (d_cvr or 0) >= 8 and (d_cpl or 0) <= -5:
        body = (
            f"Buen momentum: el CVR mejoró {_pct_html(d_cvr)} y el CPL cayó {_pct_html(d_cpl, positive_good=False)}. "
            "Recomendación: subir entre 10% y 15% el presupuesto en campañas ganadoras y mantener creativos de alto rendimiento."
        )
    elif (d_cvr or 0) <= -8 and (d_cpl or 0) >= 8:
        body = (
            f"Señal de alerta: el CVR cayó {_pct_html(d_cvr)} y el CPL subió {_pct_html(d_cpl, positive_good=False)}. "
            "Recomendación: pausar audiencias de baja intención, reforzar exclusiones y probar nuevas variantes de mensaje/oferta."
        )
    elif (d_impr or 0) >= 12 and (d_conv or 0) <= 2:
        body = (
            f"Hay más alcance ({_pct_html(d_impr)}) pero la conversión está plana ({_pct_html(d_conv)}). "
            "Recomendación: optimizar segmentación y CTA para mejorar calidad de tráfico antes de seguir escalando inversión."
        )
    elif (d_ctr or 0) <= -7:
        body = (
            f"El CTR retrocedió {_pct_html(d_ctr)} y los clics variaron {_pct_html(d_clicks)}. "
            "Recomendación: refrescar copys/creativos y revisar fatiga de anuncios en campañas con mayor frecuencia."
        )
    else:
        body = (
            f"Comportamiento estable: conversiones {_pct_html(d_conv)}, CPL {_pct_html(d_cpl, positive_good=False)} "
            f"y gasto {_pct_html(pct_delta(cur_spend, prev_spend))}. "
            "Recomendación: mantener presupuesto actual y priorizar tests A/B en audiencias y landings para ganar eficiencia incremental."
        )

    st.markdown(
        f"""
        <div class="daily-fact">
          <div class="daily-fact-icon">✦</div>
          <div>
            <div class="title">Recomendación IA de Performance</div>
            <div class="body">{body}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _report_cache_signature(path: Path) -> tuple[str, int, int]:
    return dashboard_data.report_cache_signature(path)


def _report_parquet_dataset_path(report_path: Path, dataset_key: str) -> Path:
    return dashboard_data.report_parquet_dataset_path(
        report_path,
        dataset_key,
        REPORT_PARQUET_DIRNAME,
    )


def _parquet_cache_signature(report_path: Path, dataset_key: str) -> tuple[str, int, int] | None:
    return dashboard_data.parquet_cache_signature(
        report_path,
        dataset_key,
        REPORT_PARQUET_DIRNAME,
    )


def _parquet_bundle_health(report_path: Path) -> dict[str, Any]:
    return dashboard_data.parquet_bundle_health(
        report_path,
        parquet_dirname=REPORT_PARQUET_DIRNAME,
        core_datasets=PARQUET_CORE_DATASETS,
        stale_tolerance_ns=PARQUET_STALE_TOLERANCE_NS,
    )


@st.cache_data(show_spinner=False)
def _load_report_cached(path_str: str, modified_ns: int, size_bytes: int) -> dict[str, Any]:
    _ = modified_ns
    _ = size_bytes
    return dashboard_data.load_report_json(path_str)


@st.cache_data(show_spinner=False)
def _load_parquet_df_cached(path_str: str, modified_ns: int, size_bytes: int) -> pd.DataFrame:
    _ = modified_ns
    _ = size_bytes
    return dashboard_data.load_parquet_df(path_str)


@st.cache_data(show_spinner=False)
def _load_daily_df_cached(path_str: str, modified_ns: int, size_bytes: int) -> pd.DataFrame:
    report = _load_report_cached(path_str, modified_ns, size_bytes)
    return dashboard_data.daily_df(report)


@st.cache_data(show_spinner=False)
def _load_hourly_df_cached(path_str: str, modified_ns: int, size_bytes: int) -> pd.DataFrame:
    report = _load_report_cached(path_str, modified_ns, size_bytes)
    return dashboard_data.hourly_df(report)


@st.cache_data(show_spinner=False)
def _load_acq_df_cached(path_str: str, modified_ns: int, size_bytes: int, key: str) -> pd.DataFrame:
    report = _load_report_cached(path_str, modified_ns, size_bytes)
    return dashboard_data.acq_df(report, key)


@st.cache_data(show_spinner=False)
def _load_campaign_unified_df_cached(path_str: str, modified_ns: int, size_bytes: int) -> pd.DataFrame:
    report_path = Path(path_str)
    pq_sig = _parquet_cache_signature(report_path, PARQUET_CAMPAIGN_UNIFIED_DATASET)
    if pq_sig is not None:
        return dashboard_data.normalize_campaign_unified_table(_load_parquet_df_cached(*pq_sig))

    meta_df = _load_acq_df_cached(path_str, modified_ns, size_bytes, "meta_campaign_daily")
    google_df = _load_acq_df_cached(path_str, modified_ns, size_bytes, "google_campaign_daily")
    merged = dashboard_data.build_campaign_unified_from_raw_tables(meta_df, google_df)
    return dashboard_data.normalize_campaign_unified_table(merged)


@st.cache_data(show_spinner=False)
def _load_piece_enriched_df_cached(path_str: str, modified_ns: int, size_bytes: int) -> pd.DataFrame:
    report_path = Path(path_str)
    pq_sig = _parquet_cache_signature(report_path, PARQUET_PIECE_ENRICHED_DATASET)
    if pq_sig is not None:
        return dashboard_data.normalize_paid_piece_enriched_table(_load_parquet_df_cached(*pq_sig))

    piece_df = _load_acq_df_cached(path_str, modified_ns, size_bytes, "paid_piece_daily")
    return dashboard_data.normalize_paid_piece_enriched_table(piece_df)


def load_report(path: Path) -> dict[str, Any]:
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return _load_report_cached(path_str, modified_ns, size_bytes)


def load_daily_df_from_report_path(path: Path) -> pd.DataFrame:
    pq_sig = _parquet_cache_signature(path, PARQUET_DAILY_DATASET)
    if pq_sig is not None:
        return dashboard_data.normalize_daily_table(_load_parquet_df_cached(*pq_sig))
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return _load_daily_df_cached(path_str, modified_ns, size_bytes)


def load_hourly_df_from_report_path(path: Path) -> pd.DataFrame:
    pq_sig = _parquet_cache_signature(path, PARQUET_HOURLY_DATASET)
    if pq_sig is not None:
        return dashboard_data.normalize_hourly_table(_load_parquet_df_cached(*pq_sig))
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return _load_hourly_df_cached(path_str, modified_ns, size_bytes)


def load_acq_df_from_report_path(path: Path, key: str) -> pd.DataFrame:
    pq_sig = _parquet_cache_signature(path, key)
    if pq_sig is not None:
        return dashboard_data.normalize_acq_table(_load_parquet_df_cached(*pq_sig))
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return _load_acq_df_cached(path_str, modified_ns, size_bytes, key)


def load_campaign_unified_df_from_report_path(path: Path) -> pd.DataFrame:
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return _load_campaign_unified_df_cached(path_str, modified_ns, size_bytes)


def load_piece_enriched_df_from_report_path(path: Path) -> pd.DataFrame:
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return _load_piece_enriched_df_cached(path_str, modified_ns, size_bytes)


def load_paid_device_df_from_report_path(path: Path) -> pd.DataFrame:
    pq_sig = _parquet_cache_signature(path, "paid_device_daily")
    if pq_sig is not None:
        return dashboard_data.normalize_paid_device_table(_load_parquet_df_cached(*pq_sig))
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return dashboard_data.normalize_paid_device_table(
        _load_acq_df_cached(path_str, modified_ns, size_bytes, "paid_device_daily")
    )


def load_paid_lead_demographics_df_from_report_path(path: Path) -> pd.DataFrame:
    pq_sig = _parquet_cache_signature(path, "paid_lead_demographics_daily")
    if pq_sig is not None:
        return dashboard_data.normalize_paid_lead_demographics_table(_load_parquet_df_cached(*pq_sig))
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return dashboard_data.normalize_paid_lead_demographics_table(
        _load_acq_df_cached(path_str, modified_ns, size_bytes, "paid_lead_demographics_daily")
    )


def load_paid_lead_geo_df_from_report_path(path: Path) -> pd.DataFrame:
    pq_sig = _parquet_cache_signature(path, "paid_lead_geo_daily")
    if pq_sig is not None:
        return dashboard_data.normalize_paid_lead_geo_table(
            _load_parquet_df_cached(*pq_sig),
            COUNTRY_CODE_TO_NAME,
        )
    path_str, modified_ns, size_bytes = _report_cache_signature(path)
    return dashboard_data.normalize_paid_lead_geo_table(
        _load_acq_df_cached(path_str, modified_ns, size_bytes, "paid_lead_geo_daily"),
        COUNTRY_CODE_TO_NAME,
    )


def default_tenants_config() -> dict[str, dict[str, Any]]:
    return {
        DEFAULT_TENANT_ID: {
            "id": DEFAULT_TENANT_ID,
            "name": "YAP",
            "report_path": str(REPORT_PATH),
            "meta_account_id": META_ACCOUNT_ID,
            "google_customer_id": GOOGLE_CUSTOMER_ID,
            "ga4_conversion_event_name": GA4_GTC_SOLICITAR_CODIGO_EVENT,
            "logo": str(LOGO_PATH) if LOGO_PATH.exists() else LOGO_PLACEHOLDER,
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
            "logo": _normalize_logo_source(
                entry.get("logo", entry.get("logo_path", entry.get("logo_url", "")))
            ),
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


def _safe_filename_part(raw: Any) -> str:
    txt = str(raw or "").strip().lower()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in txt)
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return cleaned or "tenant"


def _save_uploaded_logo_file(uploaded: Any, scope_key: str) -> tuple[str | None, str | None]:
    if uploaded is None:
        return None, None
    try:
        raw = uploaded.getvalue()
    except Exception:
        raw = b""
    if not raw:
        return None, "El archivo de logo está vacío."

    try:
        img = Image.open(BytesIO(raw))
        width_px, height_px = img.size
        img.close()
    except Exception:
        return None, "No se pudo leer el archivo de logo. Usa un PNG/JPG/WebP válido."

    if (width_px, height_px) != (TENANT_LOGO_UPLOAD_WIDTH_PX, TENANT_LOGO_UPLOAD_HEIGHT_PX):
        return (
            None,
            (
                f"El logo debe medir exactamente {TENANT_LOGO_UPLOAD_WIDTH_PX}x{TENANT_LOGO_UPLOAD_HEIGHT_PX}px. "
                f"Recibido: {width_px}x{height_px}px."
            ),
        )

    ext = Path(str(getattr(uploaded, "name", ""))).suffix.lower()
    if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
        ext = ".png"

    scope_part = _safe_filename_part(scope_key)
    digest_full = hashlib.sha1(raw).hexdigest()
    digest = digest_full[:10]

    try:
        TENANT_LOGOS_DIR.mkdir(parents=True, exist_ok=True)
        existing = sorted(
            TENANT_LOGOS_DIR.glob(f"{scope_part}_*_{digest}{ext}"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if existing:
            rel_existing = existing[0].relative_to(BASE_DIR).as_posix()
            return rel_existing, None

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{scope_part}_{stamp}_{digest}{ext}"
        out_path = TENANT_LOGOS_DIR / file_name
        out_path.write_bytes(raw)
    except Exception as exc:
        return None, f"No se pudo guardar el logo: {exc}"

    rel_path = out_path.relative_to(BASE_DIR).as_posix()
    return rel_path, None


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


def append_coco_usage_event(
    *,
    actor: str,
    tenant_id: str,
    query: str,
    response: str,
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
    status: str,
    error: str = "",
) -> None:
    try:
        AI_USAGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp_utc": _utc_now_iso(),
            "event_date": date.today().isoformat(),
            "actor": str(actor).strip().lower() or "unknown",
            "tenant_id": str(tenant_id).strip().lower() or "unknown",
            "provider": str(provider).strip().lower() or "local",
            "model": str(model).strip() or "local",
            "query": str(query).strip(),
            "response_preview": str(response).strip()[:240],
            "input_tokens": _normalize_non_negative_int(input_tokens, 0, minimum=0, maximum=5_000_000),
            "output_tokens": _normalize_non_negative_int(output_tokens, 0, minimum=0, maximum=5_000_000),
            "total_tokens": _normalize_non_negative_int(input_tokens, 0, minimum=0, maximum=5_000_000)
            + _normalize_non_negative_int(output_tokens, 0, minimum=0, maximum=5_000_000),
            "cost_usd": _normalize_non_negative_float(cost_usd, 0.0, minimum=0.0, maximum=1_000_000.0),
            "status": str(status).strip().lower() or "ok",
            "error": str(error).strip()[:500],
        }
        with AI_USAGE_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception:
        pass


def read_coco_usage(limit: int = 5_000) -> list[dict[str, Any]]:
    if not AI_USAGE_LOG_PATH.exists():
        return []
    try:
        lines = AI_USAGE_LOG_PATH.read_text(encoding="utf-8").splitlines()
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


def clear_coco_usage_log() -> tuple[bool, str]:
    try:
        backup_path = ""
        if AI_USAGE_LOG_PATH.exists():
            ok_backup, backup_info = _backup_config_file(AI_USAGE_LOG_PATH)
            if not ok_backup:
                return False, f"No se pudo crear backup del log: {backup_info}"
            backup_path = str(backup_info or "")
        AI_USAGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        AI_USAGE_LOG_PATH.write_text("", encoding="utf-8")
        return True, backup_path
    except Exception as exc:
        return False, str(exc)


def _estimate_token_count(text: str) -> int:
    clean = str(text or "").strip()
    if not clean:
        return 0
    # Fast approximation for dashboards where exact tokenizer dependency is not available.
    return max(1, int(math.ceil(len(clean) / 4.0)))


def _estimate_coco_cost_usd(
    input_tokens: int,
    output_tokens: int,
    coco_cfg: dict[str, Any],
) -> float:
    input_rate = _normalize_non_negative_float(
        coco_cfg.get("input_cost_per_1m", COCO_DEFAULT_INPUT_COST_PER_1M),
        COCO_DEFAULT_INPUT_COST_PER_1M,
        minimum=0.0,
        maximum=1000.0,
    )
    output_rate = _normalize_non_negative_float(
        coco_cfg.get("output_cost_per_1m", COCO_DEFAULT_OUTPUT_COST_PER_1M),
        COCO_DEFAULT_OUTPUT_COST_PER_1M,
        minimum=0.0,
        maximum=1000.0,
    )
    return ((float(input_tokens) * input_rate) + (float(output_tokens) * output_rate)) / 1_000_000.0


def _count_coco_queries_today(rows: list[dict[str, Any]], username: str, tenant_id: str) -> int:
    today_iso = date.today().isoformat()
    uname = str(username).strip().lower()
    tid = str(tenant_id).strip().lower()
    total = 0
    for row in rows:
        if str(row.get("event_date", "")).strip() != today_iso:
            continue
        if str(row.get("status", "")).strip().lower() != "ok":
            continue
        if str(row.get("actor", "")).strip().lower() != uname:
            continue
        if str(row.get("tenant_id", "")).strip().lower() != tid:
            continue
        total += 1
    return total


def _coco_tenant_cost_today_usd(rows: list[dict[str, Any]], tenant_id: str) -> float:
    today_iso = date.today().isoformat()
    tid = str(tenant_id).strip().lower()
    total_cost = 0.0
    for row in rows:
        if str(row.get("event_date", "")).strip() != today_iso:
            continue
        if str(row.get("status", "")).strip().lower() != "ok":
            continue
        if str(row.get("tenant_id", "")).strip().lower() != tid:
            continue
        total_cost += _normalize_non_negative_float(row.get("cost_usd"), 0.0, minimum=0.0, maximum=1_000_000.0)
    return total_cost


def _parse_coco_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _parse_iso_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _coco_thread_key(username: str, tenant_id: str) -> str:
    uname = str(username).strip().lower() or "unknown"
    tid = str(tenant_id).strip().lower() or "unknown"
    return f"{uname}@{tid}"


def _coco_trim_text(value: Any, max_chars: int = 4_000) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text[: max(int(max_chars), 1)]


def _load_coco_chat_state_store() -> dict[str, Any]:
    if not COCO_CHAT_STATE_PATH.exists():
        return {}
    try:
        payload = json.loads(COCO_CHAT_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_coco_chat_state_store(store: dict[str, Any]) -> None:
    try:
        COCO_CHAT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        COCO_CHAT_STATE_PATH.write_text(
            json.dumps(store, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def read_coco_chat_state(thread_key: str) -> dict[str, Any]:
    store = _load_coco_chat_state_store()
    thread = store.get(str(thread_key).strip(), {})
    return thread if isinstance(thread, dict) else {}


def write_coco_chat_state(thread_key: str, state: dict[str, Any]) -> None:
    key = str(thread_key).strip()
    if not key:
        return
    store = _load_coco_chat_state_store()
    safe_state = state if isinstance(state, dict) else {}
    safe_state["updated_at"] = _utc_now_iso()
    store[key] = safe_state
    _save_coco_chat_state_store(store)


def clear_coco_chat_state(thread_key: str) -> None:
    key = str(thread_key).strip()
    if not key:
        return
    store = _load_coco_chat_state_store()
    if key in store:
        store.pop(key, None)
        _save_coco_chat_state_store(store)


def append_coco_chat_event(
    *,
    thread_key: str,
    actor: str,
    tenant_id: str,
    role: str,
    content: str,
) -> None:
    safe_role = str(role).strip().lower()
    if safe_role not in {"user", "assistant"}:
        return
    safe_content = _coco_trim_text(content, max_chars=6_000)
    if not safe_content:
        return
    try:
        COCO_CHAT_MEMORY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp_utc": _utc_now_iso(),
            "event_date": date.today().isoformat(),
            "thread_key": str(thread_key).strip().lower(),
            "actor": str(actor).strip().lower() or "unknown",
            "tenant_id": str(tenant_id).strip().lower() or "unknown",
            "role": safe_role,
            "content": safe_content,
        }
        with COCO_CHAT_MEMORY_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def read_coco_chat_events(thread_key: str, limit: int = COCO_HISTORY_PERSIST_LIMIT) -> list[dict[str, str]]:
    key = str(thread_key).strip().lower()
    if not key or not COCO_CHAT_MEMORY_LOG_PATH.exists():
        return []
    try:
        lines = COCO_CHAT_MEMORY_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []

    rows: list[dict[str, str]] = []
    for raw in reversed(lines):
        if len(rows) >= max(int(limit), 1):
            break
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        if str(parsed.get("thread_key", "")).strip().lower() != key:
            continue
        role = str(parsed.get("role", "")).strip().lower()
        content = _coco_trim_text(parsed.get("content", ""), max_chars=6_000)
        if role not in {"user", "assistant"} or not content:
            continue
        rows.append({"role": role, "content": content})
    rows.reverse()
    return rows


def clear_coco_chat_events(thread_key: str) -> None:
    key = str(thread_key).strip().lower()
    if not key or not COCO_CHAT_MEMORY_LOG_PATH.exists():
        return
    try:
        lines = COCO_CHAT_MEMORY_LOG_PATH.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    keep: list[str] = []
    for raw in lines:
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        if str(parsed.get("thread_key", "")).strip().lower() == key:
            continue
        keep.append(json.dumps(parsed, ensure_ascii=False))
    try:
        COCO_CHAT_MEMORY_LOG_PATH.write_text(
            ("\n".join(keep) + ("\n" if keep else "")),
            encoding="utf-8",
        )
    except Exception:
        pass


def _coco_history_for_model(
    history: list[dict[str, str]],
    *,
    max_messages: int = COCO_HISTORY_FOR_MODEL_LIMIT,
    max_chars_per_message: int = COCO_HISTORY_MSG_MAX_CHARS,
) -> list[dict[str, str]]:
    if not isinstance(history, list):
        return []
    selected = history[-max(int(max_messages), 1):]
    prepared: list[dict[str, str]] = []
    for row in selected:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip().lower()
        content = _coco_trim_text(row.get("content", ""), max_chars=max_chars_per_message)
        if role not in {"user", "assistant"} or not content:
            continue
        prepared.append({"role": role, "content": content})
    return prepared


def _summarize_coco_history(
    history: list[dict[str, str]],
    *,
    keep_recent_messages: int = 8,
    max_chars: int = COCO_MEMORY_SUMMARY_MAX_CHARS,
) -> str:
    if not isinstance(history, list):
        return ""
    older = history[:-max(int(keep_recent_messages), 1)] if len(history) > keep_recent_messages else []
    if not older:
        return ""
    lines: list[str] = []
    for row in older:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _coco_trim_text(row.get("content", ""), max_chars=240)
        if not content:
            continue
        label = "Usuario" if role == "user" else "COCO"
        lines.append(f"- {label}: {content}")
    summary = "\n".join(lines).strip()
    return summary[: max(int(max_chars), 1)]


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
        role = str(user.get("role", "viewer")).strip().lower() or "viewer"
        if bool(user.get("enabled", True)):
            if not str(user.get("password_hash", "")).strip() or not str(user.get("password_salt", "")).strip():
                issues.append(f"Usuario activo '{uname}' sin password_hash/password_salt.")
        allowed = _normalize_allowed_tenants(user.get("allowed_tenants", ["*"]))
        scopes = _normalize_tenant_scopes(user.get("tenant_scopes"), allowed, role)
        for tenant_id in allowed:
            if tenant_id != "*" and tenant_id not in tenant_ids:
                issues.append(f"Usuario '{uname}' apunta a tenant inexistente '{tenant_id}'.")
        for scope in scopes:
            tenant_id = str(scope.get("tenant_id", "")).strip().lower()
            if tenant_id and tenant_id != "*" and tenant_id not in tenant_ids:
                issues.append(f"Usuario '{uname}' tiene scope huérfano en tenant '{tenant_id}'.")
    return issues


def repair_users_tenant_integrity(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    tenant_ids = list(tenants.keys())
    tenant_set = set(tenant_ids)
    fallback_tenant = DEFAULT_TENANT_ID if DEFAULT_TENANT_ID in tenant_set else (tenant_ids[0] if tenant_ids else "")

    repaired: dict[str, dict[str, Any]] = {}
    notes: list[str] = []

    for username, raw_user in users.items():
        user = dict(raw_user) if isinstance(raw_user, dict) else {}
        uname = str(user.get("username", username)).strip().lower() or str(username).strip().lower()
        role = str(user.get("role", "viewer")).strip().lower() or "viewer"
        global_role = str(user.get("global_role", "")).strip().lower()
        if not global_role:
            global_role = "admin" if role == "admin" else "user"

        allowed = _normalize_allowed_tenants(user.get("allowed_tenants", ["*"]))
        scopes = _normalize_tenant_scopes(user.get("tenant_scopes"), allowed, role)

        invalid_allowed = sorted({tid for tid in allowed if tid != "*" and tid not in tenant_set})
        invalid_scopes = sorted(
            {
                str(scope.get("tenant_id", "")).strip().lower()
                for scope in scopes
                if str(scope.get("tenant_id", "")).strip().lower() not in {"", "*"}
                and str(scope.get("tenant_id", "")).strip().lower() not in tenant_set
            }
        )

        allowed_clean = [tid for tid in allowed if tid == "*" or tid in tenant_set]
        if "*" in allowed_clean:
            allowed_clean = ["*"]

        scope_map: dict[str, str] = {}
        for scope in scopes:
            tenant_id = str(scope.get("tenant_id", "")).strip().lower()
            if not tenant_id:
                continue
            if tenant_id != "*" and tenant_id not in tenant_set:
                continue
            if tenant_id in scope_map:
                continue
            scope_map[tenant_id] = str(scope.get("role", role)).strip().lower() or role

        if "*" in allowed_clean:
            star_role = scope_map.get("*", "admin" if global_role == "admin" or role == "admin" else role)
            scopes_clean = [{"tenant_id": "*", "role": star_role}]
        else:
            allowed_from_scopes = [tenant_id for tenant_id in scope_map.keys() if tenant_id != "*"]
            if not allowed_clean and allowed_from_scopes:
                allowed_clean = allowed_from_scopes
            scopes_clean = []
            for tenant_id in allowed_clean:
                scopes_clean.append(
                    {
                        "tenant_id": tenant_id,
                        "role": scope_map.get(tenant_id, role),
                    }
                )

        enabled = bool(user.get("enabled", True))
        if not allowed_clean:
            if global_role == "admin" or role == "admin":
                allowed_clean = ["*"]
                scopes_clean = [{"tenant_id": "*", "role": "admin"}]
                notes.append(f"Usuario '{uname}': sin tenants válidos, se restauró acceso global admin.")
            elif fallback_tenant:
                allowed_clean = [fallback_tenant]
                scopes_clean = [{"tenant_id": fallback_tenant, "role": role}]
                enabled = False
                notes.append(
                    f"Usuario '{uname}': se removieron tenants huérfanos y quedó deshabilitado con scope temporal '{fallback_tenant}'."
                )

        if invalid_allowed or invalid_scopes:
            removed = sorted(set(invalid_allowed + invalid_scopes))
            notes.append(f"Usuario '{uname}': se removieron referencias a tenants inexistentes {removed}.")

        user["username"] = uname
        user["role"] = role
        user["global_role"] = global_role
        user["enabled"] = enabled
        user["allowed_tenants"] = allowed_clean
        user["tenant_scopes"] = scopes_clean
        repaired[uname] = user

    return repaired, notes


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


def _normalize_view_mode_keys(raw_keys: Any, fallback_keys: list[str]) -> list[str]:
    allowed = set(VIEW_MODE_OPTIONS)
    values: list[str] = []
    if isinstance(raw_keys, list):
        values = [str(v).strip() for v in raw_keys if str(v).strip()]
    elif isinstance(raw_keys, str):
        values = [part.strip() for part in raw_keys.split(",") if part.strip()]
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
        normalized = list(VIEW_MODE_OPTIONS)
    return normalized


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


def _normalize_logo_source(raw_value: Any) -> str:
    return str(raw_value or "").strip()


def _normalize_hex_color(raw_value: Any, fallback: str) -> str:
    default_color = str(fallback or "#000000").strip()
    if not re.fullmatch(r"#?[0-9A-Fa-f]{6}", default_color):
        default_color = "#000000"
    if not default_color.startswith("#"):
        default_color = f"#{default_color}"
    value = str(raw_value or "").strip()
    if not re.fullmatch(r"#?[0-9A-Fa-f]{6}", value):
        return default_color.upper()
    if not value.startswith("#"):
        value = f"#{value}"
    return value.upper()


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    color = _normalize_hex_color(hex_color, "#000000")
    a = max(0.0, min(float(alpha), 1.0))
    r = int(color[1:3], 16)
    g = int(color[3:5], 16)
    b = int(color[5:7], 16)
    return f"rgba({r},{g},{b},{a:.3f})"


def _normalize_theme_colors(raw_value: Any, fallback: dict[str, str] | None = None) -> dict[str, str]:
    base = dict(DEFAULT_THEME_COLORS if fallback is None else fallback)
    source = raw_value if isinstance(raw_value, dict) else {}
    normalized: dict[str, str] = {}
    for key in THEME_COLOR_KEYS:
        normalized[key] = _normalize_hex_color(source.get(key), base.get(key, DEFAULT_THEME_COLORS[key]))
    return normalized


def _apply_theme_palette(theme_colors: Any) -> dict[str, str]:
    global C_GOOGLE, C_META, C_ACCENT, C_MUTE, C_GRID, C_PANEL_BORDER, ACTIVE_THEME_COLORS
    normalized = _normalize_theme_colors(theme_colors, ACTIVE_THEME_COLORS)
    ACTIVE_THEME_COLORS = dict(normalized)
    C_GOOGLE = normalized["google"]
    C_META = normalized["meta"]
    C_ACCENT = normalized["accent"]
    C_MUTE = normalized["neutral"]
    C_GRID = _hex_to_rgba(C_MUTE, 0.18)
    C_PANEL_BORDER = _hex_to_rgba(C_MUTE, 0.24)
    return normalized


def _theme_color_scale(base_hex: str) -> list[list[Any]]:
    base = _normalize_hex_color(base_hex, C_GOOGLE)
    return [
        [0.0, _hex_to_rgba(base, 0.18)],
        [0.35, _hex_to_rgba(base, 0.34)],
        [0.7, _hex_to_rgba(base, 0.58)],
        [1.0, base],
    ]


def apply_tenant_theme_overrides(theme_colors: Any) -> None:
    palette = _normalize_theme_colors(theme_colors, ACTIVE_THEME_COLORS)
    meta_bg = _hex_to_rgba(palette["meta"], 0.12)
    meta_border = _hex_to_rgba(palette["meta"], 0.30)
    google_bg = _hex_to_rgba(palette["google"], 0.12)
    google_border = _hex_to_rgba(palette["google"], 0.30)
    success_bg = _hex_to_rgba(palette["success"], 0.16)
    success_border = _hex_to_rgba(palette["success"], 0.28)
    danger_bg = _hex_to_rgba(palette["danger"], 0.12)
    danger_border = _hex_to_rgba(palette["danger"], 0.28)
    accent_bg_20 = _hex_to_rgba(palette["accent"], 0.20)
    accent_bg_28 = _hex_to_rgba(palette["accent"], 0.28)
    accent_bg_35 = _hex_to_rgba(palette["accent"], 0.35)
    accent_border_44 = _hex_to_rgba(palette["accent"], 0.44)
    accent_border_72 = _hex_to_rgba(palette["accent"], 0.72)
    accent_shadow = _hex_to_rgba(palette["accent"], 0.26)
    accent_shadow_soft = _hex_to_rgba(palette["accent"], 0.18)
    neutral_bg = _hex_to_rgba(palette["neutral"], 0.12)
    neutral_border = _hex_to_rgba(palette["neutral"], 0.30)
    neutral_border_soft = _hex_to_rgba(palette["neutral"], 0.18)
    panel_border = _hex_to_rgba(palette["neutral"], 0.24)
    panel_shadow = _hex_to_rgba(palette["neutral"], 0.24)
    text_on_dark = "#FFFFFF"
    st.markdown(
        f"""
        <style>
          [data-baseweb="tab-list"] {{
            border-color: {neutral_border_soft} !important;
          }}
          [aria-selected="true"][data-baseweb="tab"] {{
            background: {palette["accent"]} !important;
            color: {text_on_dark} !important;
            box-shadow: 0 8px 16px {accent_shadow_soft} !important;
          }}
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[data-checked="true"],
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[aria-checked="true"],
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label:has(input:checked) {{
            background: linear-gradient(180deg, {accent_bg_28} 0%, {accent_bg_20} 100%) !important;
            border-color: {accent_border_72} !important;
            box-shadow: 0 4px 12px {accent_shadow_soft} !important;
          }}
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[data-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label[aria-checked="true"] [data-testid="stMarkdownContainer"] p,
          [data-testid="stMain"] .stRadio [role="radiogroup"] > label:has(input:checked) [data-testid="stMarkdownContainer"] p {{
            color: {palette["accent"]} !important;
          }}
          [data-testid="stSegmentedControl"] [data-baseweb="button-group"] {{
            border-color: {neutral_border_soft} !important;
          }}
          [data-testid="stSegmentedControl"] [data-baseweb="button"][aria-pressed="true"],
          [data-testid="stSegmentedControl"] [data-baseweb="button"][aria-selected="true"],
          [data-testid="stSegmentedControl"] [data-baseweb="button"][data-active="true"] {{
            background: linear-gradient(180deg, {accent_bg_28} 0%, {accent_bg_20} 100%) !important;
            color: {palette["accent"]} !important;
            border-color: {accent_border_72} !important;
            box-shadow: 0 4px 12px {accent_shadow_soft} !important;
          }}
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked) {{
            background: linear-gradient(180deg, {accent_bg_28} 0%, {accent_bg_20} 100%) !important;
            border-color: {accent_border_72} !important;
          }}
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"]::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"]::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked)::before {{
            color: {palette["accent"]} !important;
            border-color: {accent_border_72} !important;
            background: linear-gradient(180deg, {accent_bg_35} 0%, {accent_bg_20} 100%) !important;
          }}
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[data-checked="true"] [data-testid="stMarkdownContainer"] *,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label[aria-checked="true"] [data-testid="stMarkdownContainer"] *,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:has(input:checked) [data-testid="stMarkdownContainer"] * {{
            color: {palette["accent"]} !important;
          }}
          [data-testid="stSidebar"] [data-testid="stButton"] button[kind="primary"],
          [data-testid="stMain"] [data-testid="stButton"] button[kind="primary"],
          [data-testid="stFormSubmitButton"] button {{
            border-color: {accent_border_72} !important;
            background: {palette["accent"]} !important;
            color: {text_on_dark} !important;
            box-shadow: 0 10px 20px {accent_shadow} !important;
          }}
          [data-testid="stSidebar"] [data-testid="stButton"] button[kind="secondary"]:hover,
          [data-testid="stSidebar"] [data-testid="stButton"] button:not([kind="primary"]):hover {{
            border-color: {accent_border_44} !important;
            background: {accent_bg_20} !important;
            color: {palette["accent"]} !important;
            box-shadow: 0 6px 14px {accent_shadow_soft} !important;
          }}
          [data-testid="stSidebar"] [data-testid="stButton"] button[kind="secondary"]:hover svg,
          [data-testid="stSidebar"] [data-testid="stButton"] button:not([kind="primary"]):hover svg {{
            fill: {palette["accent"]} !important;
            stroke: {palette["accent"]} !important;
          }}
          [data-testid="stSidebar"] [data-testid="stButton"] button[kind="primary"]:hover,
          [data-testid="stMain"] [data-testid="stButton"] button[kind="primary"]:hover,
          [data-testid="stFormSubmitButton"] button:hover {{
            background: {palette["google"]} !important;
            border-color: {google_border} !important;
          }}
          .filter-chip {{
            border-color: {neutral_border} !important;
            color: {palette["neutral"]} !important;
            background: {neutral_bg} !important;
          }}
          .filter-chip .k {{
            color: {palette["neutral"]} !important;
          }}
          .sidebar-nav-item.active {{
            color: {palette["accent"]} !important;
            border-color: {accent_border_72} !important;
            background: linear-gradient(180deg, {accent_bg_28} 0%, {accent_bg_20} 100%) !important;
            box-shadow: 0 6px 14px {accent_shadow_soft} !important;
          }}
          .piece-link:hover {{
            border-color: {accent_border_72} !important;
            color: {palette["accent"]} !important;
            box-shadow: 0 6px 14px {accent_shadow_soft} !important;
          }}
          .daily-fact-icon {{
            border-color: {success_border} !important;
            background: {success_bg} !important;
            color: {palette["success"]} !important;
          }}
          .daily-fact-highlight {{
            color: {palette["success"]} !important;
          }}
          .daily-fact-highlight.neg {{
            color: {palette["danger"]} !important;
          }}
          .funnel-drop {{
            color: {palette["danger"]} !important;
            background: {danger_bg} !important;
            border-color: {danger_border} !important;
          }}
          .funnel-drop-base {{
            color: {palette["neutral"]} !important;
            background: {neutral_bg} !important;
            border-color: {neutral_border} !important;
          }}
          .pill-meta {{
            background: {meta_bg} !important;
            border-color: {meta_border} !important;
            color: {palette["meta"]} !important;
          }}
          .pill-google {{
            background: {google_bg} !important;
            border-color: {google_border} !important;
            color: {palette["google"]} !important;
          }}
          .roas-good {{
            color: {palette["success"]} !important;
          }}
          .roas-mid {{
            color: {palette["neutral"]} !important;
          }}
          .top-pieces-footer {{
            color: {palette["accent"]} !important;
          }}
          .sidebar-token-days.good {{
            color: {palette["success"]} !important;
          }}
          .sidebar-token-days.warn {{
            color: {palette["accent"]} !important;
          }}
          .sidebar-token-days.bad {{
            color: {palette["danger"]} !important;
          }}
          .sidebar-token-days.na {{
            color: {palette["neutral"]} !important;
          }}
          [class*="st-key-coco-fab-"] [data-testid="stButton"] button,
          [class*="st-key-coco_toggle_panel_"] [data-testid="stButton"] button {{
            border-color: {accent_border_44} !important;
            box-shadow: 0 14px 30px {accent_shadow} !important;
            background: linear-gradient(145deg, {palette["accent"]} 0%, {palette["google"]} 58%, {palette["success"]} 100%) !important;
          }}
          [class*="st-key-coco-panel-"],
          [class*="st-key-coco_panel_"] {{
            border-color: {panel_border} !important;
            box-shadow: 0 22px 48px {panel_shadow} !important;
          }}
          [class*="st-key-coco-panel-"] [data-testid="stTextArea"] textarea,
          [class*="st-key-coco_panel_"] [data-testid="stTextArea"] textarea {{
            border-color: {neutral_border_soft} !important;
          }}
          [class*="st-key-coco-panel-"] [data-testid="stTextArea"] textarea:focus,
          [class*="st-key-coco_panel_"] [data-testid="stTextArea"] textarea:focus {{
            border-color: {accent_border_72} !important;
            box-shadow: 0 0 0 1px {accent_bg_35} !important;
          }}
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stMetricLabel"] {{
            color: {palette["accent"]} !important;
          }}
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stMetricValue"] {{
            color: {palette["accent"]} !important;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _resolve_logo_image_source(raw_value: Any) -> str:
    logo_value = _normalize_logo_source(raw_value)
    if logo_value:
        if logo_value.startswith(("http://", "https://", "data:image/")):
            return logo_value
        logo_path = _resolve_repo_path(logo_value)
        if logo_path.exists():
            return str(logo_path)
    if LOGO_PATH.exists():
        return str(LOGO_PATH)
    return LOGO_PLACEHOLDER


def _image_source_to_data_uri(image_source: str) -> str:
    src = str(image_source or "").strip()
    if not src:
        return src
    if src.startswith(("http://", "https://", "data:image/")):
        return src
    path = Path(src)
    if not path.exists():
        return src
    mime_by_suffix = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
    }
    mime = mime_by_suffix.get(path.suffix.lower(), "image/png")
    try:
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{encoded}"
    except Exception:
        return src


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


def _normalize_campaign_filter_keys(raw_keys: Any, fallback_keys: list[str]) -> list[str]:
    allowed = set(CAMPAIGN_FILTER_OPTIONS.keys())
    values: list[str] = []
    explicit_empty = False
    if isinstance(raw_keys, list):
        values = [str(v).strip().lower() for v in raw_keys if str(v).strip()]
        explicit_empty = len(values) == 0
    elif isinstance(raw_keys, str):
        values = [part.strip().lower() for part in raw_keys.split(",") if part.strip()]
        explicit_empty = len(values) == 0 and raw_keys.strip() == ""
    normalized: list[str] = []
    seen: set[str] = set()
    for key in values:
        if key not in allowed or key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    if not normalized and not explicit_empty:
        for key in fallback_keys:
            if key in allowed and key not in seen:
                seen.add(key)
                normalized.append(key)
    if not normalized and not explicit_empty:
        normalized = [k for k in DEFAULT_CAMPAIGN_FILTER_KEYS if k in allowed]
    return normalized[:5]


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


def _normalize_non_negative_int(value: Any, fallback: int, *, minimum: int = 0, maximum: int = 100_000) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = fallback
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


def _normalize_non_negative_float(value: Any, fallback: float, *, minimum: float = 0.0, maximum: float = 1_000_000.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(fallback)
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


def _normalize_coco_limit_map(raw_map: Any, *, key_mode: str = "generic") -> dict[str, int]:
    normalized: dict[str, int] = {}
    if not isinstance(raw_map, dict):
        return normalized
    for raw_key, raw_value in raw_map.items():
        key = str(raw_key).strip().lower()
        if not key:
            continue
        if key_mode == "user_tenant":
            if "@" not in key:
                continue
            user_part, tenant_part = key.split("@", 1)
            user_part = user_part.strip().lower()
            tenant_part = tenant_part.strip().lower()
            if not user_part or not tenant_part:
                continue
            key = f"{user_part}@{tenant_part}"
        elif key_mode in {"tenant", "user"}:
            key = key.strip().lower()
        normalized[key] = _normalize_non_negative_int(raw_value, COCO_DEFAULT_MAX_DAILY_QUERIES, minimum=0, maximum=10_000)
    return normalized


def _normalize_coco_enabled_tenant_map(
    raw_map: Any,
    tenants: dict[str, dict[str, Any]],
    fallback_enabled: bool,
) -> dict[str, bool]:
    normalized: dict[str, bool] = {}
    if isinstance(raw_map, dict):
        for raw_key, raw_value in raw_map.items():
            key = str(raw_key).strip().lower()
            if not key:
                continue
            normalized[key] = bool(raw_value)
    for tenant_id in tenants.keys():
        tenant_key = str(tenant_id).strip().lower()
        if tenant_key not in normalized:
            normalized[tenant_key] = bool(fallback_enabled)
    return normalized


def _normalize_coco_budget_map(
    raw_map: Any,
    tenants: dict[str, dict[str, Any]],
    fallback_budget: float,
) -> dict[str, float]:
    normalized: dict[str, float] = {}
    if isinstance(raw_map, dict):
        for raw_key, raw_value in raw_map.items():
            key = str(raw_key).strip().lower()
            if not key:
                continue
            normalized[key] = _normalize_non_negative_float(raw_value, fallback_budget, minimum=0.0, maximum=1_000_000.0)
    for tenant_id in tenants.keys():
        tenant_key = str(tenant_id).strip().lower()
        if tenant_key not in normalized:
            normalized[tenant_key] = _normalize_non_negative_float(fallback_budget, COCO_DEFAULT_DAILY_BUDGET_USD, minimum=0.0, maximum=1_000_000.0)
    return normalized


def _default_coco_ia_settings(tenants: dict[str, dict[str, Any]]) -> dict[str, Any]:
    tenant_limits = {str(tid).strip().lower(): COCO_DEFAULT_MAX_DAILY_QUERIES for tid in tenants.keys()}
    tenant_enabled = {str(tid).strip().lower(): True for tid in tenants.keys()}
    tenant_budgets = {str(tid).strip().lower(): COCO_DEFAULT_DAILY_BUDGET_USD for tid in tenants.keys()}
    return {
        "enabled": False,
        "enabled_tenants": tenant_enabled,
        "provider": COCO_DEFAULT_PROVIDER,
        "model": COCO_DEFAULT_MODEL,
        "max_daily_queries_default": COCO_DEFAULT_MAX_DAILY_QUERIES,
        "max_daily_queries_tenant": tenant_limits,
        "max_daily_queries_user": {},
        "max_daily_queries_user_tenant": {},
        "daily_budget_usd_default": COCO_DEFAULT_DAILY_BUDGET_USD,
        "daily_budget_usd_tenant": tenant_budgets,
        "input_cost_per_1m": COCO_DEFAULT_INPUT_COST_PER_1M,
        "output_cost_per_1m": COCO_DEFAULT_OUTPUT_COST_PER_1M,
    }


def _normalize_coco_ia_settings(raw_cfg: Any, tenants: dict[str, dict[str, Any]]) -> dict[str, Any]:
    defaults = _default_coco_ia_settings(tenants)
    source = raw_cfg if isinstance(raw_cfg, dict) else {}
    enabled = bool(source.get("enabled", defaults["enabled"]))
    enabled_tenants = _normalize_coco_enabled_tenant_map(
        source.get("enabled_tenants"),
        tenants,
        fallback_enabled=enabled,
    )
    daily_budget_default = _normalize_non_negative_float(
        source.get("daily_budget_usd_default", defaults.get("daily_budget_usd_default", COCO_DEFAULT_DAILY_BUDGET_USD)),
        defaults.get("daily_budget_usd_default", COCO_DEFAULT_DAILY_BUDGET_USD),
        minimum=0.0,
        maximum=1_000_000.0,
    )
    daily_budget_tenant = _normalize_coco_budget_map(
        source.get("daily_budget_usd_tenant"),
        tenants,
        daily_budget_default,
    )
    tenant_limits = _normalize_coco_limit_map(source.get("max_daily_queries_tenant"), key_mode="tenant")
    for tenant_id in tenants.keys():
        tenant_key = str(tenant_id).strip().lower()
        if tenant_key not in tenant_limits:
            tenant_limits[tenant_key] = defaults["max_daily_queries_default"]
    return {
        "enabled": enabled,
        "enabled_tenants": enabled_tenants,
        "provider": str(source.get("provider", defaults["provider"])).strip().lower() or defaults["provider"],
        "model": str(source.get("model", defaults["model"])).strip() or defaults["model"],
        "max_daily_queries_default": _normalize_non_negative_int(
            source.get("max_daily_queries_default", defaults["max_daily_queries_default"]),
            defaults["max_daily_queries_default"],
            minimum=0,
            maximum=10_000,
        ),
        "max_daily_queries_tenant": tenant_limits,
        "max_daily_queries_user": _normalize_coco_limit_map(source.get("max_daily_queries_user"), key_mode="user"),
        "max_daily_queries_user_tenant": _normalize_coco_limit_map(
            source.get("max_daily_queries_user_tenant"),
            key_mode="user_tenant",
        ),
        "daily_budget_usd_default": daily_budget_default,
        "daily_budget_usd_tenant": daily_budget_tenant,
        "input_cost_per_1m": _normalize_non_negative_float(
            source.get("input_cost_per_1m", defaults["input_cost_per_1m"]),
            defaults["input_cost_per_1m"],
            minimum=0.0,
            maximum=1000.0,
        ),
        "output_cost_per_1m": _normalize_non_negative_float(
            source.get("output_cost_per_1m", defaults["output_cost_per_1m"]),
            defaults["output_cost_per_1m"],
            minimum=0.0,
            maximum=1000.0,
        ),
    }


def _is_coco_enabled_for_tenant(coco_cfg: dict[str, Any], tenant_id: str) -> bool:
    if not bool(coco_cfg.get("enabled", False)):
        return False
    tenant_key = str(tenant_id).strip().lower()
    enabled_map = coco_cfg.get("enabled_tenants", {})
    if isinstance(enabled_map, dict):
        if tenant_key in enabled_map:
            return bool(enabled_map.get(tenant_key))
        if "*" in enabled_map:
            return bool(enabled_map.get("*"))
    return True


def _resolve_coco_daily_limit(coco_cfg: dict[str, Any], username: str, tenant_id: str) -> int:
    uname = str(username).strip().lower()
    tid = str(tenant_id).strip().lower()
    user_tenant_key = f"{uname}@{tid}"
    user_tenant_limits = coco_cfg.get("max_daily_queries_user_tenant", {})
    if isinstance(user_tenant_limits, dict) and user_tenant_key in user_tenant_limits:
        return _normalize_non_negative_int(user_tenant_limits.get(user_tenant_key), COCO_DEFAULT_MAX_DAILY_QUERIES, minimum=0, maximum=10_000)
    user_limits = coco_cfg.get("max_daily_queries_user", {})
    if isinstance(user_limits, dict) and uname in user_limits:
        return _normalize_non_negative_int(user_limits.get(uname), COCO_DEFAULT_MAX_DAILY_QUERIES, minimum=0, maximum=10_000)
    tenant_limits = coco_cfg.get("max_daily_queries_tenant", {})
    if isinstance(tenant_limits, dict) and tid in tenant_limits:
        return _normalize_non_negative_int(tenant_limits.get(tid), COCO_DEFAULT_MAX_DAILY_QUERIES, minimum=0, maximum=10_000)
    return _normalize_non_negative_int(
        coco_cfg.get("max_daily_queries_default", COCO_DEFAULT_MAX_DAILY_QUERIES),
        COCO_DEFAULT_MAX_DAILY_QUERIES,
        minimum=0,
        maximum=10_000,
    )


def _resolve_coco_daily_budget_usd(coco_cfg: dict[str, Any], tenant_id: str) -> float:
    tid = str(tenant_id).strip().lower()
    tenant_budget_map = coco_cfg.get("daily_budget_usd_tenant", {})
    if isinstance(tenant_budget_map, dict) and tid in tenant_budget_map:
        return _normalize_non_negative_float(tenant_budget_map.get(tid), COCO_DEFAULT_DAILY_BUDGET_USD, minimum=0.0, maximum=1_000_000.0)
    return _normalize_non_negative_float(
        coco_cfg.get("daily_budget_usd_default", COCO_DEFAULT_DAILY_BUDGET_USD),
        COCO_DEFAULT_DAILY_BUDGET_USD,
        minimum=0.0,
        maximum=1_000_000.0,
    )


def default_dashboard_settings(tenants: dict[str, dict[str, Any]]) -> dict[str, Any]:
    defaults = {
        "overview_kpis": list(DEFAULT_OVERVIEW_KPI_KEYS),
        "traffic_kpis": list(DEFAULT_TRAFFIC_KPI_KEYS),
        "overview_sections": list(DEFAULT_OVERVIEW_SECTION_KEYS),
        "traffic_sections": list(DEFAULT_TRAFFIC_SECTION_KEYS),
        "campaign_filters": list(DEFAULT_CAMPAIGN_FILTER_KEYS),
        "enabled_view_modes": list(VIEW_MODE_OPTIONS),
        "default_platform": "All",
        "default_view_mode": "Overview",
        "tenant_logo": "",
        "theme_colors": dict(DEFAULT_THEME_COLORS),
    }
    tenant_cfg: dict[str, dict[str, Any]] = {}
    for tenant_id in tenants.keys():
        tenant_cfg[tenant_id] = {
            "overview_kpis": list(DEFAULT_OVERVIEW_KPI_KEYS),
            "traffic_kpis": list(DEFAULT_TRAFFIC_KPI_KEYS),
            "overview_sections": list(DEFAULT_OVERVIEW_SECTION_KEYS),
            "traffic_sections": list(DEFAULT_TRAFFIC_SECTION_KEYS),
            "campaign_filters": list(DEFAULT_CAMPAIGN_FILTER_KEYS),
            "enabled_view_modes": list(VIEW_MODE_OPTIONS),
            "default_platform": "All",
            "default_view_mode": "Overview",
            "tenant_logo": "",
            "theme_colors": dict(DEFAULT_THEME_COLORS),
        }
    return {
        "defaults": defaults,
        "tenants": tenant_cfg,
        "coco_ia": _default_coco_ia_settings(tenants),
    }


def ensure_dashboard_settings_runtime_file(runtime_path: Path, template_path: Path) -> None:
    if runtime_path.exists() or not template_path.exists():
        return
    try:
        runtime_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_path.write_text(template_path.read_text(encoding="utf-8"), encoding="utf-8")
    except Exception:
        pass


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
        "campaign_filters": _normalize_campaign_filter_keys(
            raw_defaults.get("campaign_filters", base["defaults"]["campaign_filters"]),
            base["defaults"]["campaign_filters"],
        ),
        "enabled_view_modes": _normalize_view_mode_keys(
            raw_defaults.get("enabled_view_modes", base["defaults"].get("enabled_view_modes", list(VIEW_MODE_OPTIONS))),
            base["defaults"].get("enabled_view_modes", list(VIEW_MODE_OPTIONS)),
        ),
        "default_platform": _normalize_platform_option(
            raw_defaults.get("default_platform", base["defaults"]["default_platform"])
        ),
        "default_view_mode": _normalize_view_mode_option(
            raw_defaults.get("default_view_mode", base["defaults"]["default_view_mode"])
        ),
        "tenant_logo": _normalize_logo_source(
            raw_defaults.get("tenant_logo", base["defaults"].get("tenant_logo", ""))
        ),
        "theme_colors": _normalize_theme_colors(
            raw_defaults.get("theme_colors", base["defaults"].get("theme_colors", DEFAULT_THEME_COLORS)),
            base["defaults"].get("theme_colors", DEFAULT_THEME_COLORS),
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
            "campaign_filters": _normalize_campaign_filter_keys(
                raw_cfg.get("campaign_filters", defaults["campaign_filters"]),
                defaults["campaign_filters"],
            ),
            "enabled_view_modes": _normalize_view_mode_keys(
                raw_cfg.get("enabled_view_modes", defaults.get("enabled_view_modes", list(VIEW_MODE_OPTIONS))),
                defaults.get("enabled_view_modes", list(VIEW_MODE_OPTIONS)),
            ),
            "default_platform": _normalize_platform_option(
                raw_cfg.get("default_platform", defaults["default_platform"])
            ),
            "default_view_mode": _normalize_view_mode_option(
                raw_cfg.get("default_view_mode", defaults["default_view_mode"])
            ),
            "tenant_logo": _normalize_logo_source(raw_cfg.get("tenant_logo", defaults.get("tenant_logo", ""))),
            "theme_colors": _normalize_theme_colors(
                raw_cfg.get("theme_colors", defaults.get("theme_colors", DEFAULT_THEME_COLORS)),
                defaults.get("theme_colors", DEFAULT_THEME_COLORS),
            ),
        }
    raw_coco = payload.get("coco_ia", {}) if isinstance(payload, dict) else {}
    coco_ia = _normalize_coco_ia_settings(raw_coco, tenants)
    return {"defaults": defaults, "tenants": tenant_cfg, "coco_ia": coco_ia}


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
            "campaign_filters": _normalize_campaign_filter_keys(
                incoming_defaults.get("campaign_filters", normalized["defaults"].get("campaign_filters", DEFAULT_CAMPAIGN_FILTER_KEYS)),
                DEFAULT_CAMPAIGN_FILTER_KEYS,
            ),
            "enabled_view_modes": _normalize_view_mode_keys(
                incoming_defaults.get("enabled_view_modes", normalized["defaults"].get("enabled_view_modes", list(VIEW_MODE_OPTIONS))),
                list(VIEW_MODE_OPTIONS),
            ),
            "default_platform": _normalize_platform_option(
                incoming_defaults.get("default_platform", normalized["defaults"]["default_platform"])
            ),
            "default_view_mode": _normalize_view_mode_option(
                incoming_defaults.get("default_view_mode", normalized["defaults"]["default_view_mode"])
            ),
            "tenant_logo": _normalize_logo_source(
                incoming_defaults.get("tenant_logo", normalized["defaults"].get("tenant_logo", ""))
            ),
            "theme_colors": _normalize_theme_colors(
                incoming_defaults.get("theme_colors", normalized["defaults"].get("theme_colors", DEFAULT_THEME_COLORS)),
                normalized["defaults"].get("theme_colors", DEFAULT_THEME_COLORS),
            ),
        }
        if normalized["defaults"]["default_view_mode"] not in normalized["defaults"]["enabled_view_modes"]:
            normalized["defaults"]["default_view_mode"] = normalized["defaults"]["enabled_view_modes"][0]
        incoming_tenants = settings.get("tenants", {}) if isinstance(settings, dict) else {}
        if not isinstance(incoming_tenants, dict):
            incoming_tenants = {}
        tenant_cfg: dict[str, dict[str, Any]] = {}
        for tenant_id in tenants.keys():
            raw_cfg = incoming_tenants.get(tenant_id, {})
            if not isinstance(raw_cfg, dict):
                raw_cfg = {}
            existing_tenant_cfg = normalized.get("tenants", {}).get(tenant_id, {})
            if not isinstance(existing_tenant_cfg, dict):
                existing_tenant_cfg = {}
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
                "campaign_filters": _normalize_campaign_filter_keys(
                    raw_cfg.get("campaign_filters", normalized["defaults"].get("campaign_filters", DEFAULT_CAMPAIGN_FILTER_KEYS)),
                    normalized["defaults"].get("campaign_filters", DEFAULT_CAMPAIGN_FILTER_KEYS),
                ),
                "enabled_view_modes": _normalize_view_mode_keys(
                    raw_cfg.get("enabled_view_modes", normalized["defaults"].get("enabled_view_modes", list(VIEW_MODE_OPTIONS))),
                    normalized["defaults"].get("enabled_view_modes", list(VIEW_MODE_OPTIONS)),
                ),
                "default_platform": _normalize_platform_option(
                    raw_cfg.get("default_platform", normalized["defaults"]["default_platform"])
                ),
                "default_view_mode": _normalize_view_mode_option(
                    raw_cfg.get("default_view_mode", normalized["defaults"]["default_view_mode"])
                ),
                "tenant_logo": _normalize_logo_source(
                    raw_cfg.get("tenant_logo", normalized["defaults"].get("tenant_logo", ""))
                ),
                "theme_colors": _normalize_theme_colors(
                    raw_cfg.get(
                        "theme_colors",
                        existing_tenant_cfg.get(
                            "theme_colors",
                            normalized["defaults"].get("theme_colors", DEFAULT_THEME_COLORS),
                        ),
                    ),
                    normalized["defaults"].get("theme_colors", DEFAULT_THEME_COLORS),
                ),
            }
            if tenant_cfg[tenant_id]["default_view_mode"] not in tenant_cfg[tenant_id]["enabled_view_modes"]:
                tenant_cfg[tenant_id]["default_view_mode"] = tenant_cfg[tenant_id]["enabled_view_modes"][0]
        incoming_coco = settings.get("coco_ia", {}) if isinstance(settings, dict) else {}
        normalized_coco = _normalize_coco_ia_settings(
            incoming_coco if isinstance(incoming_coco, dict) and incoming_coco else normalized.get("coco_ia", {}),
            tenants,
        )
        payload = {
            "defaults": normalized["defaults"],
            "tenants": tenant_cfg,
            "coco_ia": normalized_coco,
        }
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
    defaults_campaign_filters = _normalize_campaign_filter_keys(
        defaults.get("campaign_filters", DEFAULT_CAMPAIGN_FILTER_KEYS),
        DEFAULT_CAMPAIGN_FILTER_KEYS,
    )
    defaults_enabled_view_modes = _normalize_view_mode_keys(
        defaults.get("enabled_view_modes", list(VIEW_MODE_OPTIONS)),
        list(VIEW_MODE_OPTIONS),
    )
    defaults_tenant_logo = _normalize_logo_source(defaults.get("tenant_logo", ""))
    defaults_theme_colors = _normalize_theme_colors(
        defaults.get("theme_colors", DEFAULT_THEME_COLORS),
        DEFAULT_THEME_COLORS,
    )
    enabled_view_modes = _normalize_view_mode_keys(
        raw_cfg.get("enabled_view_modes", defaults_enabled_view_modes),
        defaults_enabled_view_modes,
    )
    default_view_mode = _normalize_view_mode_option(
        raw_cfg.get("default_view_mode", defaults.get("default_view_mode", "Overview"))
    )
    if default_view_mode not in enabled_view_modes:
        default_view_mode = enabled_view_modes[0] if enabled_view_modes else "Overview"
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
        "campaign_filters": _normalize_campaign_filter_keys(
            raw_cfg.get("campaign_filters", defaults_campaign_filters),
            defaults_campaign_filters,
        ),
        "enabled_view_modes": enabled_view_modes,
        "default_platform": _normalize_platform_option(raw_cfg.get("default_platform", defaults.get("default_platform", "All"))),
        "default_view_mode": default_view_mode,
        "tenant_logo": _normalize_logo_source(raw_cfg.get("tenant_logo")) or defaults_tenant_logo,
        "theme_colors": _normalize_theme_colors(
            raw_cfg.get("theme_colors", defaults_theme_colors),
            defaults_theme_colors,
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


def _resolve_kpi_keys(
    selected_keys: list[str],
    payload: dict[str, dict[str, str]],
    fallback_keys: list[str],
) -> list[str]:
    valid = [k for k in selected_keys if k in payload]
    if not valid:
        valid = [k for k in fallback_keys if k in payload]
    if not valid:
        valid = [k for k in list(payload.keys())[:4]]
    return valid


def render_kpi_cards(
    selected_keys: list[str],
    payload: dict[str, dict[str, str]],
    fallback_keys: list[str],
    *,
    interactive: bool = False,
    state_key: str = "overview_chart_metric",
    preferred_active_key: str = "spend",
) -> str:
    valid = _resolve_kpi_keys(selected_keys, payload, fallback_keys)
    if not valid:
        return ""
    active_key = str(st.session_state.get(state_key, "")).strip()
    if active_key not in valid:
        active_key = preferred_active_key if preferred_active_key in valid else valid[0]
        st.session_state[state_key] = active_key

    if not interactive:
        cols = st.columns(len(valid))
        for col, key in zip(cols, valid):
            item = payload.get(key, {})
            col.metric(
                str(item.get("label", key)),
                str(item.get("value", "N/A")),
                str(item.get("delta", "N/A")),
                delta_color=str(item.get("delta_color", "normal")),
            )
        return active_key

    active_border = _hex_to_rgba(C_ACCENT, 0.74)
    active_bg_top = _hex_to_rgba(C_ACCENT, 0.16)
    active_glow = _hex_to_rgba(C_ACCENT, 0.22)
    active_shadow = _hex_to_rgba(C_ACCENT, 0.16)
    active_focus = _hex_to_rgba(C_ACCENT, 0.35)
    active_outline = _hex_to_rgba(C_ACCENT, 0.72)
    kpi_cards_css = textwrap.dedent(
        """
        <style>
          .st-key-overview-kpi-wrap { margin-bottom: 0.34rem; }
          [class*="st-key-overview-kpi-card-active-"],
          [class*="st-key-overview-kpi-card-idle-"] {
            position: relative;
          }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stElementContainer"],
          [class*="st-key-overview-kpi-card-idle-"] [data-testid="stElementContainer"] {
            margin: 0 !important;
            padding: 0 !important;
          }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stMetric"],
          [class*="st-key-overview-kpi-card-idle-"] [data-testid="stMetric"] {
            margin: 0 !important;
            border: 1px solid rgba(32,29,29,0.08) !important;
            border-radius: 18px !important;
            padding: 1rem !important;
            min-height: 8.7rem !important;
            box-shadow: 0 12px 28px rgba(15,23,42,0.04);
            transition: border-color 0.14s ease, box-shadow 0.14s ease, transform 0.14s ease, background 0.14s ease;
          }
          [class*="st-key-overview-kpi-card-idle-"]:hover [data-testid="stMetric"] {
            transform: translateY(-1px);
            box-shadow: 0 10px 22px rgba(15,23,42,0.08);
          }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stMetric"] {
            border: 1px solid __ACTIVE_BORDER__ !important;
            background: linear-gradient(180deg, __ACTIVE_BG_TOP__ 0%, rgba(255,255,255,0.90) 100%) !important;
            box-shadow: 0 0 0 1px __ACTIVE_GLOW__, 0 12px 24px __ACTIVE_SHADOW__ !important;
          }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stMetricLabel"] { color: __ACTIVE_ACCENT__ !important; }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stMetricValue"] { color: __ACTIVE_ACCENT__ !important; }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stButton"],
          [class*="st-key-overview-kpi-card-idle-"] [data-testid="stButton"] {
            position: relative !important;
            margin-top: -8.7rem !important;
            margin-bottom: 0 !important;
            height: 8.7rem !important;
            z-index: 5 !important;
          }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stButton"] button,
          [class*="st-key-overview-kpi-card-idle-"] [data-testid="stButton"] button {
            width: 100% !important;
            height: 8.7rem !important;
            min-height: 8.7rem !important;
            opacity: 0 !important;
            border: none !important;
            margin: 0 !important;
            padding: 0 !important;
            background: transparent !important;
            box-shadow: none !important;
            cursor: pointer;
          }
          [class*="st-key-overview-kpi-card-active-"] [data-testid="stButton"] button:focus-visible,
          [class*="st-key-overview-kpi-card-idle-"] [data-testid="stButton"] button:focus-visible {
            opacity: 0.08 !important;
            background: __ACTIVE_FOCUS__ !important;
            outline: 2px solid __ACTIVE_OUTLINE__ !important;
            outline-offset: -2px !important;
          }
        </style>
        """
    )
    kpi_cards_css = (
        kpi_cards_css
        .replace("__ACTIVE_BORDER__", active_border)
        .replace("__ACTIVE_BG_TOP__", active_bg_top)
        .replace("__ACTIVE_GLOW__", active_glow)
        .replace("__ACTIVE_SHADOW__", active_shadow)
        .replace("__ACTIVE_ACCENT__", C_ACCENT)
        .replace("__ACTIVE_FOCUS__", active_focus)
        .replace("__ACTIVE_OUTLINE__", active_outline)
    )
    st.markdown(kpi_cards_css, unsafe_allow_html=True)

    with st.container(key="overview-kpi-wrap"):
        cols = st.columns(len(valid))
        for col, key in zip(cols, valid):
            item = payload.get(key, {})
            state_tag = "active" if key == active_key else "idle"
            with col:
                with st.container(key=f"overview-kpi-card-{state_tag}-{key}"):
                    st.metric(
                        str(item.get("label", key)),
                        str(item.get("value", "N/A")),
                        str(item.get("delta", "N/A")),
                        delta_color=str(item.get("delta_color", "normal")),
                    )
                    st.button(
                        f"Seleccionar {item.get('label', key)}",
                        key=f"{state_key}_pick_{key}",
                        on_click=lambda target=key: st.session_state.__setitem__(state_key, target),
                        use_container_width=True,
                    )

    active_key = str(st.session_state.get(state_key, active_key)).strip()
    if active_key not in valid:
        active_key = preferred_active_key if preferred_active_key in valid else valid[0]
        st.session_state[state_key] = active_key
    return active_key


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
            --login-logo-size: 84px;
          }
          .stApp,
          [data-testid="stAppViewContainer"] {
            background: rgba(122, 135, 157, 0.30) !important;
            background-image: none !important;
          }
          [data-testid="stSidebar"],
          [data-testid="collapsedControl"],
          [data-testid="stSidebarCollapseButton"] {
            display: none !important;
          }
          html,
          body,
          [data-testid="stAppViewContainer"],
          [data-testid="stMain"] {
            height: 100% !important;
          }
          [data-testid="stMainBlockContainer"],
          .block-container {
            max-width: 100% !important;
            min-height: 100vh !important;
            padding-top: 0 !important;
            padding-bottom: 0 !important;
            box-sizing: border-box !important;
          }
          [data-testid="stForm"],
          .stForm {
            background: #FFFFFF !important;
            border: 1px solid rgba(32,29,29,0.07) !important;
            border-radius: 30px !important;
            padding: 1.9rem 1.8rem 1.4rem 1.8rem !important;
            box-shadow: 0 22px 50px rgba(15,23,42,0.10) !important;
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
            width: min(var(--login-card-width), calc(100vw - 2.4rem)) !important;
            max-width: min(var(--login-card-width), calc(100vw - 2.4rem)) !important;
            min-height: 0 !important;
            height: auto !important;
            box-sizing: border-box !important;
            position: fixed !important;
            left: 50% !important;
            top: 50.5% !important;
            transform: translate(-50%, -50%) !important;
            margin: 0 !important;
            z-index: 20 !important;
          }
          [data-testid="stForm"] form,
          .stForm form {
            width: 100% !important;
            max-width: 100% !important;
            margin: 0 !important;
          }
          .login-logo-wrap {
            width: var(--login-logo-size) !important;
            height: var(--login-logo-size) !important;
            max-width: var(--login-logo-size) !important;
            max-height: var(--login-logo-size) !important;
            overflow: hidden;
            flex: 0 0 var(--login-logo-size);
            margin: 0 auto 0.62rem auto;
            display: flex;
            align-items: center;
            justify-content: center;
          }
          .login-logo-wrap img {
            width: 100% !important;
            height: 100% !important;
            max-width: 100% !important;
            max-height: 100% !important;
            object-fit: contain;
            display: block;
          }
          .login-brand {
            display: flex;
            flex-direction: column;
            align-items: center;
            text-align: center;
            margin-bottom: 1.2rem;
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
            position: fixed;
            left: 0;
            right: 0;
            bottom: 0.78rem;
            margin: 0;
            text-align: center;
            color: #FFFFFF;
            font-size: 0.98rem;
            font-weight: 700;
            line-height: 1.35;
            z-index: 1000;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.18);
          }
          .login-footer a {
            color: #FFFFFF;
            text-decoration: none;
            font-weight: 800;
          }
          .login-footer a:hover { text-decoration: underline; }
          @media (max-width: 640px) {
            .block-container {
              padding-top: 1.2rem !important;
            }
            [data-testid="stForm"],
            .stForm {
              position: static !important;
              left: auto !important;
              top: auto !important;
              transform: none !important;
              border-radius: 22px !important;
              width: calc(100vw - 1.3rem) !important;
              padding: 1.1rem !important;
              margin: 0 auto !important;
            }
            .login-title { font-size: 1.95rem; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.form("login_form", clear_on_submit=False):
        login_logo_src = _image_source_to_data_uri(_resolve_logo_image_source("assets/login.png"))
        st.markdown(
            (
                f"<div class='login-logo-wrap'>"
                f"<img src='{html.escape(login_logo_src, quote=True)}' alt='Logo login' "
                "style='width:84px;height:84px;max-width:84px;max-height:84px;object-fit:contain;display:block;' />"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        st.markdown(
            """
            <div class="login-brand">
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
                <span class="login-secure-icon">&#128274;</span>
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
          Powered By <a href="https://ipalmera.com" target="_blank" rel="noopener noreferrer">iPalmera</a> 2026 Vibe Coding
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


def _default_business_date_range(min_d: date, max_d: date) -> tuple[date, date]:
    today = date.today()
    if today.day == 1:
        # On the first day of month, default to the complete previous month.
        end_candidate = today - timedelta(days=1)
        start_candidate = end_candidate.replace(day=1)
    else:
        # Otherwise, current month to yesterday.
        start_candidate = today.replace(day=1)
        end_candidate = today - timedelta(days=1)

    start = _coerce_date_value(start_candidate, min_d, max_d)
    end = _coerce_date_value(end_candidate, min_d, max_d)
    if start > end:
        start = min_d
    return start, end


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


def hourly_df(report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for r in report.get("hourly", []):
        m = r.get("meta", {})
        g = r.get("google_ads", {})
        n = r.get("normalization", {})
        dt_raw = str(r.get("datetime", "")).strip()
        if not dt_raw:
            raw_day = str(r.get("date", "")).strip()
            raw_hour = int(sf(r.get("hour")))
            if raw_day:
                dt_raw = f"{raw_day} {raw_hour:02d}:00:00"
        ms = sf(m.get("spend"))
        gs = sf(g.get("cost"))
        mc = sf(m.get("clicks"))
        gc = sf(g.get("clicks"))
        mv = sf(m.get("conversions"))
        gv = sf(g.get("conversions"))
        mi = sf(m.get("impressions"))
        gi = sf(g.get("impressions"))
        rows.append(
            {
                "timestamp": dt_raw,
                "date": r.get("date"),
                "hour": sf(r.get("hour")),
                "meta_spend": ms,
                "google_spend": gs,
                "total_spend": sf(n.get("total_spend")) or (ms + gs),
                "meta_clicks": mc,
                "google_clicks": gc,
                "total_clicks": sf(n.get("total_clicks")) or (mc + gc),
                "meta_conv": mv,
                "google_conv": gv,
                "total_conv": sf(n.get("total_conversions")) or (mv + gv),
                "meta_impr": mi,
                "google_impr": gi,
                "total_impr": sf(n.get("total_impressions")) or (mi + gi),
                "ga4_sessions": 0.0,
                "ga4_users": 0.0,
                "ga4_avg_sess": 0.0,
                "ga4_bounce": 0.0,
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "date" in df.columns:
        parsed_date = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = parsed_date.dt.date
    else:
        df["date"] = df["timestamp"].dt.date
    if "hour" not in df.columns:
        df["hour"] = df["timestamp"].dt.hour
    else:
        df["hour"] = (
            pd.to_numeric(df["hour"], errors="coerce")
            .fillna(df["timestamp"].dt.hour)
            .fillna(0)
            .astype(int)
        )
    num_cols = [c for c in df.columns if c not in {"timestamp", "date"}]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    return (
        df.dropna(subset=["timestamp", "date"])
        .sort_values(["timestamp"])
        .reset_index(drop=True)
    )


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


def _campaign_filter_values(
    camp_df: pd.DataFrame,
    *,
    field: str,
    platform: str,
    start_day: date,
    end_day: date,
) -> list[str]:
    if camp_df.empty or field not in camp_df.columns:
        return []
    cp = camp_df.copy()
    if "date" in cp.columns:
        cp = cp[(cp["date"] >= start_day) & (cp["date"] <= end_day)]
    if platform in ("Google", "Meta") and "platform" in cp.columns:
        cp = cp[cp["platform"] == platform]
    if cp.empty:
        return []
    values = (
        cp[field]
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": ""})
    )
    out = sorted({v for v in values.tolist() if v})
    return out


def _apply_campaign_filters(camp_df: pd.DataFrame, campaign_filters: dict[str, str]) -> pd.DataFrame:
    if camp_df.empty or not campaign_filters:
        return camp_df
    cp = camp_df.copy()
    for field, selected_value in campaign_filters.items():
        if field not in cp.columns:
            continue
        value = str(selected_value or "").strip()
        if not value:
            continue
        cp = cp[cp[field].astype(str).str.strip() == value]
        if cp.empty:
            break
    return cp


def _campaign_filters_cache_key(campaign_filters: dict[str, str]) -> str:
    if not isinstance(campaign_filters, dict) or not campaign_filters:
        return ""
    clean: dict[str, str] = {}
    for field in sorted(campaign_filters.keys()):
        key = str(field or "").strip()
        value = str(campaign_filters.get(field, "")).strip()
        if key and value:
            clean[key] = value
    return json.dumps(clean, sort_keys=True, ensure_ascii=False) if clean else ""


def _campaign_filters_from_cache_key(filter_key: str) -> dict[str, str]:
    text = str(filter_key or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for raw_key, raw_value in parsed.items():
        key = str(raw_key or "").strip()
        value = str(raw_value or "").strip()
        if key and value:
            out[key] = value
    return out


def _parse_iso_day(raw: Any, fallback: date) -> date:
    text = str(raw or "").strip()
    if not text:
        return fallback
    try:
        return date.fromisoformat(text)
    except Exception:
        return fallback


def _resolve_cached_range(start_iso: str, end_iso: str) -> tuple[date, date]:
    fallback = date(1970, 1, 1)
    start_day = _parse_iso_day(start_iso, fallback)
    end_day = _parse_iso_day(end_iso, fallback)
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    return start_day, end_day


@st.cache_data(show_spinner=False)
def _cached_campaign_filter_values_from_report(
    path_str: str,
    modified_ns: int,
    size_bytes: int,
    field: str,
    platform: str,
    start_iso: str,
    end_iso: str,
) -> list[str]:
    start_day, end_day = _resolve_cached_range(start_iso, end_iso)
    camp_df = _load_campaign_unified_df_cached(path_str, modified_ns, size_bytes)
    return _campaign_filter_values(
        camp_df,
        field=str(field or "").strip(),
        platform=str(platform or "All"),
        start_day=start_day,
        end_day=end_day,
    )


@st.cache_data(show_spinner=False)
def _cached_channels_roll_from_report(
    path_str: str,
    modified_ns: int,
    size_bytes: int,
    start_iso: str,
    end_iso: str,
) -> pd.DataFrame:
    start_day, end_day = _resolve_cached_range(start_iso, end_iso)
    ch = _load_acq_df_cached(path_str, modified_ns, size_bytes, "ga4_channel_daily")
    if ch.empty:
        return pd.DataFrame(columns=["sessionDefaultChannelGroup", "sessions", "conversions"])
    if "date" in ch.columns:
        ch = ch[(ch["date"] >= start_day) & (ch["date"] <= end_day)]
    if ch.empty:
        return pd.DataFrame(columns=["sessionDefaultChannelGroup", "sessions", "conversions"])
    if "sessionDefaultChannelGroup" not in ch.columns:
        ch["sessionDefaultChannelGroup"] = ""
    if "sessions" not in ch.columns:
        ch["sessions"] = 0.0
    if "conversions" not in ch.columns:
        ch["conversions"] = 0.0
    roll = (
        ch.groupby("sessionDefaultChannelGroup", as_index=False)
        .agg(sessions=("sessions", "sum"), conversions=("conversions", "sum"))
        .sort_values("sessions", ascending=False)
    )
    return roll.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _cached_top_pages_roll_from_report(
    path_str: str,
    modified_ns: int,
    size_bytes: int,
    start_iso: str,
    end_iso: str,
) -> pd.DataFrame:
    start_day, end_day = _resolve_cached_range(start_iso, end_iso)
    pg = _load_acq_df_cached(path_str, modified_ns, size_bytes, "ga4_top_pages_daily")
    if pg.empty:
        return pd.DataFrame(columns=["pagePath", "pageTitle", "views", "sessions", "avg_session"])
    if "date" in pg.columns:
        pg = pg[(pg["date"] >= start_day) & (pg["date"] <= end_day)]
    if pg.empty:
        return pd.DataFrame(columns=["pagePath", "pageTitle", "views", "sessions", "avg_session"])
    if "pagePath" not in pg.columns:
        pg["pagePath"] = ""
    if "pageTitle" not in pg.columns:
        pg["pageTitle"] = ""
    if "screenPageViews" not in pg.columns:
        pg["screenPageViews"] = 0.0
    if "sessions" not in pg.columns:
        pg["sessions"] = 0.0
    if "averageSessionDuration" not in pg.columns:
        pg["averageSessionDuration"] = 0.0
    roll = (
        pg.groupby(["pagePath", "pageTitle"], as_index=False)
        .agg(
            views=("screenPageViews", "sum"),
            sessions=("sessions", "sum"),
            avg_session=("averageSessionDuration", "mean"),
        )
        .sort_values("views", ascending=False)
    )
    return roll.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def _cached_campaign_roll_from_report(
    path_str: str,
    modified_ns: int,
    size_bytes: int,
    start_iso: str,
    end_iso: str,
    platform: str,
    filter_key: str,
) -> pd.DataFrame:
    start_day, end_day = _resolve_cached_range(start_iso, end_iso)
    cp = _load_campaign_unified_df_cached(path_str, modified_ns, size_bytes)
    if cp.empty:
        return pd.DataFrame(columns=["platform", "campaign_id", "campaign_name", "spend", "conversions", "cpl"])
    cp = cp[(cp["date"] >= start_day) & (cp["date"] <= end_day)]
    selected_platform = str(platform or "All")
    if selected_platform in {"Google", "Meta"} and "platform" in cp.columns:
        cp = cp[cp["platform"] == selected_platform]
    cp = _apply_campaign_filters(cp, _campaign_filters_from_cache_key(filter_key))
    if cp.empty:
        return pd.DataFrame(columns=["platform", "campaign_id", "campaign_name", "spend", "conversions", "cpl"])

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

    roll = (
        cp.groupby(["platform", "campaign_id", "campaign_name"], as_index=False)
        .agg(
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            conversions=("conversions", "sum"),
            ctr=("ctr", "mean"),
            cpc=("cpc", "mean"),
            reach=("reach", "max"),
            frequency=("frequency", "mean"),
        )
        .sort_values("spend", ascending=False)
        .reset_index(drop=True)
    )
    roll["cpl"] = roll.apply(lambda r: sdiv(float(r["spend"]), float(r["conversions"])), axis=1)
    return roll


@st.cache_data(show_spinner=False)
def _cached_top_pieces_roll_from_report(
    path_str: str,
    modified_ns: int,
    size_bytes: int,
    start_iso: str,
    end_iso: str,
    platform: str,
    filter_key: str,
) -> pd.DataFrame:
    start_day, end_day = _resolve_cached_range(start_iso, end_iso)
    selected_platform = str(platform or "All")
    filters = _campaign_filters_from_cache_key(filter_key)

    piece_df = _load_piece_enriched_df_cached(path_str, modified_ns, size_bytes)
    cp = piece_df.copy()
    if not cp.empty and "date" in cp.columns:
        cp["date"] = pd.to_datetime(cp["date"], errors="coerce").dt.date
        cp = cp.dropna(subset=["date"])
        cp = cp[(cp["date"] >= start_day) & (cp["date"] <= end_day)]
        if selected_platform in {"Google", "Meta"} and "platform" in cp.columns:
            cp = cp[cp["platform"] == selected_platform]
        cp = _apply_campaign_filters(cp, filters)

    if cp.empty:
        campaign_roll = _cached_campaign_roll_from_report(
            path_str,
            modified_ns,
            size_bytes,
            start_iso,
            end_iso,
            selected_platform,
            filter_key,
        )
        if campaign_roll.empty:
            return pd.DataFrame(
                columns=[
                    "platform",
                    "campaign_id",
                    "campaign_name",
                    "piece_id",
                    "piece_name",
                    "inversion",
                    "conversiones",
                    "clics",
                    "vista_previa",
                    "cpl",
                ]
            )
        out = campaign_roll.copy()
        if "campaign_id" in out.columns:
            out["campaign_id"] = out["campaign_id"].astype(str)
        else:
            out["campaign_id"] = ""
        if "campaign_name" in out.columns:
            out["campaign_name"] = out["campaign_name"].astype(str)
        else:
            out["campaign_name"] = ""
        out["piece_id"] = out["campaign_id"].astype(str)
        out["piece_name"] = out["campaign_name"].astype(str)
        out["inversion"] = pd.to_numeric(out["spend"], errors="coerce").fillna(0.0)
        out["conversiones"] = pd.to_numeric(out["conversions"], errors="coerce").fillna(0.0)
        out["clics"] = pd.to_numeric(out["clicks"], errors="coerce").fillna(0.0)
        out["vista_previa"] = ""
        out = out.sort_values(["conversiones", "clics"], ascending=[False, False], na_position="last").head(10)
        out["cpl"] = out.apply(lambda r: sdiv(float(r["inversion"]), float(r["conversiones"])), axis=1)
        return out.reset_index(drop=True)

    required_defaults: dict[str, Any] = {
        "platform": "",
        "campaign_id": "",
        "campaign_name": "",
        "piece_id": "",
        "piece_name": "",
        "preview_url": "",
        "image_url": "",
        "thumbnail_url": "",
        "spend": 0.0,
        "clicks": 0.0,
        "conversions": 0.0,
    }
    for col, default in required_defaults.items():
        if col not in cp.columns:
            cp[col] = default
    for num_col in ("spend", "clicks", "conversions"):
        cp[num_col] = pd.to_numeric(cp[num_col], errors="coerce").fillna(0.0)
    cp["piece_id"] = cp["piece_id"].astype(str).str.strip()
    cp["piece_name"] = cp["piece_name"].astype(str).str.strip()
    cp["piece_id"] = cp["piece_id"].mask(cp["piece_id"] == "", cp["campaign_id"].astype(str))
    cp["piece_name"] = cp["piece_name"].mask(cp["piece_name"] == "", cp["campaign_name"].astype(str))
    cp["piece_name"] = cp["piece_name"].replace({"": "Sin nombre"})
    for col in ("preview_url", "image_url", "thumbnail_url"):
        cp[col] = (
            cp[col]
            .astype(str)
            .str.strip()
            .replace({"nan": "", "none": "", "None": "", "null": "", "NULL": ""})
        )
    cp["vista_previa"] = (
        cp[["preview_url", "image_url", "thumbnail_url"]]
        .replace({"": pd.NA})
        .bfill(axis=1)
        .iloc[:, 0]
        .fillna("")
    )

    def _first_non_empty_value(series: pd.Series) -> str:
        for raw in series.tolist():
            txt = str(raw or "").strip()
            if txt and txt.lower() not in {"nan", "none", "null"}:
                return txt
        return ""

    roll = (
        cp.groupby(
            ["platform", "piece_id", "piece_name", "campaign_id", "campaign_name"],
            as_index=False,
        )
        .agg(
            inversion=("spend", "sum"),
            conversiones=("conversions", "sum"),
            clics=("clicks", "sum"),
            vista_previa=("vista_previa", _first_non_empty_value),
        )
        .sort_values(["conversiones", "clics"], ascending=[False, False], na_position="last")
        .head(10)
        .reset_index(drop=True)
    )
    roll["cpl"] = roll.apply(lambda r: sdiv(float(r["inversion"]), float(r["conversiones"])), axis=1)
    return roll


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


def _normalize_age_bucket(raw_value: Any) -> str:
    txt = str(raw_value or "").strip()
    if not txt:
        return "Unknown"
    up = txt.upper().replace("AGE_RANGE_", "")
    mapped = {
        "18_24": "18-24",
        "18-24": "18-24",
        "25_34": "25-34",
        "25-34": "25-34",
        "35_44": "35-44",
        "35-44": "35-44",
        "45_54": "45-54",
        "45-54": "45-54",
        "55_64": "55-64",
        "55-64": "55-64",
        "65_UP": "65+",
        "65+": "65+",
        "UNKNOWN": "Unknown",
        "UNDETERMINED": "Unknown",
    }.get(up)
    return mapped if mapped else txt


def _normalize_gender_bucket(raw_value: Any) -> str:
    txt = str(raw_value or "").strip().lower()
    if txt in {"female", "f"}:
        return "Female"
    if txt in {"male", "m"}:
        return "Male"
    return "Unknown"


_EMPTY_TEXT_TOKENS = {"nan", "none", "null", "nat", "<na>", "n/a", "na"}


def _clean_text_value(raw_value: Any, default: str = "") -> str:
    if raw_value is None:
        return default
    try:
        if bool(pd.isna(raw_value)):
            return default
    except Exception:
        pass
    txt = str(raw_value).strip()
    if not txt:
        return default
    if txt.casefold() in _EMPTY_TEXT_TOKENS:
        return default
    return txt


def _country_name_from_code(raw_value: Any) -> str:
    code = _clean_text_value(raw_value).upper()
    return COUNTRY_CODE_TO_NAME.get(code, "")


def paid_lead_demographics_df(report: dict[str, Any]) -> pd.DataFrame:
    df = acq_df(report, "paid_lead_demographics_daily")
    if df.empty:
        return df
    required = ["platform", "breakdown", "age_range", "gender", "leads", "spend", "impressions", "clicks"]
    for col in required:
        if col not in df.columns:
            if col in ("platform", "breakdown", "age_range", "gender"):
                df[col] = ""
            else:
                df[col] = 0.0
    for col in ("leads", "spend", "impressions", "clicks"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["platform"] = df["platform"].astype(str).str.strip().replace({"": "Meta"})
    df["breakdown"] = (
        df["breakdown"]
        .astype(str)
        .str.strip()
        .replace({"": "age_gender"})
        .str.lower()
    )
    valid_breakdowns = {"age_gender", "age", "gender"}
    df.loc[~df["breakdown"].isin(valid_breakdowns), "breakdown"] = "age_gender"
    df["age_range"] = df["age_range"].apply(_normalize_age_bucket)
    df["gender"] = df["gender"].apply(_normalize_gender_bucket)
    return df


def paid_lead_geo_df(report: dict[str, Any]) -> pd.DataFrame:
    df = acq_df(report, "paid_lead_geo_daily")
    if df.empty:
        return df
    if "country_code" not in df.columns and "country" in df.columns:
        df["country_code"] = df["country"]
    required = ["platform", "country_code", "country_name", "region", "leads", "spend", "impressions", "clicks"]
    for col in required:
        if col not in df.columns:
            if col in ("platform", "country_code", "country_name", "region"):
                df[col] = ""
            else:
                df[col] = 0.0
    for col in ("leads", "spend", "impressions", "clicks"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["platform"] = df["platform"].apply(lambda v: _clean_text_value(v, "Meta") or "Meta")
    df["country_code"] = df["country_code"].apply(_clean_text_value).str.upper()
    df["country_name"] = df["country_name"].apply(_clean_text_value)
    df["country_name"] = df.apply(
        lambda r: _clean_text_value(r.get("country_name")) or _country_name_from_code(r.get("country_code")),
        axis=1,
    )
    df["region"] = df["region"].apply(lambda v: _clean_text_value(v, "Unknown") or "Unknown")
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
        "cvr": sdiv(conv, clicks),
        "cpc": sdiv(spend, clicks),
        "cpm": (spend * 1000.0 / impr) if impr > 0 else None,
        "sessions": sessions_total,
        "users": float(df["ga4_users"].sum()) if not df.empty else 0.0,
        # GA4-consistent: weighted by sessions across the selected period.
        "avg_sess": avg_sess_weighted,
        "bounce": float(df["ga4_bounce"].mean()) if not df.empty else 0.0,
    }


PAID_TREND_KPI_KEYS = {"spend", "conv", "clicks", "impr", "cpl", "ctr", "cvr", "cpc", "cpm"}


def _series_num(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(0.0, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(0.0)


def _series_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    den = pd.to_numeric(denominator, errors="coerce")
    return pd.to_numeric(numerator, errors="coerce").div(den.where(den != 0)).fillna(0.0)


def _platform_kpi_series(df: pd.DataFrame, prefix: str, kpi_key: str) -> pd.Series | None:
    spend = _series_num(df, f"{prefix}_spend")
    clicks = _series_num(df, f"{prefix}_clicks")
    conv = _series_num(df, f"{prefix}_conv")
    impr = _series_num(df, f"{prefix}_impr")
    if kpi_key == "spend":
        return spend
    if kpi_key == "conv":
        return conv
    if kpi_key == "clicks":
        return clicks
    if kpi_key == "impr":
        return impr
    if kpi_key == "cpl":
        return _series_divide(spend, conv)
    if kpi_key == "ctr":
        return _series_divide(clicks, impr)
    if kpi_key == "cvr":
        return _series_divide(conv, clicks)
    if kpi_key == "cpc":
        return _series_divide(spend, clicks)
    if kpi_key == "cpm":
        return _series_divide(spend * 1000.0, impr)
    return None


def _ga4_kpi_series(df: pd.DataFrame, kpi_key: str) -> pd.Series | None:
    if kpi_key == "sessions":
        return _series_num(df, "ga4_sessions")
    if kpi_key == "users":
        return _series_num(df, "ga4_users")
    if kpi_key == "avg_sess":
        return _series_num(df, "ga4_avg_sess")
    if kpi_key == "bounce":
        return _series_num(df, "ga4_bounce")
    return None


def _kpi_axis_title(kpi_key: str) -> str:
    label = str(KPI_CATALOG.get(kpi_key, {}).get("label", kpi_key))
    fmt = str(KPI_CATALOG.get(kpi_key, {}).get("fmt", "int"))
    if fmt == "money":
        return f"{label} ($)"
    if fmt == "pct":
        return f"{label} (%)"
    if fmt == "duration":
        return f"{label} (s)"
    return label


def _kpi_hover_value_template(kpi_key: str) -> str:
    fmt = str(KPI_CATALOG.get(kpi_key, {}).get("fmt", "int"))
    if fmt == "money":
        return "$%{y:,.2f}"
    if fmt == "pct":
        return "%{y:.2%}"
    if fmt == "duration":
        return "%{y:.0f} s"
    return "%{y:,.0f}"


def _kpi_trend_subtitle(kpi_key: str) -> str:
    if kpi_key == "spend":
        return "Daily investment over time"
    label = str(KPI_CATALOG.get(kpi_key, {}).get("label", kpi_key))
    return f"Tendencia diaria de {label.lower()}"


def render_sidebar(
    tenants: dict[str, dict[str, Any]],
    dashboard_settings: dict[str, Any],
) -> tuple[str, str, str]:
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
    current_tenant_dash_cfg = tenant_dashboard_settings(dashboard_settings, current_tenant_id)
    current_tenant_logo = _resolve_logo_image_source(
        current_tenant_dash_cfg.get(
            "tenant_logo",
            tenants.get(current_tenant_id, {}).get("logo", ""),
        )
    )
    try:
        st.sidebar.image(current_tenant_logo, width=SIDEBAR_LOGO_RENDER_WIDTH_PX)
    except Exception:
        st.sidebar.image(_resolve_logo_image_source(""), width=SIDEBAR_LOGO_RENDER_WIDTH_PX)
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
        """,
        unsafe_allow_html=True,
    )
    if len(tenant_ids) > 1:
        st.sidebar.markdown("<div class='sidebar-kicker'>Workspace</div>", unsafe_allow_html=True)
        tenant_id = st.sidebar.selectbox(
            "Workspace",
            options=tenant_ids,
            key="active_tenant_id",
            format_func=lambda t: str(tenants.get(t, {}).get("name", t)),
            label_visibility="collapsed",
        )
    else:
        tenant_id = tenant_ids[0]
        if st.session_state.get("active_tenant_id") != tenant_id:
            st.session_state["active_tenant_id"] = tenant_id
    tenant_dash_cfg = tenant_dashboard_settings(dashboard_settings, tenant_id)
    enabled_view_modes = _normalize_view_mode_keys(
        tenant_dash_cfg.get("enabled_view_modes", list(VIEW_MODE_OPTIONS)),
        list(VIEW_MODE_OPTIONS),
    )
    if "sidebar_view_mode" not in st.session_state:
        st.session_state["sidebar_view_mode"] = enabled_view_modes[0]
    view_mode = str(st.session_state.get("sidebar_view_mode", "Overview"))

    def _set_view_mode(mode: str) -> None:
        if str(st.session_state.get("sidebar_view_mode", "")) != mode:
            st.session_state["sidebar_view_mode"] = mode
            st.rerun()

    if view_mode == "Administración" and not is_admin_user:
        st.session_state["sidebar_view_mode"] = enabled_view_modes[0]
        view_mode = enabled_view_modes[0]
    if view_mode not in enabled_view_modes and view_mode != "Administración":
        st.session_state["sidebar_view_mode"] = enabled_view_modes[0]
        view_mode = enabled_view_modes[0]
    if "Overview" in enabled_view_modes and st.sidebar.button(
        "Overview",
        key="nav_overview_btn",
        icon=":material/dashboard:",
        type="primary" if view_mode == "Overview" else "secondary",
        width="stretch",
    ):
        _set_view_mode("Overview")
    if "Tráfico y Adquisición" in enabled_view_modes and st.sidebar.button(
        "Tráfico y Adquisición",
        key="nav_traffic_btn",
        icon=":material/analytics:",
        type="primary" if view_mode == "Tráfico y Adquisición" else "secondary",
        width="stretch",
    ):
        _set_view_mode("Tráfico y Adquisición")
    if is_admin_user and st.sidebar.button(
        "Administración",
        key="nav_admin_btn",
        icon=":material/admin_panel_settings:",
        type="primary" if view_mode == "Administración" else "secondary",
        width="stretch",
    ):
        _set_view_mode("Administración")
    admin_section = str(st.session_state.get("admin_panel_section", "users")).strip().lower() or "users"
    if admin_section not in ADMIN_SECTION_OPTIONS:
        admin_section = "users"
        st.session_state["admin_panel_section"] = admin_section
    if is_admin_user and view_mode == "Administración":
        admin_options = list(ADMIN_SECTION_OPTIONS.keys())
        admin_selected = st.sidebar.radio(
            "Admin Section",
            options=admin_options,
            index=admin_options.index(admin_section) if admin_section in admin_options else 0,
            key="admin_panel_section",
            format_func=lambda key: ADMIN_SECTION_MENU_LABELS.get(key, key),
            label_visibility="collapsed",
        )
        admin_section = str(admin_selected).strip().lower() or "users"
        _inject_sidebar_admin_active_state_style(admin_section)
    return tenant_id, view_mode, admin_section


def _inject_sidebar_admin_active_state_style(active_section: str) -> None:
    admin_options = list(ADMIN_SECTION_OPTIONS.keys())
    try:
        active_idx = admin_options.index(active_section) + 1
    except Exception:
        active_idx = 1
    active_bg = _hex_to_rgba(C_ACCENT, 0.18)
    active_shadow = _hex_to_rgba(C_ACCENT, 0.76)
    active_border = _hex_to_rgba(C_ACCENT, 0.82)
    active_grad_top = _hex_to_rgba(C_ACCENT, 0.35)
    active_grad_bottom = _hex_to_rgba(C_ACCENT, 0.24)

    css = textwrap.dedent(
        """
        <style>
          /* Stable active state for admin submenu across Streamlit DOM versions. */
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(__ACTIVE_IDX__),
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > div:nth-child(__ACTIVE_IDX__) > label {
            color: __ACCENT__ !important;
            font-weight: 800 !important;
            background: __ACTIVE_BG__ !important;
            border-radius: 8px !important;
            box-shadow: inset 2px 0 0 __ACTIVE_SHADOW__;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(__ACTIVE_IDX__)::before,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > div:nth-child(__ACTIVE_IDX__) > label::before {
            color: __ACCENT__ !important;
            border-color: __ACTIVE_BORDER__ !important;
            background: linear-gradient(180deg, __ACTIVE_GRAD_TOP__ 0%, __ACTIVE_GRAD_BOTTOM__ 100%) !important;
          }
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(__ACTIVE_IDX__) [data-testid="stMarkdownContainer"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > label:nth-child(__ACTIVE_IDX__) [data-testid="stMarkdownContainer"] *,
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > div:nth-child(__ACTIVE_IDX__) > label [data-testid="stMarkdownContainer"],
          [data-testid="stSidebar"] .stRadio [role="radiogroup"] > div:nth-child(__ACTIVE_IDX__) > label [data-testid="stMarkdownContainer"] * {
            color: __ACCENT__ !important;
            font-weight: 800 !important;
          }
        </style>
        """
    )
    css = (
        css
        .replace("__ACTIVE_IDX__", str(active_idx))
        .replace("__ACCENT__", C_ACCENT)
        .replace("__ACTIVE_BG__", active_bg)
        .replace("__ACTIVE_SHADOW__", active_shadow)
        .replace("__ACTIVE_BORDER__", active_border)
        .replace("__ACTIVE_GRAD_TOP__", active_grad_top)
        .replace("__ACTIVE_GRAD_BOTTOM__", active_grad_bottom)
    )
    st.markdown(css, unsafe_allow_html=True)


def render_sidebar_logout_button() -> None:
    st.sidebar.markdown("<div class='sidebar-bottom'></div>", unsafe_allow_html=True)
    if st.sidebar.button("Logout", key="sidebar_logout_btn", icon=":material/logout:", width="stretch"):
        for k in (
            "auth_user",
            "sidebar_view_mode",
            "admin_panel_section",
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
    tenant_logo_source: str,
    camp_df: pd.DataFrame,
    campaign_filter_keys: list[str],
    report_cache_sig: tuple[str, int, int] | None = None,
) -> tuple[date, date, str, dict[str, str], str, date, date, str]:
    return dashboard_filters.render_top_filters(
        min_d=min_d,
        max_d=max_d,
        tenant_name=tenant_name,
        tenant_id=tenant_id,
        default_platform=default_platform,
        tenant_logo_source=tenant_logo_source,
        camp_df=camp_df,
        campaign_filter_keys=campaign_filter_keys,
        report_cache_sig=report_cache_sig,
        platform_options=PLATFORM_OPTIONS,
        date_preset_options=DATE_PRESET_OPTIONS,
        date_preset_labels=DATE_PRESET_LABELS,
        date_preset_all_options=DATE_PRESET_OPTIONS,
        compare_mode_options=COMPARE_MODE_OPTIONS,
        compare_mode_labels=COMPARE_MODE_LABELS,
        campaign_filter_options=CAMPAIGN_FILTER_OPTIONS,
        normalize_platform_option=_normalize_platform_option,
        normalize_campaign_filter_keys=_normalize_campaign_filter_keys,
        campaign_filter_values=_campaign_filter_values,
        cached_campaign_filter_values_from_report=_cached_campaign_filter_values_from_report,
    )


def render_exec(
    df_sel: pd.DataFrame,
    df_prev: pd.DataFrame,
    platform: str,
    overview_kpi_keys: list[str],
    overview_section_keys: list[str],
    paid_dev_df: pd.DataFrame,
    lead_demo_df: pd.DataFrame,
    lead_geo_df: pd.DataFrame,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    ga4_event_df: pd.DataFrame,
    ga4_conversion_event_name: str,
    tenant_meta_account_id: str,
    tenant_google_customer_id: str,
    campaign_filters: dict[str, str],
    s,
    e,
    prev_s,
    prev_e,
    overview_chart_state_key: str,
    hourly_sel: pd.DataFrame | None = None,
    report_cache_sig: tuple[str, int, int] | None = None,
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
    valid_overview_kpis = _resolve_kpi_keys(overview_kpi_keys, kpi_payload, DEFAULT_OVERVIEW_KPI_KEYS)
    active_overview_kpi = str(st.session_state.get(overview_chart_state_key, "")).strip()
    if active_overview_kpi not in valid_overview_kpis:
        active_overview_kpi = "spend" if "spend" in valid_overview_kpis else (valid_overview_kpis[0] if valid_overview_kpis else "spend")
        st.session_state[overview_chart_state_key] = active_overview_kpi
    if "kpis" in section_set:
        active_overview_kpi = render_kpi_cards(
            overview_kpi_keys,
            kpi_payload,
            DEFAULT_OVERVIEW_KPI_KEYS,
            interactive=True,
            state_key=overview_chart_state_key,
            preferred_active_key=active_overview_kpi,
        )

    c = metric_cols(platform)

    def _render_trend_chart() -> None:
        trend_label = str(KPI_CATALOG.get(active_overview_kpi, {}).get("label", active_overview_kpi))
        hover_value_template = _kpi_hover_value_template(active_overview_kpi)
        ld = df_sel.sort_values("date").copy()
        hd = hourly_sel.copy() if isinstance(hourly_sel, pd.DataFrame) else pd.DataFrame()
        if not hd.empty:
            if "timestamp" in hd.columns:
                hd["timestamp"] = pd.to_datetime(hd["timestamp"], errors="coerce")
            if "date" in hd.columns:
                hd["date"] = pd.to_datetime(hd["date"], errors="coerce").dt.date
        is_single_day = bool(not ld.empty and ld["date"].nunique() == 1)
        hd_day = pd.DataFrame()
        if is_single_day and not hd.empty and "date" in hd.columns:
            selected_day = ld["date"].iloc[0]
            hd_day = hd[hd["date"] == selected_day].copy()
        use_hourly_real = bool(
            is_single_day
            and active_overview_kpi in PAID_TREND_KPI_KEYS
            and not hd_day.empty
            and "timestamp" in hd_day.columns
        )
        trend_subtitle = (
            ("Hourly investment over time" if active_overview_kpi == "spend" else f"Tendencia por hora de {trend_label.lower()}")
            if use_hourly_real
            else ("Vista por hora del dia seleccionado" if is_single_day else _kpi_trend_subtitle(active_overview_kpi))
        )
        st.markdown(
            f"""
            <div class="viz-card">
              <p class="viz-title">Performance Across Platforms</p>
              <div class="viz-sub">{html.escape(trend_subtitle)}</div>
            """,
            unsafe_allow_html=True,
        )
        if ld.empty:
            st.info("Sin datos para el periodo seleccionado.")
        else:
            fig = go.Figure()
            traces_added = 0
            additive_kpis = {"spend", "conv", "clicks", "impr", "sessions", "users"}
            plot_df = hd_day.sort_values("timestamp").copy() if use_hourly_real else ld
            x_values: pd.Series | pd.DatetimeIndex = plot_df["timestamp"] if use_hourly_real else ld["date"]
            x_tick_format = "%H:%M" if is_single_day else "%d %b"
            x_hover_format = "%d %b %Y %H:%M" if is_single_day else "%d %b %Y"
            hourly_projection_note = ""
            if is_single_day and not use_hourly_real:
                selected_day = ld["date"].iloc[0]
                day_start = datetime.combine(selected_day, datetime.min.time())
                x_values = pd.date_range(start=day_start, periods=24, freq="h")
                hourly_projection_note = (
                    "Vista horaria proyectada con distribucion uniforme del total diario "
                    "(no hay datos horarios disponibles en el JSON actual)."
                )

            def _series_for_plot(series: pd.Series | None) -> pd.Series | None:
                if series is None:
                    return None
                numeric_series = pd.to_numeric(series, errors="coerce").fillna(0.0)
                if use_hourly_real or not is_single_day:
                    return numeric_series
                if numeric_series.empty:
                    return pd.Series([0.0] * 24)
                if active_overview_kpi in additive_kpis:
                    base_value = float(numeric_series.sum())
                    projected_hour = base_value / 24.0
                else:
                    base_value = float(numeric_series.mean())
                    projected_hour = base_value
                return pd.Series([projected_hour] * 24)

            if active_overview_kpi in PAID_TREND_KPI_KEYS:
                if platform in ("All", "Google"):
                    google_series = _platform_kpi_series(plot_df, "google", active_overview_kpi)
                    google_plot_series = _series_for_plot(google_series)
                    if google_plot_series is not None:
                        fig.add_trace(
                            go.Scatter(
                                x=x_values,
                                y=google_plot_series,
                                mode="lines+markers" if is_single_day else "lines",
                                name="Google Ads",
                                line={"color": C_GOOGLE, "width": 4, "shape": "linear"},
                                hovertemplate=f"%{{x|{x_hover_format}}}<br>Google: {hover_value_template}<extra></extra>",
                            )
                        )
                        traces_added += 1
                if platform in ("All", "Meta"):
                    meta_series = _platform_kpi_series(plot_df, "meta", active_overview_kpi)
                    meta_plot_series = _series_for_plot(meta_series)
                    if meta_plot_series is not None:
                        fig.add_trace(
                            go.Scatter(
                                x=x_values,
                                y=meta_plot_series,
                                mode="lines+markers" if is_single_day else "lines",
                                name="Meta Ads",
                                line={"color": C_META, "width": 4, "shape": "linear"},
                                hovertemplate=f"%{{x|{x_hover_format}}}<br>Meta: {hover_value_template}<extra></extra>",
                            )
                        )
                        traces_added += 1
            else:
                ga4_series = _ga4_kpi_series(ld, active_overview_kpi)
                ga4_plot_series = _series_for_plot(ga4_series)
                if ga4_plot_series is not None:
                    fig.add_trace(
                        go.Scatter(
                            x=x_values,
                            y=ga4_plot_series,
                            mode="lines+markers" if is_single_day else "lines",
                            name="GA4",
                            line={"color": C_ACCENT, "width": 4, "shape": "linear"},
                            hovertemplate=f"%{{x|{x_hover_format}}}<br>{html.escape(trend_label)}: {hover_value_template}<extra></extra>",
                        )
                    )
                    traces_added += 1
            if traces_added == 0:
                st.info("No hay datos de tendencia para la métrica seleccionada.")
            else:
                pbi_layout(fig, yaxis_title=_kpi_axis_title(active_overview_kpi), xaxis_title="")
                fig.update_layout(height=355, hovermode="x unified", showlegend=True)
                fig.update_xaxes(tickformat=x_tick_format, tickfont={"size": 10, "color": C_MUTE})
                if is_single_day:
                    fig.update_xaxes(dtick=2 * 60 * 60 * 1000)
                fig.update_yaxes(tickfont={"size": 10, "color": C_MUTE})
                fmt = str(KPI_CATALOG.get(active_overview_kpi, {}).get("fmt", "int"))
                if fmt == "pct":
                    fig.update_yaxes(tickformat=".1%")
                elif fmt == "money":
                    fig.update_yaxes(tickprefix="$", separatethousands=True)
                else:
                    fig.update_yaxes(tickformat=",.0f")
                st.plotly_chart(fig, width="stretch")
                if hourly_projection_note:
                    st.caption(hourly_projection_note)
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
            funnel_stage_colors = [C_GOOGLE, C_ACCENT, C_MUTE, C_META]
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

    def _render_media_mix() -> None:
        rows: list[dict[str, float | str | None]] = []
        if platform in ("All", "Meta"):
            meta_spend = float(df_sel["meta_spend"].sum()) if not df_sel.empty else 0.0
            meta_clicks = float(df_sel["meta_clicks"].sum()) if not df_sel.empty else 0.0
            meta_impr = float(df_sel["meta_impr"].sum()) if not df_sel.empty else 0.0
            meta_conv = float(df_sel["meta_conv"].sum()) if not df_sel.empty else 0.0
            if meta_spend > 0 or meta_clicks > 0 or meta_impr > 0:
                rows.append(
                    {
                        "platform": "Meta",
                        "spend": meta_spend,
                        "clicks": meta_clicks,
                        "impressions": meta_impr,
                        "conversions": meta_conv,
                        "cpc": sdiv(meta_spend, meta_clicks),
                        "cpm": (meta_spend * 1000.0 / meta_impr) if meta_impr > 0 else None,
                        "cvr": sdiv(meta_conv, meta_clicks),
                    }
                )
        if platform in ("All", "Google"):
            google_spend = float(df_sel["google_spend"].sum()) if not df_sel.empty else 0.0
            google_clicks = float(df_sel["google_clicks"].sum()) if not df_sel.empty else 0.0
            google_impr = float(df_sel["google_impr"].sum()) if not df_sel.empty else 0.0
            google_conv = float(df_sel["google_conv"].sum()) if not df_sel.empty else 0.0
            if google_spend > 0 or google_clicks > 0 or google_impr > 0:
                rows.append(
                    {
                        "platform": "Google",
                        "spend": google_spend,
                        "clicks": google_clicks,
                        "impressions": google_impr,
                        "conversions": google_conv,
                        "cpc": sdiv(google_spend, google_clicks),
                        "cpm": (google_spend * 1000.0 / google_impr) if google_impr > 0 else None,
                        "cvr": sdiv(google_conv, google_clicks),
                    }
                )
        mix = pd.DataFrame(rows)
        if mix.empty:
            st.info("No hay datos suficientes para Mix y Eficiencia Paid.")
            return
        mix["spend"] = pd.to_numeric(mix["spend"], errors="coerce").fillna(0.0)
        total_spend = float(mix["spend"].sum())
        mix["spend_share"] = mix["spend"].apply(lambda v: (v / total_spend) if total_spend > 0 else 0.0)

        st.markdown(
            "<div class='viz-title' style='margin-bottom:0.35rem;'>4) Mix y Eficiencia Paid (CPC / CPM / CVR)</div>",
            unsafe_allow_html=True,
        )
        pie_col, combo_col = st.columns([1.05, 1.95], gap="large")
        color_map = {"Google": C_GOOGLE, "Meta": C_META}
        with pie_col:
            pie = go.Figure(
                go.Pie(
                    labels=mix["platform"],
                    values=mix["spend"],
                    hole=0.56,
                    marker={"colors": [color_map.get(str(p), C_MUTE) for p in mix["platform"]]},
                    texttemplate="%{label}<br>%{percent}",
                    hovertemplate="%{label}<br>Spend: $%{value:,.2f}<extra></extra>",
                )
            )
            pie.update_layout(
                title={"text": "Mix de Inversión", "font": {"size": 14, "color": C_TEXT}},
                margin={"l": 4, "r": 4, "t": 42, "b": 6},
                height=290,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
            )
            st.plotly_chart(pie, width="stretch")
        with combo_col:
            combo = go.Figure()
            combo.add_trace(
                go.Bar(
                    x=mix["platform"],
                    y=mix["cpc"],
                    name="CPC",
                    marker={"color": C_ACCENT},
                    hovertemplate="%{x}<br>CPC: $%{y:,.2f}<extra></extra>",
                )
            )
            combo.add_trace(
                go.Bar(
                    x=mix["platform"],
                    y=mix["cpm"],
                    name="CPM",
                    marker={"color": C_MUTE},
                    hovertemplate="%{x}<br>CPM: $%{y:,.2f}<extra></extra>",
                )
            )
            combo.add_trace(
                go.Scatter(
                    x=mix["platform"],
                    y=(mix["cvr"] * 100.0),
                    name="CVR",
                    mode="lines+markers",
                    marker={"color": C_META, "size": 9},
                    line={"color": C_META, "width": 3},
                    yaxis="y2",
                    hovertemplate="%{x}<br>CVR: %{y:.2f}%<extra></extra>",
                )
            )
            combo.update_layout(
                barmode="group",
                title={"text": "Eficiencia por Plataforma", "font": {"size": 14, "color": C_TEXT}},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin={"l": 10, "r": 10, "t": 42, "b": 10},
                height=290,
                legend={"orientation": "h", "x": 0.0, "y": 1.12},
                xaxis={"title": "", "tickfont": {"size": 11, "color": C_MUTE}},
                yaxis={"title": "Costo ($)", "gridcolor": C_GRID, "tickfont": {"size": 11, "color": C_MUTE}},
                yaxis2={
                    "title": "CVR (%)",
                    "overlaying": "y",
                    "side": "right",
                    "showgrid": False,
                    "tickfont": {"size": 11, "color": C_MUTE},
                },
            )
            st.plotly_chart(combo, width="stretch")

        mix_view = mix.rename(
            columns={
                "platform": "Plataforma",
                "spend": "Inversión",
                "spend_share": "Share Spend",
                "cpc": "CPC",
                "cpm": "CPM",
                "cvr": "CVR",
                "clicks": "Clics",
                "impressions": "Impresiones",
                "conversions": "Conversiones",
            }
        )[["Plataforma", "Inversión", "Share Spend", "CPC", "CPM", "CVR", "Clics", "Impresiones", "Conversiones"]]
        st.dataframe(
            mix_view.style.format(
                {
                    "Inversión": lambda v: fmt_money(float(v)),
                    "Share Spend": lambda v: fmt_pct(float(v)),
                    "CPC": lambda v: fmt_money(v if pd.notna(v) else None),
                    "CPM": lambda v: fmt_money(v if pd.notna(v) else None),
                    "CVR": lambda v: fmt_pct(v if pd.notna(v) else None),
                    "Clics": "{:.0f}",
                    "Impresiones": "{:.0f}",
                    "Conversiones": "{:.2f}",
                }
            ),
            width="stretch",
            hide_index=True,
        )

    def _render_lead_demographics() -> None:
        st.markdown(
            "<div class='viz-title' style='margin-bottom:0.35rem;'>5) Distribución de Leads Paid por Edad y Género</div>",
            unsafe_allow_html=True,
        )
        st.caption(
            "Métrica de análisis (breakdown de plataformas). No es comparable 1:1 con el KPI global de Conversiones."
        )
        if lead_demo_df.empty:
            st.info("No hay datos de leads por edad y género para el tenant.")
            return

        dcur = lead_demo_df[(lead_demo_df["date"] >= s) & (lead_demo_df["date"] <= e)].copy()
        dprev = lead_demo_df[(lead_demo_df["date"] >= prev_s) & (lead_demo_df["date"] <= prev_e)].copy()
        if platform in ("Google", "Meta"):
            dcur = dcur[dcur["platform"] == platform]
            dprev = dprev[dprev["platform"] == platform]
        if dcur.empty:
            st.info("Sin datos demográficos de leads para el rango/plataforma seleccionados.")
            return

        age_cur = dcur[dcur["breakdown"].isin(["age", "age_gender"])].copy()
        age_prev = dprev[dprev["breakdown"].isin(["age", "age_gender"])].copy()
        gender_cur = dcur[dcur["breakdown"].isin(["gender", "age_gender"])].copy()
        gender_prev = dprev[dprev["breakdown"].isin(["gender", "age_gender"])].copy()
        if age_cur.empty and gender_cur.empty:
            st.info("Sin datos demográficos para el rango/plataforma seleccionados.")
            return

        age_totals = (
            age_cur.groupby("age_range", as_index=False)
            .agg(
                leads=("leads", "sum"),
                spend=("spend", "sum"),
                clicks=("clicks", "sum"),
                impressions=("impressions", "sum"),
            )
            if not age_cur.empty
            else pd.DataFrame(columns=["age_range", "leads", "spend", "clicks", "impressions"])
        )
        age_prev_totals = (
            age_prev.groupby("age_range", as_index=False).agg(leads_prev=("leads", "sum"))
            if not age_prev.empty
            else pd.DataFrame(columns=["age_range", "leads_prev"])
        )
        age_totals = age_totals.merge(age_prev_totals, on="age_range", how="left").fillna({"leads_prev": 0.0})
        total_leads = float(age_totals["leads"].sum()) if not age_totals.empty else 0.0
        total_prev_leads = float(age_totals["leads_prev"].sum()) if not age_totals.empty else 0.0

        gender_totals = (
            gender_cur.groupby("gender", as_index=False)
            .agg(leads=("leads", "sum"))
            .sort_values("leads", ascending=False, na_position="last")
            if not gender_cur.empty
            else pd.DataFrame(columns=["gender", "leads"])
        )
        gender_prev_totals = (
            gender_prev.groupby("gender", as_index=False).agg(leads_prev=("leads", "sum"))
            if not gender_prev.empty
            else pd.DataFrame(columns=["gender", "leads_prev"])
        )
        gender_totals = gender_totals.merge(gender_prev_totals, on="gender", how="left").fillna(
            {"leads_prev": 0.0}
        )
        if total_leads <= 0 and not gender_totals.empty:
            total_leads = float(gender_totals["leads"].sum())
            total_prev_leads = float(gender_totals["leads_prev"].sum())

        if total_leads <= 0:
            st.info("No se detectaron leads para construir el desglose de edad y género.")
            return

        if not age_totals.empty:
            age_totals["share"] = age_totals["leads"].apply(lambda v: sdiv(float(v), total_leads) or 0.0)
            age_totals["age_order"] = age_totals["age_range"].apply(
                lambda v: AGE_BUCKET_ORDER.index(v) if v in AGE_BUCKET_ORDER else len(AGE_BUCKET_ORDER)
            )
            age_totals = age_totals.sort_values(["age_order", "leads"], ascending=[True, False]).drop(
                columns=["age_order"]
            )
        top_age = str(age_totals.iloc[0]["age_range"]) if not age_totals.empty else "N/A"
        top_age_share = float(age_totals.iloc[0]["share"]) if not age_totals.empty else 0.0
        gender_totals["share"] = gender_totals["leads"].apply(lambda v: sdiv(float(v), total_leads) or 0.0)
        top_gender = str(gender_totals.iloc[0]["gender"]) if not gender_totals.empty else "N/A"
        top_gender_share = float(gender_totals.iloc[0]["share"]) if not gender_totals.empty else 0.0

        m1, m2, m3 = st.columns(3)
        m1.metric(
            "Leads (breakdown demográfico)",
            f"{total_leads:,.0f}",
            fmt_delta_compact(pct_delta(total_leads, total_prev_leads)),
        )
        m2.metric("Top Edad (share)", top_age, fmt_pct(top_age_share))
        m3.metric("Top Género (share)", top_gender, fmt_pct(top_gender_share))

        cross_roll = (
            dcur[dcur["breakdown"] == "age_gender"]
            .groupby(["age_range", "gender"], as_index=False)
            .agg(
                leads=("leads", "sum"),
                spend=("spend", "sum"),
                clicks=("clicks", "sum"),
                impressions=("impressions", "sum"),
            )
            if "age_gender" in set(dcur["breakdown"].astype(str))
            else pd.DataFrame(columns=["age_range", "gender", "leads", "spend", "clicks", "impressions"])
        )
        age_gender = (
            cross_roll.pivot_table(index="age_range", columns="gender", values="leads", aggfunc="sum", fill_value=0.0)
            if not cross_roll.empty
            else pd.DataFrame()
        )
        has_cross = not age_gender.empty

        viz_col_1, viz_col_2 = st.columns([1.9, 1.1], gap="large")
        with viz_col_1:
            if has_cross:
                ordered_age = [a for a in AGE_BUCKET_ORDER if a in age_gender.index]
                if not ordered_age:
                    ordered_age = sorted([str(v) for v in age_gender.index.tolist()])
                age_gender = age_gender.reindex(ordered_age, fill_value=0.0)

                bar = go.Figure()
                for gender, color in (("Female", C_META), ("Male", C_GOOGLE), ("Unknown", C_MUTE), ("All", C_ACCENT)):
                    if gender not in age_gender.columns:
                        continue
                    bar.add_trace(
                        go.Bar(
                            x=age_gender.index.tolist(),
                            y=age_gender[gender].tolist(),
                            name=gender,
                            marker={"color": color},
                            hovertemplate="%{x}<br>" + gender + ": %{y:,.0f} leads<extra></extra>",
                        )
                    )
                bar.update_layout(
                    barmode="stack",
                    height=290,
                    margin={"l": 8, "r": 8, "t": 40, "b": 10},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    legend={"orientation": "h", "x": 0.0, "y": 1.15},
                    xaxis={"title": "", "tickfont": {"size": 11, "color": C_MUTE}},
                    yaxis={"title": "Leads", "gridcolor": C_GRID, "tickfont": {"size": 11, "color": C_MUTE}},
                    title={"text": "Distribución de Leads por Edad", "font": {"size": 14, "color": C_TEXT}},
                )
                st.plotly_chart(bar, width="stretch")
            elif not age_totals.empty:
                bar = go.Figure(
                    go.Bar(
                        x=age_totals["age_range"],
                        y=age_totals["leads"],
                        marker={"color": C_GOOGLE},
                        hovertemplate="%{x}<br>Leads: %{y:,.0f}<extra></extra>",
                    )
                )
                bar.update_layout(
                    height=290,
                    margin={"l": 8, "r": 8, "t": 40, "b": 10},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis={"title": "", "tickfont": {"size": 11, "color": C_MUTE}},
                    yaxis={"title": "Leads", "gridcolor": C_GRID, "tickfont": {"size": 11, "color": C_MUTE}},
                    title={"text": "Distribución de Leads por Edad", "font": {"size": 14, "color": C_TEXT}},
                )
                st.plotly_chart(bar, width="stretch")
            else:
                st.info("Sin datos por edad para el rango seleccionado.")
        with viz_col_2:
            if gender_totals.empty:
                st.info("Sin datos por género para el rango seleccionado.")
            else:
                gender_color = {"Female": C_META, "Male": C_GOOGLE, "Unknown": C_MUTE, "All": C_ACCENT}
                pie = go.Figure(
                    go.Pie(
                        labels=gender_totals["gender"],
                        values=gender_totals["leads"],
                        hole=0.56,
                        marker={"colors": [gender_color.get(str(g), C_MUTE) for g in gender_totals["gender"]]},
                        texttemplate="%{label}<br>%{percent}",
                        hovertemplate="%{label}: %{value:,.0f} leads<extra></extra>",
                        sort=False,
                    )
                )
                pie.update_layout(
                    height=290,
                    margin={"l": 4, "r": 4, "t": 40, "b": 8},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                    title={"text": "Mix de Género", "font": {"size": 14, "color": C_TEXT}},
                )
                st.plotly_chart(pie, width="stretch")

        if not cross_roll.empty:
            demo_table = cross_roll.copy()
        elif not age_totals.empty:
            demo_table = age_totals[["age_range", "leads", "spend", "impressions", "clicks"]].copy()
            demo_table["gender"] = "All"
        else:
            demo_table = gender_totals[["gender", "leads"]].copy()
            demo_table["age_range"] = "All"
            demo_table["spend"] = 0.0
            demo_table["impressions"] = 0.0
            demo_table["clicks"] = 0.0
        demo_table["share_leads"] = demo_table["leads"].apply(lambda v: sdiv(float(v), total_leads) or 0.0)
        demo_table = demo_table.rename(
            columns={
                "age_range": "Edad",
                "gender": "Género",
                "leads": "Leads",
                "share_leads": "Share Leads",
                "spend": "Gasto",
                "impressions": "Impresiones",
                "clicks": "Clicks",
            }
        )[["Edad", "Género", "Leads", "Share Leads", "Gasto", "Impresiones", "Clicks"]]
        demo_table = demo_table.sort_values(["Leads", "Share Leads"], ascending=[False, False], na_position="last")
        st.dataframe(
            demo_table.style.format(
                {
                    "Leads": "{:.0f}",
                    "Share Leads": lambda v: fmt_pct(float(v)),
                    "Gasto": lambda v: fmt_money(float(v)),
                    "Impresiones": "{:.0f}",
                    "Clicks": "{:.0f}",
                }
            ),
            width="stretch",
            hide_index=True,
        )

    def _render_lead_geo_map() -> None:
        st.markdown(
            "<div class='viz-title' style='margin-bottom:0.35rem;'>6) Mapa de Distribución de Leads Paid</div>",
            unsafe_allow_html=True,
        )
        st.caption(
            "Métrica de análisis (breakdown de plataformas). No es comparable 1:1 con el KPI global de Conversiones."
        )
        if lead_geo_df.empty:
            st.info("No hay datos de geografía de leads para el tenant.")
            return

        gcur = lead_geo_df[(lead_geo_df["date"] >= s) & (lead_geo_df["date"] <= e)].copy()
        gprev = lead_geo_df[(lead_geo_df["date"] >= prev_s) & (lead_geo_df["date"] <= prev_e)].copy()
        if platform in ("Google", "Meta"):
            gcur = gcur[gcur["platform"] == platform]
            gprev = gprev[gprev["platform"] == platform]
        if gcur.empty:
            st.info("Sin datos geográficos de leads para el rango/plataforma seleccionados.")
            return

        def _first_text(values: pd.Series) -> str:
            for raw in values:
                txt = _clean_text_value(raw)
                if txt:
                    return txt
            return ""

        geo_roll = (
            gcur.groupby(["country_code", "region"], as_index=False)
            .agg(
                country_name=("country_name", _first_text),
                leads=("leads", "sum"),
                spend=("spend", "sum"),
                clicks=("clicks", "sum"),
                impressions=("impressions", "sum"),
            )
        )
        geo_roll["country_name"] = geo_roll.apply(
            lambda r: _clean_text_value(r.get("country_name")) or _country_name_from_code(r.get("country_code", "")),
            axis=1,
        )
        total_geo_leads = float(geo_roll["leads"].sum())
        if total_geo_leads <= 0:
            st.info("No se detectaron leads para construir el mapa geográfico.")
            return

        prev_geo_total = float(gprev["leads"].sum()) if not gprev.empty else 0.0
        country_roll = (
            geo_roll.groupby("country_code", as_index=False)
            .agg(
                country_name=("country_name", _first_text),
                leads=("leads", "sum"),
                spend=("spend", "sum"),
                clicks=("clicks", "sum"),
                impressions=("impressions", "sum"),
            )
        )
        country_roll["country_name"] = country_roll.apply(
            lambda r: _clean_text_value(r.get("country_name")) or _country_name_from_code(r.get("country_code", "")),
            axis=1,
        )
        country_roll["share_leads"] = country_roll["leads"].apply(lambda v: sdiv(float(v), total_geo_leads) or 0.0)
        top_country = country_roll.sort_values("leads", ascending=False, na_position="last").head(1)
        top_country_name = (
            _clean_text_value(top_country.iloc[0]["country_name"])
            or _clean_text_value(top_country.iloc[0]["country_code"])
            or "N/A"
        ) if not top_country.empty else "N/A"
        top_country_share = float(top_country.iloc[0]["share_leads"]) if not top_country.empty else 0.0
        country_count = int((country_roll["leads"] > 0).sum())
        prev_country_count = int((gprev.groupby("country_code", as_index=False)["leads"].sum()["leads"] > 0).sum()) if not gprev.empty else 0

        m1, m2, m3 = st.columns(3)
        m1.metric("Leads (geo breakdown)", f"{total_geo_leads:,.0f}", fmt_delta_compact(pct_delta(total_geo_leads, prev_geo_total)))
        m2.metric("País Top (share)", top_country_name, fmt_pct(top_country_share))
        m3.metric("Cobertura Países", f"{country_count}", fmt_delta_compact(pct_delta(float(country_count), float(prev_country_count))))

        map_df = country_roll.copy()
        map_df["country_code"] = map_df["country_code"].astype(str).str.strip().str.upper()
        map_df["country_name"] = map_df.apply(
            lambda r: _clean_text_value(r.get("country_name")) or _country_name_from_code(r.get("country_code", "")),
            axis=1,
        )
        map_df["country_label"] = map_df.apply(
            lambda r: _clean_text_value(r.get("country_name")) or _clean_text_value(r.get("country_code")),
            axis=1,
        )
        map_df["cpl"] = map_df.apply(
            lambda r: sdiv(float(r.get("spend", 0.0)), float(r.get("leads", 0.0))),
            axis=1,
        )
        map_df = map_df[
            map_df["country_code"].str.fullmatch(r"[A-Z]{2}", na=False)
            & (pd.to_numeric(map_df["leads"], errors="coerce").fillna(0.0) > 0)
        ].copy()
        map_col, table_col = st.columns([1.7, 1.3], gap="large")
        with map_col:
            if map_df.empty:
                st.info("No hay países mapeables para mostrar en el mapa.")
            else:
                if int(map_df["country_name"].nunique()) <= 2:
                    ch = px.choropleth(
                        map_df,
                        locations="country_name",
                        locationmode="country names",
                        color="share_leads",
                        hover_name="country_label",
                        hover_data={
                            "country_code": True,
                            "leads": ":.0f",
                            "spend": ":.2f",
                            "cpl": ":.2f",
                            "share_leads": ":.2%",
                        },
                        color_continuous_scale=_theme_color_scale(C_GOOGLE),
                    )
                    ch.update_layout(
                        height=330,
                        margin={"l": 0, "r": 0, "t": 6, "b": 0},
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        coloraxis_colorbar={"title": "Share Leads", "tickformat": ".0%"},
                    )
                    ch.update_geos(
                        fitbounds="locations",
                        visible=False,
                        showcountries=True,
                        countrycolor="rgba(255,255,255,0.9)",
                        bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(ch, width="stretch")
                else:
                    bubble_df = map_df.copy()
                    bubble_df["bubble_size"] = bubble_df["leads"].clip(lower=0.0)
                    ch = px.scatter_geo(
                        bubble_df,
                        locations="country_name",
                        locationmode="country names",
                        size="bubble_size",
                        color="share_leads",
                        hover_name="country_label",
                        hover_data={
                            "country_code": True,
                            "leads": ":.0f",
                            "spend": ":.2f",
                            "cpl": ":.2f",
                            "clicks": ":.0f",
                            "impressions": ":.0f",
                            "share_leads": ":.2%",
                        },
                        color_continuous_scale=_theme_color_scale(C_GOOGLE),
                        size_max=48,
                        projection="natural earth",
                    )
                    ch.update_traces(
                        marker={"line": {"width": 0.7, "color": "rgba(255,255,255,0.9)"}, "opacity": 0.82}
                    )
                    ch.update_layout(
                        height=330,
                        margin={"l": 0, "r": 0, "t": 6, "b": 0},
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        coloraxis_colorbar={"title": "Share Leads", "tickformat": ".0%"},
                    )
                    ch.update_geos(showframe=False, showcoastlines=False, bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(ch, width="stretch")
        with table_col:
            top_source = map_df if not map_df.empty else country_roll.copy()
            top_source = top_source[pd.to_numeric(top_source["leads"], errors="coerce").fillna(0.0) > 0].copy()
            top_country_view = (
                top_source.sort_values(["leads", "share_leads"], ascending=[False, False], na_position="last")
                .head(10)
                .copy()
            )
            if not top_country_view.empty:
                top_country_view["country_label"] = top_country_view.apply(
                    lambda r: _clean_text_value(r.get("country_name")) or _clean_text_value(r.get("country_code")),
                    axis=1,
                )
                top_country_view = top_country_view.sort_values("leads", ascending=True, na_position="last")
                top_bar = go.Figure(
                    go.Bar(
                        x=top_country_view["leads"],
                        y=top_country_view["country_label"],
                        orientation="h",
                        marker={"color": C_GOOGLE},
                        hovertemplate="%{y}<br>Leads: %{x:,.0f}<extra></extra>",
                    )
                )
                top_bar.update_layout(
                    height=210,
                    margin={"l": 6, "r": 6, "t": 18, "b": 6},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis={"title": "Leads", "gridcolor": C_GRID, "tickfont": {"size": 10, "color": C_MUTE}},
                    yaxis={"title": "", "tickfont": {"size": 10, "color": C_MUTE}},
                    title={"text": "Top Países por Leads", "font": {"size": 13, "color": C_TEXT}},
                )
                st.plotly_chart(top_bar, width="stretch")

            region_view = geo_roll.copy()
            region_view["country_label"] = region_view.apply(
                lambda r: _clean_text_value(r.get("country_name")) or _clean_text_value(r.get("country_code")),
                axis=1,
            )
            region_view["share_leads"] = region_view["leads"].apply(lambda v: sdiv(float(v), total_geo_leads) or 0.0)
            region_view = region_view.rename(
                columns={
                    "country_label": "País",
                    "region": "Región",
                    "leads": "Leads",
                    "share_leads": "Share Leads",
                    "spend": "Gasto",
                    "clicks": "Clicks",
                    "impressions": "Impresiones",
                }
            )[["País", "Región", "Leads", "Share Leads", "Gasto", "Clicks", "Impresiones"]]
            region_view = region_view.sort_values(["Leads", "Share Leads"], ascending=[False, False], na_position="last").head(15)
            st.dataframe(
                region_view.style.format(
                    {
                        "Leads": "{:.0f}",
                        "Share Leads": lambda v: fmt_pct(float(v)),
                        "Gasto": lambda v: fmt_money(float(v)),
                        "Clicks": "{:.0f}",
                        "Impresiones": "{:.0f}",
                    }
                ),
                width="stretch",
                hide_index=True,
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

    if "media_mix" in section_set:
        st.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
        _render_media_mix()

    if "lead_demographics" in section_set:
        st.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
        _render_lead_demographics()

    if "lead_geo_map" in section_set:
        st.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
        _render_lead_geo_map()

    if "device_breakdown" in section_set:
        st.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='viz-title' style='margin-bottom:0.35rem;'>7) Dispositivos de Pauta (Desktop / Mobile / Other)</div>",
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
            total_impressions = float(pd.to_numeric(roll["impressions"], errors="coerce").fillna(0.0).sum())
            if total_impressions <= 0:
                st.info("Sin impresiones para graficar distribución por dispositivo.")
            else:
                pie = go.Figure(
                    go.Pie(
                        labels=roll["device"],
                        values=roll["impressions"],
                        hole=0.56,
                        marker={
                            "colors": [C_GOOGLE, C_META, C_MUTE],
                            "line": {"color": "rgba(32,29,29,0.10)", "width": 1},
                        },
                        texttemplate="%{label}<br>%{percent}",
                        hovertemplate="%{label}: %{value:,.0f} impresiones<extra></extra>",
                        sort=False,
                    )
                )
                pie.update_layout(
                    height=300,
                    margin={"l": 10, "r": 10, "t": 8, "b": 8},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                )
                st.plotly_chart(pie, width="stretch")
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
            "<div class='viz-title' style='margin-bottom:0.35rem;'>8) Tabla Maestra de Auditoria</div>",
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
            piece_df,
            platform,
            s,
            e,
            tenant_meta_account_id=tenant_meta_account_id,
            tenant_google_customer_id=tenant_google_customer_id,
            campaign_filters=campaign_filters,
            report_cache_sig=report_cache_sig,
        )

def render_top_pieces_range(
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    platform: str,
    start_ref,
    end_ref,
    *,
    tenant_meta_account_id: str = "",
    tenant_google_customer_id: str = "",
    campaign_filters: dict[str, str] | None = None,
    report_cache_sig: tuple[str, int, int] | None = None,
):
    if report_cache_sig is not None:
        path_str, modified_ns, size_bytes = report_cache_sig
        filter_key = _campaign_filters_cache_key(campaign_filters or {})
        top = _cached_top_pieces_roll_from_report(
            path_str,
            modified_ns,
            size_bytes,
            str(start_ref.isoformat()),
            str(end_ref.isoformat()),
            str(platform or "All"),
            filter_key,
        )
        if top.empty:
            st.info("No hay piezas/campañas para el rango seleccionado.")
            return
        top = top.copy()
        if "campaign_id" not in top.columns:
            top["campaign_id"] = ""
        if "piece_id" not in top.columns:
            top["piece_id"] = ""
        if "piece_name" not in top.columns:
            top["piece_name"] = "Sin nombre"
        if "platform" not in top.columns:
            top["platform"] = ""
        if "inversion" not in top.columns:
            top["inversion"] = 0.0
        if "conversiones" not in top.columns:
            top["conversiones"] = 0.0
        if "cpl" not in top.columns:
            top["cpl"] = top.apply(
                lambda r: sdiv(float(r.get("inversion", 0.0)), float(r.get("conversiones", 0.0))),
                axis=1,
            )
        if "vista_previa" not in top.columns:
            top["vista_previa"] = ""
        top["Ver"] = top.apply(
            lambda r: piece_platform_link(
                r.get("platform"),
                piece_id=r.get("piece_id"),
                campaign_id=r.get("campaign_id"),
                meta_account_id=tenant_meta_account_id,
                google_customer_id=tenant_google_customer_id,
            ),
            axis=1,
        )
        top["Campaña / Pieza"] = top["piece_name"].astype(str).replace({"": "Sin nombre"})
        top["Vista"] = top["vista_previa"].astype(str).replace({"nan": "", "None": ""})
        top["Plataforma"] = top["platform"].astype(str).replace({"": "N/A"})
        top["Gasto"] = pd.to_numeric(top["inversion"], errors="coerce")
        top["Conversiones"] = pd.to_numeric(top["conversiones"], errors="coerce")
        top["CPL"] = pd.to_numeric(top["cpl"], errors="coerce")
        top_view = top[["Vista", "Ver", "Campaña / Pieza", "Plataforma", "Gasto", "Conversiones", "CPL"]].copy()
        top_view["Vista"] = top_view["Vista"].fillna("")
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
        if campaign_filters:
            active_labels = [
                f"{CAMPAIGN_FILTER_OPTIONS.get(k, k)}: {v}"
                for k, v in campaign_filters.items()
                if str(v).strip()
            ]
            if active_labels:
                st.caption(" | ".join(active_labels))
        st.dataframe(
            top_view,
            width="stretch",
            hide_index=True,
            column_config={
                "Vista": st.column_config.ImageColumn(
                    "Vista",
                    help="Vista previa de la pieza (si está disponible).",
                ),
                "Ver": st.column_config.LinkColumn("Ver", help="Abrir campaña/pieza", display_text="Abrir"),
                "Campaña / Pieza": st.column_config.TextColumn("Campaña / Pieza"),
                "Plataforma": st.column_config.TextColumn("Plataforma"),
                "Gasto": st.column_config.NumberColumn("Gasto", format="$%.2f"),
                "Conversiones": st.column_config.NumberColumn("Conversiones", format="%.0f"),
                "CPL": st.column_config.NumberColumn("CPL", format="$%.2f"),
            },
        )
        st.markdown("</div>", unsafe_allow_html=True)
        return

    has_piece_data = bool(
        isinstance(piece_df, pd.DataFrame)
        and not piece_df.empty
        and "date" in piece_df.columns
    )
    source_df = piece_df if has_piece_data else camp_df
    if source_df.empty:
        st.info("No hay datos de piezas/campañas para construir el top 10.")
        return

    cp = source_df.copy()
    if "date" not in cp.columns:
        st.info("El dataset de piezas no contiene columna de fecha.")
        return

    cp["date"] = pd.to_datetime(cp["date"], errors="coerce").dt.date
    cp = cp.dropna(subset=["date"])
    cp = cp[(cp["date"] >= start_ref) & (cp["date"] <= end_ref)]

    if platform in ("Google", "Meta") and "platform" in cp.columns:
        cp = cp[cp["platform"] == platform]
    cp = _apply_campaign_filters(cp, campaign_filters or {})

    def _clean_text(value: Any) -> str:
        txt = str(value or "").strip()
        return "" if txt.lower() in {"nan", "none", "null"} else txt

    def _first_non_empty(series: pd.Series) -> str:
        for raw in series.tolist():
            txt = _clean_text(raw)
            if txt:
                return txt
        return ""

    if has_piece_data:
        required_defaults: dict[str, Any] = {
            "platform": "",
            "campaign_id": "",
            "campaign_name": "",
            "piece_id": "",
            "piece_name": "",
            "ad_id": "",
            "ad_name": "",
            "image_url": "",
            "thumbnail_url": "",
            "preview_url": "",
            "spend": 0.0,
            "impressions": 0.0,
            "clicks": 0.0,
            "conversions": 0.0,
        }
        for col, default in required_defaults.items():
            if col not in cp.columns:
                cp[col] = default
        camp_fallback_all = pd.DataFrame()
        if isinstance(camp_df, pd.DataFrame) and not camp_df.empty and "date" in camp_df.columns:
            camp_fallback_all = camp_df.copy()
            camp_fallback_all["date"] = pd.to_datetime(camp_fallback_all["date"], errors="coerce").dt.date
            camp_fallback_all = camp_fallback_all.dropna(subset=["date"])
            camp_fallback_all = camp_fallback_all[
                (camp_fallback_all["date"] >= start_ref) & (camp_fallback_all["date"] <= end_ref)
            ]
            if platform in ("Google", "Meta") and "platform" in camp_fallback_all.columns:
                camp_fallback_all = camp_fallback_all[camp_fallback_all["platform"] == platform]
            camp_fallback_all = _apply_campaign_filters(camp_fallback_all, campaign_filters or {})
        piece_platforms = (
            set(cp["platform"].astype(str).str.strip())
            if "platform" in cp.columns
            else set()
        )
        fallback_platforms = (
            set(camp_fallback_all["platform"].astype(str).str.strip())
            if not camp_fallback_all.empty and "platform" in camp_fallback_all.columns
            else set()
        )
        if platform in ("Google", "Meta"):
            missing_platforms = {platform} - piece_platforms
        else:
            missing_platforms = fallback_platforms - piece_platforms
        if missing_platforms and not camp_fallback_all.empty:
            camp_fallback = camp_fallback_all[
                camp_fallback_all["platform"].astype(str).str.strip().isin(missing_platforms)
            ].copy()
            if not camp_fallback.empty:
                for col, default in required_defaults.items():
                    if col not in camp_fallback.columns:
                        camp_fallback[col] = default
                camp_fallback["piece_id"] = camp_fallback["campaign_id"].astype(str)
                camp_fallback["piece_name"] = camp_fallback["campaign_name"].astype(str)
                camp_fallback["ad_id"] = ""
                camp_fallback["ad_name"] = ""
                camp_fallback["image_url"] = ""
                camp_fallback["thumbnail_url"] = ""
                camp_fallback["preview_url"] = ""
                for col in cp.columns:
                    if col not in camp_fallback.columns:
                        camp_fallback[col] = ""
                cp = pd.concat([cp, camp_fallback[cp.columns]], ignore_index=True)
        cp = cp.reset_index(drop=True)
        cp["piece_id"] = cp.apply(
            lambda r: (
                _clean_text(r.get("piece_id"))
                or _clean_text(r.get("ad_id"))
                or f"piece::{_clean_text(r.get('campaign_id')) or 'na'}::{r.name}"
            ),
            axis=1,
        )
        cp["piece_name"] = cp.apply(
            lambda r: (
                _clean_text(r.get("piece_name"))
                or _clean_text(r.get("ad_name"))
                or _clean_text(r.get("campaign_name"))
                or "Sin nombre"
            ),
            axis=1,
        )
        cp["preview_image"] = cp.apply(
            lambda r: (
                _clean_text(r.get("preview_url"))
                or _clean_text(r.get("image_url"))
                or _clean_text(r.get("thumbnail_url"))
            ),
            axis=1,
        )
        for num_col in ("spend", "impressions", "clicks", "conversions"):
            cp[num_col] = pd.to_numeric(cp[num_col], errors="coerce").fillna(0.0)

        if cp.empty:
            st.info("No hay piezas/campañas para el rango seleccionado.")
            return

        top = (
            cp.groupby(
                ["platform", "piece_id", "piece_name", "campaign_id", "campaign_name"],
                as_index=False,
            )
            .agg(
                inversion=("spend", "sum"),
                conversiones=("conversions", "sum"),
                clics=("clicks", "sum"),
                vista_previa=("preview_image", _first_non_empty),
            )
        )
        top["Ver"] = top.apply(
            lambda r: piece_platform_link(
                r.get("platform"),
                piece_id=r.get("piece_id"),
                campaign_id=r.get("campaign_id"),
                meta_account_id=tenant_meta_account_id,
                google_customer_id=tenant_google_customer_id,
            ),
            axis=1,
        )
        top["Campaña / Pieza"] = top["piece_name"].astype(str).replace({"": "Sin nombre"})
        top["Vista"] = top["vista_previa"].astype(str).replace({"nan": "", "None": ""})
    else:
        required_defaults = {
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
        top["Vista"] = ""

    top["cpl"] = top.apply(lambda r: sdiv(float(r["inversion"]), float(r["conversiones"])), axis=1)
    top = top.sort_values(["conversiones", "clics"], ascending=[False, False], na_position="last").head(10)
    top["Plataforma"] = top["platform"].astype(str).replace({"": "N/A"})
    top["Gasto"] = pd.to_numeric(top["inversion"], errors="coerce")
    top["Conversiones"] = pd.to_numeric(top["conversiones"], errors="coerce")
    top["CPL"] = pd.to_numeric(top["cpl"], errors="coerce")
    top_view = top[["Vista", "Ver", "Campaña / Pieza", "Plataforma", "Gasto", "Conversiones", "CPL"]].copy()
    top_view["Vista"] = top_view["Vista"].fillna("")
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
    if campaign_filters:
        active_labels = [
            f"{CAMPAIGN_FILTER_OPTIONS.get(k, k)}: {v}"
            for k, v in campaign_filters.items()
            if str(v).strip()
        ]
        if active_labels:
            st.caption(" | ".join(active_labels))
    st.dataframe(
        top_view,
        width="stretch",
        hide_index=True,
        column_config={
            "Vista": st.column_config.ImageColumn(
                "Vista",
                help="Vista previa de la pieza (si está disponible).",
            ),
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
    campaign_filters: dict[str, str],
    traffic_kpi_keys: list[str],
    traffic_section_keys: list[str],
    report_cache_sig: tuple[str, int, int] | None = None,
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
    cache_path_str = ""
    cache_modified_ns = 0
    cache_size_bytes = 0
    if report_cache_sig is not None:
        cache_path_str, cache_modified_ns, cache_size_bytes = report_cache_sig
    start_iso = s.isoformat()
    end_iso = e.isoformat()
    campaign_filter_key = _campaign_filters_cache_key(campaign_filters)

    def _render_channels() -> None:
        section_title("Canales / Adquisicion")
        if report_cache_sig is not None:
            b = _cached_channels_roll_from_report(
                cache_path_str,
                cache_modified_ns,
                cache_size_bytes,
                start_iso,
                end_iso,
            ).head(10)
            if b.empty:
                st.info("Sin datos para el rango seleccionado.")
            else:
                fig = go.Figure()
                fig.add_trace(go.Bar(x=b["sessionDefaultChannelGroup"], y=b["sessions"], name="Sessions", marker={"color": C_ACCENT}))
                fig.add_trace(go.Scatter(x=b["sessionDefaultChannelGroup"], y=b["conversions"], name="Conversions", mode="lines+markers", line={"color": C_META, "width": 2}, yaxis="y2"))
                pbi_layout(fig, yaxis_title="Sessions", xaxis_title="Canal", y2_title="Conversions")
                st.plotly_chart(fig, width="stretch")
        elif ch_df.empty:
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
        if report_cache_sig is not None:
            top_p = _cached_top_pages_roll_from_report(
                cache_path_str,
                cache_modified_ns,
                cache_size_bytes,
                start_iso,
                end_iso,
            ).head(10)
            if top_p.empty:
                st.info("Sin datos para el rango seleccionado.")
            else:
                st.dataframe(top_p, width="stretch", hide_index=True)
        elif pg_df.empty:
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
        if report_cache_sig is not None:
            roll = _cached_campaign_roll_from_report(
                cache_path_str,
                cache_modified_ns,
                cache_size_bytes,
                start_iso,
                end_iso,
                platform,
                campaign_filter_key,
            ).head(20)
            if roll.empty:
                st.info("Sin datos de campanas para filtros actuales.")
            else:
                if campaign_filters:
                    active_labels = [
                        f"{CAMPAIGN_FILTER_OPTIONS.get(k, k)}: {v}"
                        for k, v in campaign_filters.items()
                        if str(v).strip()
                    ]
                    if active_labels:
                        st.caption(" | ".join(active_labels))
                st.dataframe(roll, width="stretch", hide_index=True)
        elif camp_df.empty:
            st.info("No hay datos de campanas en JSON.")
        else:
            cp = camp_df[(camp_df["date"] >= s) & (camp_df["date"] <= e)].copy()
            if platform in ("Google", "Meta"):
                cp = cp[cp["platform"] == platform]
            cp = _apply_campaign_filters(cp, campaign_filters)
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
                agg_map: dict[str, tuple[str, str]] = {
                    "spend": ("spend", "sum"),
                    "impressions": ("impressions", "sum"),
                    "clicks": ("clicks", "sum"),
                    "conversions": ("conversions", "sum"),
                    "ctr": ("ctr", "mean"),
                    "cpc": ("cpc", "mean"),
                    "reach": ("reach", "max"),
                    "frequency": ("frequency", "mean"),
                }
                roll = cp.groupby(["platform", "campaign_id", "campaign_name"], as_index=False).agg(**agg_map).sort_values("spend", ascending=False)
                roll["cpl"] = roll.apply(lambda r: sdiv(float(r["spend"]), float(r["conversions"])), axis=1)
                if campaign_filters:
                    active_labels = [
                        f"{CAMPAIGN_FILTER_OPTIONS.get(k, k)}: {v}"
                        for k, v in campaign_filters.items()
                        if str(v).strip()
                    ]
                    if active_labels:
                        st.caption(" | ".join(active_labels))
                st.dataframe(roll.head(20), width="stretch", hide_index=True)


def _render_admin_user_create_panel(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
    tenant_options: list[str],
    role_options: list[str],
    global_role_options: list[str],
) -> None:
    create_username_raw = st.text_input(
        "Username",
        value="",
        key="adm_create_username_wire",
        help="Solo minúsculas, números, punto, guión o guión bajo.",
    )
    create_name = st.text_input("Nombre", value="", key="adm_create_name_wire")
    col_create_1, col_create_2 = st.columns(2)
    with col_create_1:
        create_global_role = st.selectbox(
            "Rol global",
            options=global_role_options,
            index=0,
            key="adm_create_global_role_wire",
        )
    with col_create_2:
        create_role = st.selectbox(
            "Rol base",
            options=role_options,
            index=0,
            key="adm_create_role_wire",
        )
    create_enabled = st.toggle("Usuario activo", value=True, key="adm_create_enabled_wire")
    create_tenants = st.multiselect(
        "Tenants",
        options=tenant_options,
        default=["*"] if create_global_role == "admin" else [],
        format_func=lambda t: "Todos (*)" if t == "*" else str(tenants.get(t, {}).get("name", t)),
        key="adm_create_tenants_wire",
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
                key=f"adm_create_scope_role_wire_{safe_tenant}",
            )

    create_password = st.text_input(
        "Password",
        type="password",
        key="adm_create_password_wire",
    )
    create_password_confirm = st.text_input(
        "Confirmar password",
        type="password",
        key="adm_create_password_confirm_wire",
    )

    if st.button("Guardar Cambios", key="adm_create_submit_wire", type="primary", width="stretch"):
        errors: list[str] = []
        create_username = str(create_username_raw).strip().lower()
        valid_chars = set("abcdefghijklmnopqrstuvwxyz0123456789._-")
        if not create_username:
            errors.append("El username es obligatorio.")
        elif any(ch not in valid_chars for ch in create_username):
            errors.append("El username contiene caracteres no permitidos.")
        elif create_username in users:
            errors.append("Ese username ya existe.")

        clean_name = str(create_name).strip() or create_username
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
                st.session_state["adm_users_mode"] = "edit"
                st.session_state["adm_users_selected"] = create_username
                _set_admin_users_flash("success", f"Usuario '{create_username}' creado correctamente.")
                st.rerun()

    st.button(
        "Eliminar usuario",
        key="adm_create_delete_disabled_wire",
        width="stretch",
        disabled=True,
    )


def _set_admin_users_flash(kind: str, message: str) -> None:
    st.session_state["adm_users_flash"] = {
        "kind": str(kind).strip().lower() or "info",
        "message": str(message).strip(),
    }


@st.dialog("Confirmar eliminación", width="small", icon=":material/warning:")
def _render_delete_user_confirm_dialog(
    users: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
    selected_username: str,
) -> None:
    st.markdown(
        f"Vas a eliminar al usuario **{html.escape(selected_username)}**. Esta acción es permanente.",
        unsafe_allow_html=True,
    )
    col_cancel, col_confirm = st.columns(2)
    with col_cancel:
        if st.button("Cancelar", key=f"adm_delete_cancel_{selected_username}", width="stretch"):
            st.session_state.pop("adm_delete_target", None)
            st.rerun()
    with col_confirm:
        if st.button(
            "Eliminar",
            key=f"adm_delete_confirm_{selected_username}",
            type="primary",
            width="stretch",
        ):
            user = users.get(selected_username, {})
            errors: list[str] = []
            current_username = str(auth_user.get("username", "")).strip().lower()
            if selected_username.strip().lower() == current_username:
                errors.append("No puedes eliminar tu propio usuario en sesión.")
            if bool(user.get("enabled", True)) and _user_record_is_admin(user) and _enabled_admin_count(users) <= 1:
                errors.append("No puedes eliminar el último admin activo.")
            if errors:
                for err in errors:
                    st.error(err)
                return

            users_next = dict(users)
            users_next.pop(selected_username, None)
            ok, err_msg = save_users_config(USERS_CONFIG_PATH, users_next)
            if not ok:
                st.error(f"No se pudo guardar config/users.json: {err_msg}")
                return

            append_admin_audit(
                "user_deleted",
                str(auth_user.get("username", "unknown")),
                target=selected_username,
                details={},
            )
            st.session_state["adm_users_selected"] = sorted(users_next.keys())[0] if users_next else ""
            st.session_state["adm_users_panel_open"] = False
            st.session_state.pop("adm_delete_target", None)
            _set_admin_users_flash("success", f"Usuario '{selected_username}' eliminado correctamente.")
            st.rerun()


def _render_admin_user_edit_panel(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
    tenant_options: list[str],
    role_options: list[str],
    global_role_options: list[str],
    selected_username: str,
) -> None:
    user = users.get(selected_username, {})
    existing_role = str(user.get("role", "viewer")).strip().lower() or "viewer"
    existing_global_role = str(user.get("global_role", "user")).strip().lower() or "user"
    existing_scope_map = _scope_map_for_user(user)
    selected_defaults = _normalize_tenant_selection(list(existing_scope_map.keys()))
    if not selected_defaults:
        selected_defaults = _normalize_allowed_tenants(user.get("allowed_tenants", ["*"]))
    tenant_options_set = set(tenant_options)
    selected_defaults = [tenant_id for tenant_id in selected_defaults if tenant_id in tenant_options_set]
    if not selected_defaults and existing_global_role == "admin":
        selected_defaults = ["*"]

    st.text_input(
        "Username",
        value=selected_username,
        key=f"adm_edit_username_wire_{selected_username}",
        disabled=True,
    )
    edit_name = st.text_input(
        "Nombre",
        value=str(user.get("name", selected_username)),
        key=f"adm_edit_name_wire_{selected_username}",
    )
    role_col_1, role_col_2 = st.columns(2)
    with role_col_1:
        edit_global_role = st.selectbox(
            "Rol global",
            options=global_role_options,
            index=global_role_options.index(existing_global_role)
            if existing_global_role in global_role_options
            else 0,
            key=f"adm_edit_global_role_wire_{selected_username}",
        )
    with role_col_2:
        edit_role = st.selectbox(
            "Rol base",
            options=role_options,
            index=role_options.index(existing_role) if existing_role in role_options else 0,
            key=f"adm_edit_role_wire_{selected_username}",
        )
    edit_enabled = st.toggle(
        "Usuario activo",
        value=bool(user.get("enabled", True)),
        key=f"adm_edit_enabled_wire_{selected_username}",
    )

    selected_tenants = st.multiselect(
        "Tenants",
        options=tenant_options,
        default=selected_defaults,
        format_func=lambda t: "Todos (*)" if t == "*" else str(tenants.get(t, {}).get("name", t)),
        key=f"adm_edit_tenants_wire_{selected_username}",
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
                key=f"adm_edit_scope_role_wire_{selected_username}_{safe_tenant}",
            )

    edit_new_password = st.text_input(
        "Password nueva (opcional)",
        type="password",
        key=f"adm_edit_password_wire_{selected_username}",
    )
    edit_confirm_password = st.text_input(
        "Confirmar password",
        type="password",
        key=f"adm_edit_password_confirm_wire_{selected_username}",
    )

    if st.button(
        "Guardar Cambios",
        key=f"adm_edit_save_wire_{selected_username}",
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
                _set_admin_users_flash("success", f"Usuario '{selected_username}' actualizado correctamente.")
                st.rerun()

    st.caption("Esta acción elimina el usuario de forma permanente.")
    if st.button(
        "Eliminar usuario",
        key=f"adm_edit_delete_wire_{selected_username}",
        width="stretch",
    ):
        st.session_state["adm_delete_target"] = selected_username

    pending_delete = str(st.session_state.get("adm_delete_target", "")).strip().lower()
    if pending_delete and pending_delete == selected_username.strip().lower():
        _render_delete_user_confirm_dialog(users, auth_user, selected_username)


def _render_admin_users_wireframe(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
) -> None:
    st.markdown(
        """
        <style>
          .adm-kpi-card {
            border: 1px solid rgba(32,29,29,0.12);
            background: rgba(255,255,255,0.76);
            border-radius: 10px;
            padding: 0.6rem 0.85rem 0.5rem 0.85rem;
            min-height: 4.2rem;
            box-shadow: 0 8px 20px rgba(15,23,42,0.04);
          }
          .adm-kpi-label {
            color: #5A6170;
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.06em;
          }
          .adm-kpi-value {
            color: #201D1D;
            font-size: 1.85rem;
            line-height: 1.05;
            font-weight: 800;
            margin-top: 0.22rem;
          }
          .adm-card-title {
            color: #2D333E;
            font-size: 0.92rem;
            font-weight: 800;
            letter-spacing: 0.01em;
            text-transform: uppercase;
            margin-top: 0.12rem;
          }
          .adm-table-head {
            color: #5A6170;
            font-size: 0.72rem;
            text-transform: uppercase;
            font-weight: 800;
            letter-spacing: 0.05em;
          }
          .adm-table-cell {
            color: #2D333E;
            font-size: 0.9rem;
            font-weight: 600;
            line-height: 1.2;
          }
          .adm-table-cell-muted {
            color: #7A879D;
            font-size: 0.84rem;
            font-weight: 600;
            line-height: 1.2;
          }
          .adm-status-pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            padding: 0.14rem 0.48rem;
            border: 1px solid rgba(103,178,45,0.38);
            color: #4E8B22;
            background: rgba(123,204,53,0.16);
            font-size: 0.7rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
          }
          .adm-status-pill.off {
            border-color: rgba(122,135,157,0.36);
            color: #6C778A;
            background: rgba(122,135,157,0.12);
          }
          .adm-user-detail-title {
            color: #201D1D;
            font-size: 1.18rem;
            line-height: 1.1;
            font-weight: 800;
            margin-top: 0.2rem;
          }
          .adm-users-scrim {
            position: fixed;
            inset: 0;
            background: rgba(245,245,247,0.74);
            z-index: 1350;
            animation: admScrimIn 35ms linear;
          }
          .st-key-adm-user-drawer {
            position: fixed;
            right: 1.05rem;
            bottom: 0.9rem;
            width: min(420px, calc(100vw - 1.2rem));
            z-index: 1400;
            background: #F8F9FB;
            border-radius: 14px;
            overflow: hidden;
            opacity: 1 !important;
            transform-origin: right bottom;
            animation: admDrawerIn 45ms linear;
          }
          .st-key-adm-user-drawer [data-testid="stVerticalBlock"] {
            background: #F8F9FB !important;
          }
          .st-key-adm-user-drawer [data-testid="stVerticalBlockBorderWrapper"] {
            background: #FFFFFF !important;
            border: 1px solid rgba(32,29,29,0.14);
            box-shadow: 0 24px 52px rgba(15,23,42,0.18);
            opacity: 1 !important;
          }
          .st-key-adm-user-drawer [data-testid="stVerticalBlockBorderWrapper"] > div {
            max-height: min(78vh, 820px);
            overflow-y: auto;
            overflow-x: hidden;
            background: #FFFFFF !important;
          }
          .st-key-adm-user-drawer [data-testid="stMultiSelect"] {
            max-width: 100%;
          }
          .st-key-adm-user-drawer [data-baseweb="tag"] {
            max-width: 100%;
          }
          .st-key-adm-user-drawer [data-testid="stTextInput"] > div > div,
          .st-key-adm-user-drawer [data-testid="stSelectbox"] > div > div,
          .st-key-adm-user-drawer [data-testid="stMultiSelect"] > div > div {
            background: #FFFFFF !important;
            border-color: rgba(32,29,29,0.14) !important;
          }
          .st-key-adm-users-toolbar {
            margin-top: 0.95rem;
            margin-bottom: 0.35rem;
          }
          .st-key-adm-new-user-btn [data-testid="stButton"] button {
            border-radius: 999px !important;
            border: 1px solid rgba(103,178,45,0.66) !important;
            background: linear-gradient(180deg, rgba(123,204,53,0.30) 0%, rgba(123,204,53,0.24) 100%) !important;
            color: #2D333E !important;
            font-weight: 800 !important;
            box-shadow: 0 8px 16px rgba(103,178,45,0.20) !important;
          }
          .st-key-adm-new-user-btn [data-testid="stButton"] button:hover {
            background: rgba(123,204,53,0.22) !important;
          }
          @keyframes admScrimIn {
            from { opacity: 0; }
            to { opacity: 1; }
          }
          @keyframes admDrawerIn {
            from {
              opacity: 0;
              transform: translateY(4px) scale(0.998);
            }
            to {
              opacity: 1;
              transform: translateY(0) scale(1);
            }
          }
          @media (max-width: 960px) {
            .st-key-adm-user-drawer {
              left: 0.7rem;
              right: 0.7rem;
              width: auto;
              bottom: 0.65rem;
              transform-origin: center bottom;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    total_users = len(users)
    active_users = sum(1 for u in users.values() if bool(u.get("enabled", True)))
    enabled_admins = _enabled_admin_count(users)
    tenant_options = ["*"] + sorted(tenants.keys())
    role_options = ["viewer", "editor", "admin"]
    global_role_options = ["user", "admin"]
    sorted_usernames = sorted(users.keys())

    users_view_context = (
        f"{str(st.session_state.get('sidebar_view_mode', ''))}|"
        f"{str(st.session_state.get('admin_panel_section', ''))}|"
        f"{str(st.session_state.get('active_tenant_id', ''))}"
    )
    if st.session_state.get("adm_users_view_context") != users_view_context:
        st.session_state["adm_users_view_context"] = users_view_context
        st.session_state["adm_users_panel_open"] = False
        st.session_state["adm_users_mode"] = "edit"

    if "adm_users_mode" not in st.session_state:
        st.session_state["adm_users_mode"] = "edit"
    if "adm_users_panel_open" not in st.session_state:
        st.session_state["adm_users_panel_open"] = False
    if "adm_users_selected" not in st.session_state:
        st.session_state["adm_users_selected"] = sorted_usernames[0] if sorted_usernames else ""
    if st.session_state.get("adm_users_selected") not in users:
        st.session_state["adm_users_selected"] = sorted_usernames[0] if sorted_usernames else ""
    if not sorted_usernames:
        st.session_state["adm_users_mode"] = "create"

    flash_payload = st.session_state.pop("adm_users_flash", None)
    if isinstance(flash_payload, dict):
        flash_kind = str(flash_payload.get("kind", "info")).strip().lower() or "info"
        flash_message = str(flash_payload.get("message", "")).strip()
        if flash_message:
            toast_icon = "\u2705" if flash_kind == "success" else "\u26A0\uFE0F"
            st.toast(flash_message, icon=toast_icon)

    kpi_col_1, kpi_col_2, kpi_col_3 = st.columns(3)
    with kpi_col_1:
        st.markdown(
            f"<div class='adm-kpi-card'><div class='adm-kpi-label'>Usuarios</div><div class='adm-kpi-value'>{total_users}</div></div>",
            unsafe_allow_html=True,
        )
    with kpi_col_2:
        st.markdown(
            f"<div class='adm-kpi-card'><div class='adm-kpi-label'>Activos</div><div class='adm-kpi-value'>{active_users}</div></div>",
            unsafe_allow_html=True,
        )
    with kpi_col_3:
        st.markdown(
            f"<div class='adm-kpi-card'><div class='adm-kpi-label'>Admins activos</div><div class='adm-kpi-value'>{enabled_admins}</div></div>",
            unsafe_allow_html=True,
        )

    with st.container(key="adm-users-toolbar"):
        table_top_left, table_top_right = st.columns([1.0, 0.24], gap="small")
        with table_top_left:
            st.markdown("<div class='adm-card-title'>Usuarios</div>", unsafe_allow_html=True)
        with table_top_right:
            with st.container(key="adm-new-user-btn"):
                if st.button(
                    "Nuevo usuario",
                    key="adm_users_new_btn",
                    icon=":material/person_add:",
                    width="stretch",
                ):
                    st.session_state["adm_users_mode"] = "create"
                    st.session_state["adm_users_selected"] = ""
                    st.session_state["adm_users_panel_open"] = True

    table_box = st.container(border=True)
    with table_box:
        h1, h2, h3, h4, h5, h6, h7 = st.columns([1.05, 1.08, 0.86, 0.92, 0.72, 1.68, 0.32], gap="small")
        with h1:
            st.markdown("<div class='adm-table-head'>Usuario</div>", unsafe_allow_html=True)
        with h2:
            st.markdown("<div class='adm-table-head'>Nombre</div>", unsafe_allow_html=True)
        with h3:
            st.markdown("<div class='adm-table-head'>Global Role</div>", unsafe_allow_html=True)
        with h4:
            st.markdown("<div class='adm-table-head'>Role (legacy)</div>", unsafe_allow_html=True)
        with h5:
            st.markdown("<div class='adm-table-head'>Activo</div>", unsafe_allow_html=True)
        with h6:
            st.markdown("<div class='adm-table-head'>Scopes por tenant</div>", unsafe_allow_html=True)
        with h7:
            st.markdown("<div class='adm-table-head'>&nbsp;</div>", unsafe_allow_html=True)
        st.divider()

        if sorted_usernames:
            for username in sorted_usernames:
                user = users.get(username, {})
                role = str(user.get("role", "viewer")).strip().lower() or "viewer"
                global_role = str(user.get("global_role", "user")).strip().lower() or "user"
                enabled = bool(user.get("enabled", True))
                scope_map = _scope_map_for_user(user)
                labels: list[str] = []
                for tenant_id in sorted(scope_map.keys()):
                    tenant_role = scope_map.get(tenant_id, role)
                    if tenant_id == "*":
                        labels.append(f"Todos (*):{tenant_role}")
                    else:
                        tenant_name = str(tenants.get(tenant_id, {}).get("name", tenant_id))
                        labels.append(f"{tenant_name}:{tenant_role}")
                scope_text = " | ".join(labels) if labels else "Sin scope"
                if len(scope_text) > 48:
                    scope_text = f"{scope_text[:45]}..."

                c1, c2, c3, c4, c5, c6, c7 = st.columns([1.05, 1.08, 0.86, 0.92, 0.72, 1.68, 0.32], gap="small")
                with c1:
                    st.markdown(
                        f"<div class='adm-table-cell'>{html.escape(str(user.get('username', username)))}</div>",
                        unsafe_allow_html=True,
                    )
                with c2:
                    name_value = str(user.get("name", "")).strip() or "—"
                    st.markdown(
                        f"<div class='adm-table-cell-muted'>{html.escape(name_value)}</div>",
                        unsafe_allow_html=True,
                    )
                with c3:
                    st.markdown(f"<div class='adm-table-cell'>{html.escape(global_role)}</div>", unsafe_allow_html=True)
                with c4:
                    st.markdown(f"<div class='adm-table-cell'>{html.escape(role)}</div>", unsafe_allow_html=True)
                with c5:
                    status_class = "adm-status-pill" if enabled else "adm-status-pill off"
                    status_text = "Activo" if enabled else "Inactivo"
                    st.markdown(f"<span class='{status_class}'>{status_text}</span>", unsafe_allow_html=True)
                with c6:
                    st.markdown(
                        f"<div class='adm-table-cell-muted'>{html.escape(scope_text)}</div>",
                        unsafe_allow_html=True,
                    )
                with c7:
                    if st.button("⋯", key=f"adm_users_pick_{username}", width="stretch"):
                        st.session_state["adm_users_mode"] = "edit"
                        st.session_state["adm_users_selected"] = username
                        st.session_state["adm_users_panel_open"] = True
        else:
            st.info("No hay usuarios cargados en config/users.json.")

    if bool(st.session_state.get("adm_users_panel_open", False)):
        st.markdown("<div class='adm-users-scrim'></div>", unsafe_allow_html=True)
        with st.container(key="adm-user-drawer"):
            detail_box = st.container(border=True)
            with detail_box:
                mode = str(st.session_state.get("adm_users_mode", "edit")).strip().lower() or "edit"
                is_create_mode = mode == "create"
                selected_username = str(st.session_state.get("adm_users_selected", "")).strip()
                if not selected_username and sorted_usernames:
                    selected_username = sorted_usernames[0]
                    st.session_state["adm_users_selected"] = selected_username

                head_left, head_right = st.columns([0.7, 0.3], gap="small")
                with head_left:
                    detail_title = "Nuevo Usuario" if is_create_mode else "Detalles del Usuario"
                    st.markdown(f"<div class='adm-user-detail-title'>{detail_title}</div>", unsafe_allow_html=True)
                with head_right:
                    if st.button("Close ×", key="adm_user_detail_close", width="stretch"):
                        st.session_state["adm_users_panel_open"] = False
                        st.rerun()

                if is_create_mode:
                    _render_admin_user_create_panel(
                        users,
                        tenants,
                        auth_user,
                        tenant_options,
                        role_options,
                        global_role_options,
                    )
                else:
                    if not sorted_usernames:
                        st.info("No hay usuarios para editar.")
                    elif selected_username not in users:
                        st.info("Selecciona un usuario de la tabla.")
                    else:
                        _render_admin_user_edit_panel(
                            users,
                            tenants,
                            auth_user,
                            tenant_options,
                            role_options,
                            global_role_options,
                            selected_username,
                        )

# Context builder moved to coco_agent.context_builder.

def _normalize_question_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    ascii_txt = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_txt).strip()


def _safe_make_date(year_value: int, month_value: int, day_value: int) -> date | None:
    try:
        return date(int(year_value), int(month_value), int(day_value))
    except Exception:
        return None


def _month_bounds(year_value: int, month_value: int) -> tuple[date, date] | None:
    try:
        y = int(year_value)
        m = int(month_value)
    except Exception:
        return None
    if m < 1 or m > 12:
        return None
    last_day = calendar.monthrange(y, m)[1]
    start_day = _safe_make_date(y, m, 1)
    end_day = _safe_make_date(y, m, last_day)
    if start_day is None or end_day is None:
        return None
    return start_day, end_day


def _extract_first_date_token(text: str) -> date | None:
    txt = _normalize_question_text(text)
    if not txt:
        return None
    iso_match = re.search(r"\b(20\d{2})[/-](\d{1,2})[/-](\d{1,2})\b", txt)
    if iso_match:
        return _safe_make_date(
            int(iso_match.group(1)),
            int(iso_match.group(2)),
            int(iso_match.group(3)),
        )
    long_match = re.search(
        r"\b(\d{1,2})\s+de\s+([a-z]+)\s+(?:de|del)\s+(20\d{2})\b",
        txt,
    )
    if long_match:
        month_idx = SPANISH_MONTHS.get(str(long_match.group(2)).strip().lower())
        if month_idx:
            return _safe_make_date(
                int(long_match.group(3)),
                int(month_idx),
                int(long_match.group(1)),
            )
    short_match = re.search(r"\b([a-z]+)\s+(?:de|del)\s+(20\d{2})\b", txt)
    if short_match:
        month_idx = SPANISH_MONTHS.get(str(short_match.group(1)).strip().lower())
        if month_idx:
            return _safe_make_date(int(short_match.group(2)), int(month_idx), 1)
    return None


def _is_one_edit_or_adjacent_swap(source: str, target: str) -> bool:
    s = str(source or "").strip().lower()
    t = str(target or "").strip().lower()
    if not s or not t:
        return False
    if s == t:
        return True

    ls, lt = len(s), len(t)
    if abs(ls - lt) > 1:
        return False

    if ls == lt:
        mismatches = [idx for idx, (a, b) in enumerate(zip(s, t)) if a != b]
        if len(mismatches) == 1:
            return True
        if len(mismatches) == 2:
            i, j = mismatches
            if j == i + 1 and s[i] == t[j] and s[j] == t[i]:
                return True
        return False

    shorter, longer = (s, t) if ls < lt else (t, s)
    i = 0
    j = 0
    edits = 0
    while i < len(shorter) and j < len(longer):
        if shorter[i] == longer[j]:
            i += 1
            j += 1
            continue
        edits += 1
        if edits > 1:
            return False
        j += 1
    return True


def _question_has_last_year_intent(normalized_question: str) -> bool:
    q = str(normalized_question or "").strip().lower()
    if not q:
        return False
    year_last_tokens = ("ano", "a?o", "anio")
    if not any(token in q for token in year_last_tokens):
        return False
    words = re.findall(r"[a-z0-9?]+", q)
    return any(_is_one_edit_or_adjacent_swap(word, "pasado") for word in words)


# Deterministic resolver/scope helpers moved to coco_agent.deterministic_resolvers.

def _format_coco_structured_answer(
    *,
    headline: str,
    findings: list[str],
    actions: list[str] | None = None,
    note: str = "",
) -> str:
    finding_lines = "\n".join(f"- {line}" for line in findings if str(line).strip())
    action_lines = "\n".join(f"- {line}" for line in (actions or []) if str(line).strip())
    output = (
        f"**Resumen**\n{headline.strip()}\n\n"
        f"**Hallazgos**\n{finding_lines or '- Sin hallazgos relevantes.'}"
    )
    if action_lines:
        output += f"\n\n**Acción sugerida**\n{action_lines}"
    if str(note).strip():
        output += f"\n\n**Nota**\n{note.strip()}"
    return output


def _local_coco_answer(
    question: str,
    context: dict[str, Any],
    include_actions: bool = True,
) -> str:
    q = str(question).strip().lower()
    cur = context.get("current_period", {}) if isinstance(context.get("current_period"), dict) else {}
    prev = context.get("previous_period", {}) if isinstance(context.get("previous_period"), dict) else {}
    delta = context.get("delta_vs_previous_pct", {}) if isinstance(context.get("delta_vs_previous_pct"), dict) else {}
    coverage = context.get("data_coverage", {}) if isinstance(context.get("data_coverage"), dict) else {}
    if coverage:
        requested_explicit = _coerce_bool(coverage.get("requested_range_explicit"), False)
        has_data = _coerce_bool(coverage.get("has_data_in_requested_range"), True)
        if requested_explicit and not has_data:
            req_start = str(coverage.get("requested_start", "")).strip() or "N/A"
            req_end = str(coverage.get("requested_end", "")).strip() or "N/A"
            data_start = str(coverage.get("tenant_data_start", "")).strip() or "N/A"
            data_end = str(coverage.get("tenant_data_end", "")).strip() or "N/A"
            return _format_coco_structured_answer(
                headline="No encontré datos para el rango explícito solicitado.",
                findings=[
                    f"Rango solicitado: {req_start} a {req_end}.",
                    f"Cobertura disponible del tenant: {data_start} a {data_end}.",
                ],
                actions=(
                    [
                        "Prueba con un rango dentro de la cobertura disponible o actualiza la extracción histórica.",
                    ]
                    if include_actions
                    else []
                ),
            )

    lookup_rules = [
        (("conversion", "conv"), "conv"),
        (("cpl",), "cpl"),
        (("ctr",), "ctr"),
        (("cvr",), "cvr"),
        (("impres", "impr"), "impr"),
        (("click", "clic"), "clicks"),
        (("cpc",), "cpc"),
        (("cpm",), "cpm"),
        (("sesion", "session"), "sessions"),
        (("usuario", "users"), "users"),
        (("gasto", "inversion", "spend", "costo"), "spend"),
    ]
    selected_key = ""
    for terms, candidate in lookup_rules:
        if any(term in q for term in terms):
            selected_key = candidate
            break

    if selected_key:
        label = str(KPI_CATALOG.get(selected_key, {}).get("label", selected_key)).strip()
        fmt_key = str(KPI_CATALOG.get(selected_key, {}).get("fmt", "int")).strip()
        cur_v = cur.get(selected_key)
        prev_v = prev.get(selected_key)
        d_v = delta.get(selected_key)
        cur_txt = _format_kpi_value(fmt_key, sf(cur_v) if cur_v is not None else None)
        prev_txt = _format_kpi_value(fmt_key, sf(prev_v) if prev_v is not None else None)
        delta_txt = fmt_delta_compact(d_v)
        trend = "mejoró" if (d_v or 0.0) > 0 else "cayó" if (d_v or 0.0) < 0 else "se mantuvo estable"
        return _format_coco_structured_answer(
            headline=f"{label}: {cur_txt} en el periodo activo.",
            findings=[
                f"Comparado al periodo anterior ({prev_txt}), el cambio fue {delta_txt}.",
                f"El comportamiento del indicador {trend} frente al periodo previo.",
            ],
            actions=(
                [
                    "Mantener seguimiento diario para validar si la tendencia se sostiene.",
                    "Cruzar este KPI con canal/plataforma para identificar causa principal.",
                ]
                if include_actions
                else []
            ),
        )

    spend = _format_kpi_value("money", sf(cur.get("spend")) if cur.get("spend") is not None else None)
    conv = _format_kpi_value("int", sf(cur.get("conv")) if cur.get("conv") is not None else None)
    cpl = _format_kpi_value("money", sf(cur.get("cpl")) if cur.get("cpl") is not None else None)
    ctr = _format_kpi_value("pct", sf(cur.get("ctr")) if cur.get("ctr") is not None else None)
    spend_delta = fmt_delta_compact(delta.get("spend"))
    conv_delta = fmt_delta_compact(delta.get("conv"))
    cpl_delta = fmt_delta_compact(delta.get("cpl"))
    ctr_delta = fmt_delta_compact(delta.get("ctr"))
    return _format_coco_structured_answer(
        headline=f"Resumen general del tenant y rango activo: gasto {spend}, conversiones {conv}.",
        findings=[
            f"Gasto: {spend} ({spend_delta} vs periodo anterior).",
            f"Conversiones: {conv} ({conv_delta} vs periodo anterior).",
            f"CPL: {cpl} ({cpl_delta}) y CTR: {ctr} ({ctr_delta}).",
        ],
        actions=(
            [
                "Priorizar campañas con mejor relación conversiones/costo.",
                "Revisar creativos y segmentación donde el CTR esté cayendo.",
            ]
            if include_actions
            else []
        ),
    )


def _call_openai_coco(
    *,
    model: str,
    question: str,
    context: dict[str, Any],
    conversation_history: list[dict[str, str]] | None = None,
    memory_summary: str = "",
    include_actions: bool = True,
) -> tuple[str, int, int, str]:
    api_key = str(os.environ.get("OPENAI_API_KEY", "")).strip()
    if not api_key:
        return "", 0, 0, "OPENAI_API_KEY no está configurada."
    context_json = json.dumps(context, ensure_ascii=False)
    answer, prompt_tokens_raw, completion_tokens_raw, err = run_coco_agent_turn(
        api_key=api_key,
        model=model,
        question=question,
        context=context,
        conversation_history=conversation_history,
        memory_summary=memory_summary,
        include_actions=include_actions,
        max_tool_rounds=6,
        temperature=0.2,
        timeout_seconds=45,
    )
    prompt_tokens = _normalize_non_negative_int(
        prompt_tokens_raw,
        _estimate_token_count(question + context_json),
        minimum=0,
        maximum=5_000_000,
    )
    completion_tokens = _normalize_non_negative_int(
        completion_tokens_raw,
        _estimate_token_count(answer),
        minimum=0,
        maximum=5_000_000,
    )
    if answer:
        return answer, prompt_tokens, completion_tokens, ""
    return "", prompt_tokens, completion_tokens, (err or "OpenAI no devolvió contenido en la respuesta.")


def _generate_coco_answer(
    *,
    question: str,
    context: dict[str, Any],
    coco_cfg: dict[str, Any],
    conversation_history: list[dict[str, str]] | None = None,
    memory_summary: str = "",
    include_actions: bool = True,
) -> tuple[str, int, int, str, str, str]:
    provider = str(coco_cfg.get("provider", COCO_DEFAULT_PROVIDER)).strip().lower() or COCO_DEFAULT_PROVIDER
    model = str(coco_cfg.get("model", COCO_DEFAULT_MODEL)).strip() or COCO_DEFAULT_MODEL
    if provider == "openai":
        answer, input_tokens, output_tokens, err = _call_openai_coco(
            model=model,
            question=question,
            context=context,
            conversation_history=conversation_history,
            memory_summary=memory_summary,
            include_actions=include_actions,
        )
        if answer:
            return answer, input_tokens, output_tokens, provider, model, ""
        fallback = _local_coco_answer(question, context, include_actions=include_actions)
        in_est = _estimate_token_count(question + json.dumps(context, ensure_ascii=False))
        out_est = _estimate_token_count(fallback)
        tagged_fallback = f"{fallback}\n\nNota: respuesta local temporal ({err})"
        return tagged_fallback, in_est, out_est, "local", "fallback-local", err

    fallback = _local_coco_answer(question, context, include_actions=include_actions)
    in_est = _estimate_token_count(question + json.dumps(context, ensure_ascii=False))
    out_est = _estimate_token_count(fallback)
    return fallback, in_est, out_est, "local", "fallback-local", ""


def _openai_key_status() -> tuple[bool, str]:
    api_key = str(os.environ.get("OPENAI_API_KEY", "")).strip()
    if not api_key:
        return False, "No configurada"
    if len(api_key) < 20:
        return False, "Formato inválido"
    masked = f"{api_key[:7]}...{api_key[-4:]}"
    return True, masked


def _check_openai_connectivity(timeout: int = 10) -> tuple[bool, str]:
    api_key = str(os.environ.get("OPENAI_API_KEY", "")).strip()
    if not api_key:
        return False, "OPENAI_API_KEY no configurada."
    req = urllib.request.Request(
        url="https://api.openai.com/v1/models",
        headers={"Authorization": f"Bearer {api_key}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if int(getattr(resp, "status", 0)) in {200, 201}:
                return True, "Conexión OK con OpenAI."
            return False, f"OpenAI respondió estado {getattr(resp, 'status', 'desconocido')}."
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, f"HTTP {exc.code}: {body[:220]}"
    except Exception as exc:
        return False, f"Error conexión OpenAI: {exc}"


def render_coco_ia_widget(
    *,
    df_base: pd.DataFrame,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    df_sel: pd.DataFrame,
    df_prev: pd.DataFrame,
    platform: str,
    s: date,
    e: date,
    tenant_id: str,
    tenant_name: str,
    auth_user: dict[str, Any],
    coco_cfg: dict[str, Any],
) -> None:
    if not _is_coco_enabled_for_tenant(coco_cfg, tenant_id):
        return

    username = str(auth_user.get("username", "unknown")).strip().lower() or "unknown"
    thread_key = _coco_thread_key(username, tenant_id)
    context_key = f"coco_chat_context::{tenant_id}::{username}"
    history_key = f"coco_chat_history::{tenant_id}::{username}"
    panel_state_key = f"coco_panel_open::{tenant_id}::{username}"
    if panel_state_key not in st.session_state:
        st.session_state[panel_state_key] = False
    panel_open = bool(st.session_state.get(panel_state_key, False))

    st.markdown(
        """
        <style>
          [class*="st-key-coco-fab-"],
          [class*="st-key-coco_toggle_panel_"] {
            position: fixed;
            right: 1.15rem;
            bottom: 1.15rem;
            z-index: 1100;
            margin: 0 !important;
            padding: 0 !important;
          }
          [class*="st-key-coco-fab-"] [data-testid="stButton"],
          [class*="st-key-coco_toggle_panel_"] [data-testid="stButton"] {
            margin: 0 !important;
          }
          [class*="st-key-coco-fab-"] [data-testid="stButton"] button,
          [class*="st-key-coco_toggle_panel_"] [data-testid="stButton"] button {
            width: 56px;
            min-width: 56px;
            height: 56px;
            border-radius: 999px;
            border: 1px solid rgba(255, 255, 255, 0.35);
            box-shadow: 0 14px 30px rgba(17, 24, 39, 0.26);
            background: linear-gradient(145deg, #396f14 0%, #5ea920 52%, #7dcf38 100%);
            color: #ffffff !important;
            padding: 0 !important;
          }
          [class*="st-key-coco-fab-"] [data-testid="stButton"] button p,
          [class*="st-key-coco_toggle_panel_"] [data-testid="stButton"] button p {
            margin: 0;
            line-height: 1;
            font-size: 1.28rem;
            font-weight: 700;
          }
          [class*="st-key-coco-panel-"],
          [class*="st-key-coco_panel_"] {
            position: fixed;
            right: 1rem;
            bottom: 5.2rem;
            top: auto;
            width: min(430px, calc(100vw - 1.4rem));
            max-height: min(72vh, 760px);
            z-index: 1090;
            margin: 0 !important;
            padding: 0.8rem 0.8rem 0.65rem 0.8rem !important;
            border-radius: 18px;
            border: 1px solid rgba(60, 60, 67, 0.18);
            background: #ffffff !important;
            box-shadow: 0 22px 48px rgba(17, 24, 39, 0.22);
            overflow-y: auto;
            transform-origin: bottom right;
            transition: opacity 220ms ease, transform 260ms cubic-bezier(0.2, 0.8, 0.2, 1);
            will-change: transform, opacity;
          }
          [class*="st-key-coco-panel-"] [data-testid="stVerticalBlock"],
          [class*="st-key-coco-panel-"] [data-testid="stHorizontalBlock"],
          [class*="st-key-coco-panel-"] [data-testid="stElementContainer"],
          [class*="st-key-coco_panel_"] [data-testid="stVerticalBlock"],
          [class*="st-key-coco_panel_"] [data-testid="stHorizontalBlock"],
          [class*="st-key-coco_panel_"] [data-testid="stElementContainer"] {
            background: transparent !important;
          }
          [class*="st-key-coco-panel-"] [data-testid="stTextArea"] textarea,
          [class*="st-key-coco_panel_"] [data-testid="stTextArea"] textarea {
            background: #ffffff !important;
          }
          @media (max-width: 780px) {
            [class*="st-key-coco-fab-"],
            [class*="st-key-coco_toggle_panel_"] {
              right: 0.65rem;
              bottom: 0.65rem;
            }
            [class*="st-key-coco-fab-"] [data-testid="stButton"] button,
            [class*="st-key-coco_toggle_panel_"] [data-testid="stButton"] button {
              width: 48px;
              min-width: 48px;
              height: 48px;
            }
            [class*="st-key-coco-panel-"],
            [class*="st-key-coco_panel_"] {
              right: 0.6rem;
              bottom: 4.6rem;
              width: calc(100vw - 1.2rem);
              max-height: min(74vh, 640px);
              border-radius: 14px;
              padding: 0.65rem 0.65rem 0.55rem 0.65rem !important;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.container(key=f"coco-fab-{tenant_id}"):
        if st.button(
            COCO_CHAT_ICON,
            key=f"coco_toggle_panel_{tenant_id}_{username}",
            help="Abrir/Cerrar COCO IA",
            width="content",
        ):
            panel_open = not panel_open
            st.session_state[panel_state_key] = panel_open

    if not panel_open:
        return

    persisted_context = read_coco_chat_state(thread_key)
    usage_rows = read_coco_usage(limit=10_000)
    max_queries = _resolve_coco_daily_limit(coco_cfg, username, tenant_id)
    used_today = _count_coco_queries_today(usage_rows, username, tenant_id)
    query_blocked = max_queries > 0 and used_today >= max_queries
    tenant_budget_usd = _resolve_coco_daily_budget_usd(coco_cfg, tenant_id)
    tenant_cost_today_usd = _coco_tenant_cost_today_usd(usage_rows, tenant_id)
    budget_blocked = tenant_budget_usd > 0 and tenant_cost_today_usd >= tenant_budget_usd
    budget_usage_ratio = (
        (tenant_cost_today_usd / tenant_budget_usd)
        if tenant_budget_usd > 0
        else 0.0
    )
    blocked = bool(query_blocked or budget_blocked)
    remaining_queries = max(max_queries - used_today, 0) if max_queries > 0 else None
    remaining_budget_usd = max(tenant_budget_usd - tenant_cost_today_usd, 0.0) if tenant_budget_usd > 0 else None
    query_usage_ratio = (used_today / max_queries) if max_queries > 0 else 0.0

    if history_key not in st.session_state or not isinstance(st.session_state.get(history_key), list):
        st.session_state[history_key] = []
    if not st.session_state[history_key]:
        persisted_history = read_coco_chat_events(thread_key, limit=COCO_HISTORY_PERSIST_LIMIT)
        if persisted_history:
            st.session_state[history_key] = persisted_history
    history: list[dict[str, str]] = st.session_state.get(history_key, [])
    if context_key not in st.session_state or not isinstance(st.session_state.get(context_key), dict):
        st.session_state[context_key] = {}
    if isinstance(persisted_context, dict):
        last_context = persisted_context.get("last_context", {})
        if isinstance(last_context, dict):
            merged_context = dict(st.session_state.get(context_key, {}))
            merged_context.update(last_context)
            if str(persisted_context.get("preferred_scope", "")).strip().lower() in {"total", "filter"}:
                merged_context["preferred_scope"] = str(persisted_context.get("preferred_scope")).strip().lower()
            st.session_state[context_key] = merged_context

    with st.container(key=f"coco-panel-{tenant_id}"):
        head_col_1, head_col_2 = st.columns([0.72, 0.28])
        with head_col_1:
            st.markdown(f"**{COCO_CHAT_ICON} COCO IA**")
        with head_col_2:
            if st.button("Cerrar", key=f"coco_close_panel_{tenant_id}_{username}", width="stretch"):
                st.session_state[panel_state_key] = False
                panel_open = False

        top_col_1, top_col_2 = st.columns([0.68, 0.32])
        with top_col_1:
            st.caption(
                (
                    f"Consultas hoy: {used_today}/{max_queries} (restantes: {remaining_queries})"
                    if max_queries > 0
                    else f"Consultas hoy: {used_today} (sin límite)"
                )
            )
        with top_col_2:
            if st.button("Limpiar chat", key=f"coco_clear_chat_{tenant_id}_{username}", width="stretch"):
                st.session_state[history_key] = []
                st.session_state[context_key] = {}
                clear_coco_chat_events(thread_key)
                clear_coco_chat_state(thread_key)
                history = []

        st.caption(
            (
                f"Costo tenant hoy: ${tenant_cost_today_usd:,.4f} / "
                f"{'$'+format(tenant_budget_usd,',.4f') if tenant_budget_usd > 0 else 'sin tope'}"
                + (f" (restante: ${remaining_budget_usd:,.4f})" if remaining_budget_usd is not None else "")
            )
        )
        st.caption(f"Tenant: {tenant_name} | Plataforma: {platform} | Rango: {s.isoformat()} - {e.isoformat()}")
        if max_queries > 0 and query_usage_ratio >= COCO_DAILY_QUERY_ALERT_RATIO and not query_blocked:
            st.warning(
                f"Consumo alto de consultas: {query_usage_ratio*100:.1f}% del límite diario."
            )
        if tenant_budget_usd > 0 and budget_usage_ratio >= COCO_DAILY_BUDGET_ALERT_RATIO and not budget_blocked:
            st.warning(
                f"Consumo alto: {budget_usage_ratio*100:.1f}% del presupuesto diario del tenant."
            )

        recent_history = history[-8:] if len(history) > 8 else history
        for msg in recent_history:
            role = str(msg.get("role", "assistant")).strip().lower()
            content = str(msg.get("content", "")).strip()
            if not content:
                continue
            with st.chat_message("user" if role == "user" else "assistant"):
                st.write(content)

        with st.form(key=f"coco_ia_form_{tenant_id}_{username}", clear_on_submit=True):
            user_question = st.text_area(
                "Pregunta a COCO IA",
                value="",
                height=84,
                placeholder="Ej: ¿Cómo va el CTR vs el periodo anterior y qué recomiendas?",
                label_visibility="collapsed",
            )
            submit_question = st.form_submit_button(
                "Preguntar",
                type="primary",
                use_container_width=True,
            )

        if query_blocked:
            st.error(
                f"Límite diario alcanzado: {used_today}/{max_queries} consultas para @{username} en {tenant_name}."
            )
        if budget_blocked:
            st.error(
                f"Presupuesto diario agotado: ${tenant_cost_today_usd:,.4f} / ${tenant_budget_usd:,.4f} para {tenant_name}."
            )

        if submit_question:
            clean_question = str(user_question).strip()
            include_actions = coco_det._question_requests_actions(clean_question)
            include_platform_breakdown = coco_det._question_requests_platform_breakdown(clean_question)
            scope_guard = coco_det._coco_scope_guard_message(clean_question)
            if scope_guard:
                st.warning(scope_guard)
            else:
                conversation_ctx = (
                    dict(st.session_state.get(context_key, {}))
                    if isinstance(st.session_state.get(context_key), dict)
                    else {}
                )
                scope_mode, scope_source = coco_det._resolve_coco_scope_mode(
                    clean_question,
                    last_context=conversation_ctx,
                    default_mode=COCO_DEFAULT_SCOPE_MODE,
                )
                asked_platform = coco_det._platform_from_question(clean_question)
                if scope_mode == "total":
                    query_platform = asked_platform if asked_platform in {"Meta", "Google"} else "All"
                else:
                    query_platform = asked_platform if asked_platform in {"Meta", "Google"} else platform

                has_base_dates = bool(
                    not df_base.empty
                    and "date" in df_base.columns
                    and pd.to_datetime(df_base["date"], errors="coerce").notna().any()
                )
                if has_base_dates:
                    data_min = df_base["date"].min()
                    data_max = df_base["date"].max()
                    if not isinstance(data_min, date) or not isinstance(data_max, date):
                        has_base_dates = False
                        data_min = s
                        data_max = e
                else:
                    data_min = s
                    data_max = e
                default_start = data_min if scope_mode == "total" else s
                default_end = data_max if scope_mode == "total" else e

                conversation_ctx["preferred_scope"] = scope_mode
                conversation_ctx["scope_source"] = scope_source
                st.session_state[context_key] = conversation_ctx
                write_coco_chat_state(
                    thread_key,
                    {
                        "preferred_scope": scope_mode,
                        "last_context": conversation_ctx,
                    },
                )

                if blocked:
                    if query_blocked and budget_blocked:
                        block_reason = "daily_limit_and_tenant_budget_reached"
                        block_message = (
                            f"Consulta bloqueada: límite ({used_today}/{max_queries}) y presupuesto "
                            f"(${tenant_cost_today_usd:,.4f}/${tenant_budget_usd:,.4f}) agotados."
                        )
                    elif query_blocked:
                        block_reason = "daily_limit_reached"
                        block_message = f"Consulta bloqueada: límite diario alcanzado ({used_today}/{max_queries})."
                    else:
                        block_reason = "tenant_budget_reached"
                        block_message = (
                            "Consulta bloqueada: presupuesto diario del tenant agotado "
                            f"(${tenant_cost_today_usd:,.4f}/${tenant_budget_usd:,.4f})."
                        )
                    append_coco_usage_event(
                        actor=username,
                        tenant_id=tenant_id,
                        query=clean_question,
                        response="",
                        provider=str(coco_cfg.get("provider", COCO_DEFAULT_PROVIDER)),
                        model=str(coco_cfg.get("model", COCO_DEFAULT_MODEL)),
                        input_tokens=0,
                        output_tokens=0,
                        cost_usd=0.0,
                        status="blocked",
                        error=block_reason,
                    )
                    st.error(block_message)
                else:
                    answer = ""
                    in_tokens = 0
                    out_tokens = 0
                    provider_used = "local"
                    model_used = "deterministic"
                    err = ""
                    context_note = ""

                    deterministic_answer, deterministic_model_used, deterministic_meta, deterministic_context_note = coco_workflow.run_deterministic_resolver_chain(
                        question=clean_question,
                        tenant_id=tenant_id,
                        camp_df=camp_df,
                        piece_df=piece_df,
                        df_base=df_base,
                        query_platform=query_platform,
                        default_start=default_start,
                        default_end=default_end,
                        include_actions=include_actions,
                        include_platform_breakdown=include_platform_breakdown,
                        last_context=st.session_state.get(context_key, {}),
                    )
                    if deterministic_answer:
                        answer = deterministic_answer
                        model_used = deterministic_model_used
                        if deterministic_meta:
                            merged_ctx = dict(st.session_state.get(context_key, {}))
                            merged_ctx.update(deterministic_meta)
                            merged_ctx["preferred_scope"] = scope_mode
                            merged_ctx["scope_source"] = scope_source
                            st.session_state[context_key] = merged_ctx
                            write_coco_chat_state(
                                thread_key,
                                {
                                    "preferred_scope": scope_mode,
                                    "last_context": merged_ctx,
                                },
                            )
                            context_note = deterministic_context_note
                    else:
                        context_start = default_start
                        context_end = default_end
                        if has_base_dates:
                            context_start, context_end, explicit_time_range = coco_det._resolve_question_range(
                                clean_question,
                                data_min=data_min,
                                data_max=data_max,
                                ui_start=default_start,
                                ui_end=default_end,
                            )
                            scoped_df = df_base[
                                (df_base["date"] >= context_start) & (df_base["date"] <= context_end)
                            ].copy()
                            scope_days = max((context_end - context_start).days + 1, 1)
                            prev_end = context_start - timedelta(days=1)
                            prev_start = prev_end - timedelta(days=scope_days - 1)
                            scoped_prev_df = df_base[
                                (df_base["date"] >= prev_start) & (df_base["date"] <= prev_end)
                            ].copy()
                            total_source_df = scoped_df if explicit_time_range else df_base
                        else:
                            scoped_df = df_sel.copy()
                            scoped_prev_df = df_prev.copy()
                            total_source_df = df_sel.copy()
                            explicit_time_range = False
                        cur_summary = summary(scoped_df, query_platform)
                        prev_summary = summary(scoped_prev_df, query_platform)
                        active_cur_summary = summary(df_sel, platform)
                        active_prev_summary = summary(df_prev, platform)
                        total_all_summary = summary(total_source_df, "All")
                        total_selected_summary = summary(total_source_df, query_platform)
                        has_data_in_requested_range = bool(not scoped_df.empty)
                        conversation_history = list(history)
                        memory_summary = _summarize_coco_history(conversation_history)
                        raw_context = coco_context.build_coco_metrics_context(
                            tenant_name=tenant_name,
                            tenant_id=tenant_id,
                            platform=query_platform,
                            s=context_start,
                            e=context_end,
                            cur_summary=cur_summary,
                            prev_summary=prev_summary,
                            scope_mode=scope_mode,
                            scope_source=scope_source,
                            total_start=data_min,
                            total_end=data_max,
                            active_start=s,
                            active_end=e,
                            total_summary_all=total_all_summary,
                            total_summary_selected=total_selected_summary,
                            active_summary=active_cur_summary,
                            active_prev_summary=active_prev_summary,
                            requested_range_explicit=bool(explicit_time_range),
                            has_data_in_requested_range=has_data_in_requested_range,
                        )
                        raw_context["conversation_memory"] = {
                            "summary": memory_summary,
                            "turn_count": len(conversation_history),
                        }
                        safe_context = coco_context.sanitize_coco_context(
                            raw_context,
                            tenant_id=tenant_id,
                            platform=query_platform,
                            start_day=context_start,
                            end_day=context_end,
                        )
                        with st.spinner("COCO IA analizando datos del tenant..."):
                            answer, in_tokens, out_tokens, provider_used, model_used, err = _generate_coco_answer(
                                question=clean_question,
                                context=safe_context,
                                coco_cfg=coco_cfg,
                                conversation_history=conversation_history,
                                memory_summary=memory_summary,
                                include_actions=include_actions,
                            )
                        merged_ctx = dict(st.session_state.get(context_key, {}))
                        merged_ctx.update(
                            {
                                "preferred_scope": scope_mode,
                                "scope_source": scope_source,
                                "resolver": "llm_context",
                                "last_scope_range_start": context_start.isoformat(),
                                "last_scope_range_end": context_end.isoformat(),
                                "last_scope_platform": query_platform,
                            }
                        )
                        st.session_state[context_key] = merged_ctx
                        write_coco_chat_state(
                            thread_key,
                            {
                                "preferred_scope": scope_mode,
                                "last_context": merged_ctx,
                            },
                        )
                        context_note = (
                            f"Contexto aplicado: tenant `{safe_context.get('tenant_id')}`, "
                            f"rango `{safe_context.get('date_range', {}).get('start')}` a "
                            f"`{safe_context.get('date_range', {}).get('end')}`, plataforma `{safe_context.get('platform')}`, "
                            f"scope `{safe_context.get('scope', {}).get('mode', COCO_DEFAULT_SCOPE_MODE)}`."
                        )
                    if answer and "**Resumen**" not in answer and provider_used != "openai":
                        answer = _format_coco_structured_answer(
                            headline="Respuesta de COCO IA generada con el contexto actual.",
                            findings=[answer],
                            actions=(
                                ["Validar contra el dashboard y ajustar por plataforma/canal."]
                                if include_actions
                                else []
                            ),
                        )
                    if str(err).strip():
                        answer = _format_coco_structured_answer(
                            headline="Respuesta con fallback local.",
                            findings=[answer],
                            actions=(
                                ["Configura/valida OpenAI para respuestas más completas."]
                                if include_actions
                                else []
                            ),
                            note=err,
                        ) if answer else answer
                    if answer:
                        if context_note and provider_used != "openai":
                            answer += f"\n\n_{context_note}_"
                    cost_usd = _estimate_coco_cost_usd(in_tokens, out_tokens, coco_cfg)
                    append_coco_usage_event(
                        actor=username,
                        tenant_id=tenant_id,
                        query=clean_question,
                        response=answer,
                        provider=provider_used,
                        model=model_used,
                        input_tokens=in_tokens,
                        output_tokens=out_tokens,
                        cost_usd=cost_usd,
                        status="ok" if answer else "error",
                        error=err,
                    )
                    if answer:
                        history.append({"role": "user", "content": clean_question})
                        history.append({"role": "assistant", "content": answer})
                        st.session_state[history_key] = history[-COCO_HISTORY_PERSIST_LIMIT:]
                        append_coco_chat_event(
                            thread_key=thread_key,
                            actor=username,
                            tenant_id=tenant_id,
                            role="user",
                            content=clean_question,
                        )
                        append_coco_chat_event(
                            thread_key=thread_key,
                            actor=username,
                            tenant_id=tenant_id,
                            role="assistant",
                            content=answer,
                        )
                        write_coco_chat_state(
                            thread_key,
                            {
                                "preferred_scope": str(st.session_state.get(context_key, {}).get("preferred_scope", COCO_DEFAULT_SCOPE_MODE)),
                                "last_context": st.session_state.get(context_key, {}),
                            },
                        )
                        st.rerun()
                    else:
                        st.error("COCO IA no pudo generar respuesta en este momento.")

    panel_opacity = "1" if panel_open else "0"
    panel_transform = "translate3d(0, 0, 0) scale(1)" if panel_open else "translate3d(0, 16px, 0) scale(0.98)"
    panel_pointer_events = "auto" if panel_open else "none"
    st.markdown(
        f"""
        <style>
          [class*="st-key-coco-panel-"],
          [class*="st-key-coco_panel_"] {{
            opacity: {panel_opacity} !important;
            transform: {panel_transform} !important;
            pointer-events: {panel_pointer_events} !important;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_admin_coco_ia_panel(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
    dashboard_settings: dict[str, Any],
) -> None:
    st.markdown("### COCO IA")
    current_cfg = _normalize_coco_ia_settings(dashboard_settings.get("coco_ia", {}), tenants)

    st.caption("Configura límites de uso y monitorea consumo de tokens/costo por tenant y usuario.")
    cfg_col_1, cfg_col_2 = st.columns(2)
    with cfg_col_1:
        enabled = st.toggle("Activar COCO IA", value=bool(current_cfg.get("enabled", False)), key="coco_enabled")
        provider = st.selectbox(
            "Proveedor IA",
            options=["openai", "local"],
            index=0 if str(current_cfg.get("provider", "openai")).strip().lower() == "openai" else 1,
            key="coco_provider",
        )
        model = st.text_input(
            "Modelo",
            value=str(current_cfg.get("model", COCO_DEFAULT_MODEL)),
            key="coco_model",
            help="Ejemplo OpenAI: gpt-4o-mini",
        )
        if provider == "openai":
            key_ok, key_mask = _openai_key_status()
            st.caption(f"API key OpenAI: {'OK' if key_ok else 'No disponible'} ({key_mask})")
            if st.button("Probar conexión OpenAI", key="coco_probe_openai", width="stretch"):
                ok_conn, msg_conn = _check_openai_connectivity(timeout=10)
                if ok_conn:
                    st.success(msg_conn)
                else:
                    st.error(msg_conn)
    with cfg_col_2:
        max_daily_default = int(
            st.number_input(
                "Máximo consultas/día (default)",
                min_value=0,
                max_value=10_000,
                value=int(current_cfg.get("max_daily_queries_default", COCO_DEFAULT_MAX_DAILY_QUERIES)),
                step=1,
                key="coco_default_limit",
                help="0 = sin límite.",
            )
        )
        daily_budget_default = float(
            st.number_input(
                "Presupuesto diario USD (default)",
                min_value=0.0,
                max_value=1_000_000.0,
                value=float(current_cfg.get("daily_budget_usd_default", COCO_DEFAULT_DAILY_BUDGET_USD)),
                step=0.5,
                key="coco_daily_budget_default",
                help="0 = sin límite por costo.",
                format="%.2f",
            )
        )
        input_cost_per_1m = float(
            st.number_input(
                "Costo input USD / 1M tokens",
                min_value=0.0,
                max_value=1000.0,
                value=float(current_cfg.get("input_cost_per_1m", COCO_DEFAULT_INPUT_COST_PER_1M)),
                step=0.01,
                key="coco_input_cost",
                format="%.4f",
            )
        )
        output_cost_per_1m = float(
            st.number_input(
                "Costo output USD / 1M tokens",
                min_value=0.0,
                max_value=1000.0,
                value=float(current_cfg.get("output_cost_per_1m", COCO_DEFAULT_OUTPUT_COST_PER_1M)),
                step=0.01,
                key="coco_output_cost",
                format="%.4f",
            )
        )

    st.markdown("#### Activación por tenant")
    tenant_enabled_values: dict[str, bool] = {}
    tenant_ids_sorted = sorted(tenants.keys())
    if tenant_ids_sorted:
        enabled_cols = st.columns(min(3, len(tenant_ids_sorted)))
        for idx, tenant_id in enumerate(tenant_ids_sorted):
            tenant_name = str(tenants.get(tenant_id, {}).get("name", tenant_id))
            tenant_key = str(tenant_id).strip().lower()
            default_tenant_enabled = bool(
                current_cfg.get("enabled_tenants", {}).get(tenant_key, bool(enabled))
            )
            with enabled_cols[idx % len(enabled_cols)]:
                tenant_enabled_values[tenant_key] = st.toggle(
                    f"{tenant_name} ({tenant_id})",
                    value=default_tenant_enabled,
                    key=f"coco_enabled_tenant_{tenant_key}",
                )
    if bool(enabled) and tenant_enabled_values and not any(bool(v) for v in tenant_enabled_values.values()):
        st.warning("COCO IA está activo globalmente, pero no hay tenants habilitados.")

    st.markdown("#### Límites por tenant")
    tenant_limit_values: dict[str, int] = {}
    if tenant_ids_sorted:
        tenant_cols = st.columns(min(3, len(tenant_ids_sorted)))
        for idx, tenant_id in enumerate(tenant_ids_sorted):
            tenant_name = str(tenants.get(tenant_id, {}).get("name", tenant_id))
            tenant_key = str(tenant_id).strip().lower()
            default_tenant_limit = int(current_cfg.get("max_daily_queries_tenant", {}).get(tenant_key, max_daily_default))
            with tenant_cols[idx % len(tenant_cols)]:
                tenant_limit_values[tenant_key] = int(
                    st.number_input(
                        f"{tenant_name} ({tenant_id})",
                        min_value=0,
                        max_value=10_000,
                        value=default_tenant_limit,
                        step=1,
                        key=f"coco_limit_tenant_{tenant_key}",
                    )
                )

    st.markdown("#### Presupuesto diario por tenant (USD)")
    tenant_budget_values: dict[str, float] = {}
    if tenant_ids_sorted:
        budget_cols = st.columns(min(3, len(tenant_ids_sorted)))
        for idx, tenant_id in enumerate(tenant_ids_sorted):
            tenant_name = str(tenants.get(tenant_id, {}).get("name", tenant_id))
            tenant_key = str(tenant_id).strip().lower()
            default_tenant_budget = float(
                current_cfg.get("daily_budget_usd_tenant", {}).get(tenant_key, daily_budget_default)
            )
            with budget_cols[idx % len(budget_cols)]:
                tenant_budget_values[tenant_key] = float(
                    st.number_input(
                        f"{tenant_name} ({tenant_id})",
                        min_value=0.0,
                        max_value=1_000_000.0,
                        value=default_tenant_budget,
                        step=0.5,
                        key=f"coco_budget_tenant_{tenant_key}",
                        format="%.2f",
                    )
                )

    st.markdown("#### Límites por usuario")
    user_limit_values: dict[str, int] = {}
    usernames_sorted = sorted(users.keys())
    if usernames_sorted:
        user_cols = st.columns(min(3, len(usernames_sorted)))
        for idx, username in enumerate(usernames_sorted):
            user_key = str(username).strip().lower()
            default_user_limit = int(current_cfg.get("max_daily_queries_user", {}).get(user_key, max_daily_default))
            with user_cols[idx % len(user_cols)]:
                user_limit_values[user_key] = int(
                    st.number_input(
                        f"@{user_key}",
                        min_value=0,
                        max_value=10_000,
                        value=default_user_limit,
                        step=1,
                        key=f"coco_limit_user_{user_key}",
                    )
                )

    with st.expander("Overrides avanzados usuario@tenant", expanded=False):
        raw_rows: list[dict[str, Any]] = []
        current_user_tenant_limits = current_cfg.get("max_daily_queries_user_tenant", {})
        if isinstance(current_user_tenant_limits, dict):
            for key, value in sorted(current_user_tenant_limits.items()):
                user_part, tenant_part = (str(key).split("@", 1) + [""])[:2] if "@" in str(key) else ("", "")
                raw_rows.append(
                    {
                        "usuario": str(user_part).strip().lower(),
                        "tenant_id": str(tenant_part).strip().lower(),
                        "max_consultas_dia": _normalize_non_negative_int(value, max_daily_default, minimum=0, maximum=10_000),
                    }
                )
        edited_overrides = st.data_editor(
            pd.DataFrame(raw_rows or [{"usuario": "", "tenant_id": "", "max_consultas_dia": max_daily_default}]),
            num_rows="dynamic",
            width="stretch",
            key="coco_user_tenant_overrides_editor",
            hide_index=True,
        )

    save_coco_cfg = st.button("Guardar configuración COCO IA", type="primary", width="stretch", key="save_coco_ia_cfg")
    if save_coco_cfg:
        normalized_user_tenant: dict[str, int] = {}
        if isinstance(edited_overrides, pd.DataFrame):
            for _, row in edited_overrides.iterrows():
                raw_user = str(row.get("usuario", "")).strip().lower()
                raw_tenant = str(row.get("tenant_id", "")).strip().lower()
                if not raw_user or not raw_tenant:
                    continue
                key = f"{raw_user}@{raw_tenant}"
                normalized_user_tenant[key] = _normalize_non_negative_int(
                    row.get("max_consultas_dia"),
                    max_daily_default,
                    minimum=0,
                    maximum=10_000,
                )

        next_settings = load_dashboard_settings(DASHBOARD_SETTINGS_PATH, tenants)
        next_settings["coco_ia"] = {
            "enabled": bool(enabled),
            "enabled_tenants": tenant_enabled_values,
            "provider": str(provider).strip().lower() or COCO_DEFAULT_PROVIDER,
            "model": str(model).strip() or COCO_DEFAULT_MODEL,
            "max_daily_queries_default": _normalize_non_negative_int(max_daily_default, COCO_DEFAULT_MAX_DAILY_QUERIES, minimum=0, maximum=10_000),
            "max_daily_queries_tenant": tenant_limit_values,
            "max_daily_queries_user": user_limit_values,
            "max_daily_queries_user_tenant": normalized_user_tenant,
            "daily_budget_usd_default": _normalize_non_negative_float(daily_budget_default, COCO_DEFAULT_DAILY_BUDGET_USD, minimum=0.0, maximum=1_000_000.0),
            "daily_budget_usd_tenant": {
                k: _normalize_non_negative_float(v, daily_budget_default, minimum=0.0, maximum=1_000_000.0)
                for k, v in tenant_budget_values.items()
            },
            "input_cost_per_1m": _normalize_non_negative_float(input_cost_per_1m, COCO_DEFAULT_INPUT_COST_PER_1M, minimum=0.0, maximum=1000.0),
            "output_cost_per_1m": _normalize_non_negative_float(output_cost_per_1m, COCO_DEFAULT_OUTPUT_COST_PER_1M, minimum=0.0, maximum=1000.0),
        }
        ok, err_msg = save_dashboard_settings(DASHBOARD_SETTINGS_PATH, next_settings, tenants)
        if not ok:
            st.error(f"No se pudo guardar {DASHBOARD_SETTINGS_PATH.name}: {err_msg}")
        else:
            append_admin_audit(
                "coco_ia_settings_updated",
                str(auth_user.get("username", "unknown")),
                target="coco_ia",
                details={
                    "enabled": bool(enabled),
                    "enabled_tenants": [k for k, v in sorted(tenant_enabled_values.items()) if bool(v)],
                    "provider": provider,
                    "model": model,
                    "max_daily_queries_default": max_daily_default,
                    "daily_budget_usd_default": daily_budget_default,
                },
            )
            st.success("Configuración de COCO IA guardada.")
            st.rerun()

    st.markdown("#### Consumo y costo")
    usage_rows = read_coco_usage(limit=20_000)
    reset_state_key = "coco_usage_reset_armed"
    if reset_state_key not in st.session_state:
        st.session_state[reset_state_key] = False

    reset_col_1, reset_col_2 = st.columns([0.72, 0.28], gap="small")
    with reset_col_1:
        st.caption(f"Fuente: {AI_USAGE_LOG_PATH.name} | Eventos actuales: {len(usage_rows)}")
    with reset_col_2:
        if st.button("Resetear consumo", key="coco_usage_reset_open", width="stretch"):
            st.session_state[reset_state_key] = True

    if bool(st.session_state.get(reset_state_key)):
        st.warning("Se borrará el historial de consumo de COCO IA (tokens/costo). Se guardará backup automático.")
        confirm_col_1, confirm_col_2 = st.columns(2, gap="small")
        with confirm_col_1:
            if st.button("Confirmar reset", type="primary", key="coco_usage_reset_confirm", width="stretch"):
                removed_events = len(usage_rows)
                ok_reset, reset_info = clear_coco_usage_log()
                st.session_state[reset_state_key] = False
                if not ok_reset:
                    st.error(f"No se pudo limpiar el historial: {reset_info}")
                else:
                    append_admin_audit(
                        "coco_ia_usage_reset",
                        str(auth_user.get("username", "unknown")),
                        target="coco_ia",
                        details={
                            "events_removed": removed_events,
                            "backup_path": str(reset_info or ""),
                        },
                    )
                    if str(reset_info).strip():
                        st.success(f"Historial de COCO IA limpiado. Backup: {reset_info}")
                    else:
                        st.success("Historial de COCO IA limpiado.")
                    st.rerun()
        with confirm_col_2:
            if st.button("Cancelar", key="coco_usage_reset_cancel", width="stretch"):
                st.session_state[reset_state_key] = False
                st.rerun()

    if not usage_rows:
        st.info("Todavía no hay consumo registrado de COCO IA.")
        return

    available_dates: list[date] = []
    available_tenants: set[str] = set()
    available_users: set[str] = set()
    available_statuses: set[str] = set()
    for row in usage_rows:
        row_day = _parse_iso_date(row.get("event_date")) or (
            _parse_coco_timestamp(row.get("timestamp_utc")).date()
            if _parse_coco_timestamp(row.get("timestamp_utc")) is not None
            else None
        )
        if row_day is not None:
            available_dates.append(row_day)
        row_tenant = str(row.get("tenant_id", "")).strip().lower()
        row_user = str(row.get("actor", "")).strip().lower()
        row_status = str(row.get("status", "")).strip().lower()
        if row_tenant:
            available_tenants.add(row_tenant)
        if row_user:
            available_users.add(row_user)
        if row_status:
            available_statuses.add(row_status)

    min_date = min(available_dates) if available_dates else date.today()
    max_date = max(available_dates) if available_dates else date.today()
    flt_col_1, flt_col_2, flt_col_3, flt_col_4 = st.columns([1.4, 1.0, 1.0, 1.0], gap="small")
    with flt_col_1:
        raw_date_range = st.date_input(
            "Rango consumo",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key="coco_usage_date_filter",
        )
    with flt_col_2:
        tenant_filter_options = ["Todos"] + sorted([t for t in available_tenants if t])
        selected_tenant_filter = st.selectbox(
            "Tenant",
            options=tenant_filter_options,
            key="coco_usage_tenant_filter",
        )
    with flt_col_3:
        user_filter_options = ["Todos"] + sorted([u for u in available_users if u])
        selected_user_filter = st.selectbox(
            "Usuario",
            options=user_filter_options,
            key="coco_usage_user_filter",
        )
    with flt_col_4:
        status_filter_options = ["Todos"] + sorted([s for s in available_statuses if s])
        selected_status_filter = st.selectbox(
            "Estado",
            options=status_filter_options,
            key="coco_usage_status_filter",
        )

    filter_start, filter_end = _normalize_date_range(raw_date_range, min_date, max_date)
    filtered_rows: list[dict[str, Any]] = []
    for row in usage_rows:
        row_day = _parse_iso_date(row.get("event_date")) or (
            _parse_coco_timestamp(row.get("timestamp_utc")).date()
            if _parse_coco_timestamp(row.get("timestamp_utc")) is not None
            else None
        )
        if row_day is None or row_day < filter_start or row_day > filter_end:
            continue
        row_tenant = str(row.get("tenant_id", "")).strip().lower()
        row_user = str(row.get("actor", "")).strip().lower()
        row_status = str(row.get("status", "")).strip().lower()
        if selected_tenant_filter != "Todos" and row_tenant != str(selected_tenant_filter).strip().lower():
            continue
        if selected_user_filter != "Todos" and row_user != str(selected_user_filter).strip().lower():
            continue
        if selected_status_filter != "Todos" and row_status != str(selected_status_filter).strip().lower():
            continue
        filtered_rows.append(row)

    ok_rows = [row for row in filtered_rows if str(row.get("status", "")).strip().lower() == "ok"]
    blocked_rows = [row for row in filtered_rows if str(row.get("status", "")).strip().lower() == "blocked"]
    error_rows = [row for row in filtered_rows if str(row.get("status", "")).strip().lower() == "error"]
    total_queries = len(ok_rows)
    total_tokens = sum(_normalize_non_negative_int(row.get("total_tokens"), 0, minimum=0, maximum=10_000_000) for row in ok_rows)
    total_cost = sum(_normalize_non_negative_float(row.get("cost_usd"), 0.0, minimum=0.0, maximum=1_000_000.0) for row in ok_rows)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Consultas OK (filtro)", f"{total_queries}")
    m2.metric("Tokens OK (filtro)", f"{total_tokens:,}")
    m3.metric("Costo OK (USD)", f"${total_cost:,.4f}")
    m4.metric("Eventos filtro", f"{len(filtered_rows)}")
    if not filtered_rows:
        st.info("No hay eventos de COCO IA para el filtro actual.")
        st.caption(f"Fuente: {AI_USAGE_LOG_PATH.name}")
        return

    # Presupuesto diario por tenant: alertas del día actual.
    today_iso = date.today().isoformat()
    today_ok_rows = [row for row in usage_rows if str(row.get("status", "")).strip().lower() == "ok" and str(row.get("event_date", "")).strip() == today_iso]
    tenant_today_cost: dict[str, float] = {}
    for row in today_ok_rows:
        tenant_key = str(row.get("tenant_id", "")).strip().lower()
        tenant_today_cost[tenant_key] = tenant_today_cost.get(tenant_key, 0.0) + _normalize_non_negative_float(row.get("cost_usd"), 0.0, minimum=0.0, maximum=1_000_000.0)
    budget_alerts: list[str] = []
    for tenant_key, cost_today in sorted(tenant_today_cost.items()):
        budget = _resolve_coco_daily_budget_usd(current_cfg, tenant_key)
        if budget <= 0:
            continue
        ratio = cost_today / budget if budget > 0 else 0.0
        if ratio >= 1.0:
            budget_alerts.append(f"{tenant_key}: presupuesto excedido ({ratio*100:.1f}%, ${cost_today:,.4f}/${budget:,.4f})")
        elif ratio >= COCO_DAILY_BUDGET_ALERT_RATIO:
            budget_alerts.append(f"{tenant_key}: alerta consumo alto ({ratio*100:.1f}%, ${cost_today:,.4f}/${budget:,.4f})")
    for msg in budget_alerts:
        if "excedido" in msg:
            st.error(msg)
        else:
            st.warning(msg)

    grouped: dict[str, dict[str, Any]] = {}
    for row in ok_rows:
        actor = str(row.get("actor", "unknown")).strip().lower() or "unknown"
        tenant = str(row.get("tenant_id", "unknown")).strip().lower() or "unknown"
        key = f"{actor}@{tenant}"
        if key not in grouped:
            grouped[key] = {
                "Cuenta": key,
                "Usuario": actor,
                "Tenant": tenant,
                "Consultas": 0,
                "Tokens": 0,
                "Costo USD": 0.0,
                "Última consulta UTC": "",
            }
        grouped[key]["Consultas"] += 1
        grouped[key]["Tokens"] += _normalize_non_negative_int(row.get("total_tokens"), 0, minimum=0, maximum=10_000_000)
        grouped[key]["Costo USD"] += _normalize_non_negative_float(row.get("cost_usd"), 0.0, minimum=0.0, maximum=1_000_000.0)
        timestamp = str(row.get("timestamp_utc", "")).strip()
        if timestamp and (not grouped[key]["Última consulta UTC"] or timestamp > grouped[key]["Última consulta UTC"]):
            grouped[key]["Última consulta UTC"] = timestamp

    grouped_rows = sorted(grouped.values(), key=lambda item: float(item.get("Costo USD", 0.0)), reverse=True)
    if grouped_rows:
        top = grouped_rows[0]
        st.info(
            "Cuenta con mayor consumo (filtro): "
            f"{top.get('Cuenta')} | Costo ${float(top.get('Costo USD', 0.0)):,.4f} | "
            f"Tokens {int(top.get('Tokens', 0)):,} | Consultas {int(top.get('Consultas', 0))}"
        )
        st.dataframe(pd.DataFrame(grouped_rows), width="stretch", hide_index=True)

    if ok_rows:
        tenant_rank: dict[str, dict[str, Any]] = {}
        user_rank: dict[str, dict[str, Any]] = {}
        for row in ok_rows:
            tenant_key = str(row.get("tenant_id", "unknown")).strip().lower() or "unknown"
            user_key = str(row.get("actor", "unknown")).strip().lower() or "unknown"
            tokens = _normalize_non_negative_int(row.get("total_tokens"), 0, minimum=0, maximum=10_000_000)
            cost = _normalize_non_negative_float(row.get("cost_usd"), 0.0, minimum=0.0, maximum=1_000_000.0)

            if tenant_key not in tenant_rank:
                tenant_rank[tenant_key] = {"Tenant": tenant_key, "Consultas": 0, "Tokens": 0, "Costo USD": 0.0}
            tenant_rank[tenant_key]["Consultas"] += 1
            tenant_rank[tenant_key]["Tokens"] += tokens
            tenant_rank[tenant_key]["Costo USD"] += cost

            if user_key not in user_rank:
                user_rank[user_key] = {"Usuario": user_key, "Consultas": 0, "Tokens": 0, "Costo USD": 0.0}
            user_rank[user_key]["Consultas"] += 1
            user_rank[user_key]["Tokens"] += tokens
            user_rank[user_key]["Costo USD"] += cost

        rank_col_1, rank_col_2 = st.columns(2, gap="small")
        with rank_col_1:
            st.markdown("**Ranking por tenant (filtro)**")
            st.dataframe(
                pd.DataFrame(sorted(tenant_rank.values(), key=lambda item: float(item.get("Costo USD", 0.0)), reverse=True)),
                width="stretch",
                hide_index=True,
            )
        with rank_col_2:
            st.markdown("**Ranking por usuario (filtro)**")
            st.dataframe(
                pd.DataFrame(sorted(user_rank.values(), key=lambda item: float(item.get("Costo USD", 0.0)), reverse=True)),
                width="stretch",
                hide_index=True,
            )

    st.markdown("#### Consultas recientes")
    recent_rows: list[dict[str, Any]] = []
    for row in filtered_rows[:300]:
        recent_rows.append(
            {
                "timestamp_utc": str(row.get("timestamp_utc", "")),
                "event_date": str(row.get("event_date", "")),
                "tenant": str(row.get("tenant_id", "")),
                "usuario": str(row.get("actor", "")),
                "estado": str(row.get("status", "")),
                "provider": str(row.get("provider", "")),
                "model": str(row.get("model", "")),
                "tokens": _normalize_non_negative_int(row.get("total_tokens"), 0, minimum=0, maximum=10_000_000),
                "cost_usd": round(_normalize_non_negative_float(row.get("cost_usd"), 0.0, minimum=0.0, maximum=1_000_000.0), 6),
                "query": str(row.get("query", ""))[:180],
                "respuesta_preview": str(row.get("response_preview", ""))[:180],
                "error": str(row.get("error", ""))[:180],
            }
        )
    if recent_rows:
        st.dataframe(pd.DataFrame(recent_rows), width="stretch", hide_index=True)
    else:
        st.info("No hay eventos para el filtro seleccionado.")

    st.caption(
        f"Eventos filtro: OK={len(ok_rows)} | Bloqueados={len(blocked_rows)} | Errores={len(error_rows)} | "
        f"Fuente: {AI_USAGE_LOG_PATH.name}"
    )


def render_admin_panel(
    users: dict[str, dict[str, Any]],
    tenants: dict[str, dict[str, Any]],
    auth_user: dict[str, Any],
    dashboard_settings: dict[str, Any],
    admin_section: str,
) -> None:
    section_title("04 > Administración por Tenant (Fase 4)")

    if not USERS_CONFIG_PATH.exists():
        st.warning("No existe config/users.json todavía. Puedes crearlo desde este panel.")

    if admin_section == "users":
        _render_admin_users_wireframe(users, tenants, auth_user)
        return

    if admin_section == "coco_ia":
        _render_admin_coco_ia_panel(users, tenants, auth_user, dashboard_settings)
        return

    if admin_section == "users":
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
    if admin_section == "users":
        st.markdown("### Editar Usuario")
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
            tenant_options_set = set(tenant_options)
            selected_defaults = [tenant_id for tenant_id in selected_defaults if tenant_id in tenant_options_set]
            if not selected_defaults and existing_global_role == "admin":
                selected_defaults = ["*"]

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

    if admin_section == "users":
        st.markdown("### Crear Usuario")
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

    if admin_section == "dashboard":
        st.markdown("### Variables Dashboard")
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
        campaign_filters_defaults = _normalize_campaign_filter_keys(
            base_cfg.get("campaign_filters", DEFAULT_CAMPAIGN_FILTER_KEYS),
            DEFAULT_CAMPAIGN_FILTER_KEYS,
        )
        enabled_view_modes_defaults = _normalize_view_mode_keys(
            base_cfg.get("enabled_view_modes", list(VIEW_MODE_OPTIONS)),
            list(VIEW_MODE_OPTIONS),
        )
        default_platform = _normalize_platform_option(base_cfg.get("default_platform", "All"))
        default_view_mode = _normalize_view_mode_option(base_cfg.get("default_view_mode", "Overview"))
        if default_view_mode not in enabled_view_modes_defaults:
            default_view_mode = enabled_view_modes_defaults[0]
        default_tenant_logo = _normalize_logo_source(base_cfg.get("tenant_logo", ""))
        base_theme_colors = _normalize_theme_colors(
            base_cfg.get("theme_colors", DEFAULT_THEME_COLORS),
            DEFAULT_THEME_COLORS,
        )
        kpi_options = list(KPI_CATALOG.keys())
        overview_section_options = list(OVERVIEW_SECTION_OPTIONS.keys())
        traffic_section_options = list(TRAFFIC_SECTION_OPTIONS.keys())
        campaign_filter_options = list(CAMPAIGN_FILTER_OPTIONS.keys())

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
        campaign_filters_selected = st.multiselect(
            "Filtros dinámicos de campañas",
            options=campaign_filter_options,
            default=campaign_filters_defaults,
            format_func=lambda k: str(CAMPAIGN_FILTER_OPTIONS.get(k, k)),
            key=f"adm_dash_campaign_filters_{target_scope}",
            help="Se mostrarán automáticamente solo cuando exista data para ese filtro en el tenant.",
        )
        enabled_view_modes_selected = st.multiselect(
            "Vistas habilitadas en menú lateral",
            options=list(VIEW_MODE_OPTIONS),
            default=enabled_view_modes_defaults,
            key=f"adm_dash_enabled_views_{target_scope}",
            help="Controla qué vistas aparecen en el sidebar para este scope.",
        )
        default_view_mode_options = _normalize_view_mode_keys(
            enabled_view_modes_selected,
            enabled_view_modes_defaults,
        )
        if default_view_mode not in default_view_mode_options:
            default_view_mode = default_view_mode_options[0]
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
                options=default_view_mode_options,
                index=default_view_mode_options.index(default_view_mode)
                if default_view_mode in default_view_mode_options
                else 0,
                key=f"adm_dash_view_{target_scope}",
            )
        tenant_logo_input = st.text_input(
            "Logo tenant (ruta local o URL)",
            value=default_tenant_logo,
            key=f"adm_dash_logo_{target_scope}",
            help=(
                f"Ejemplo local: assets/logos/hyundai.png | URL: https://.../logo.png | "
                f"Si subes archivo, usa {TENANT_LOGO_UPLOAD_WIDTH_PX}x{TENANT_LOGO_UPLOAD_HEIGHT_PX}px."
            ),
        )
        logo_upload = st.file_uploader(
            "Subir logo desde tu computador",
            type=["png", "jpg", "jpeg", "webp"],
            key=f"adm_dash_logo_upload_{target_scope}",
            help=(
                f"Tamaño obligatorio: {TENANT_LOGO_UPLOAD_WIDTH_PX}x{TENANT_LOGO_UPLOAD_HEIGHT_PX}px. "
                "Al guardar, se almacenará en assets/logos y se actualizará la ruta automáticamente."
            ),
        )
        if logo_upload is not None:
            st.image(logo_upload, width=260)
            st.caption(
                f"Archivo listo para guardar: {logo_upload.name} | "
                f"Formato esperado: {TENANT_LOGO_UPLOAD_WIDTH_PX}x{TENANT_LOGO_UPLOAD_HEIGHT_PX}px"
            )
        st.markdown("#### Paleta visual por tenant")
        cpal_1, cpal_2, cpal_3 = st.columns(3)
        with cpal_1:
            color_google = st.color_picker(
                "Google",
                value=base_theme_colors["google"],
                key=f"adm_dash_color_google_{target_scope}",
            )
            color_success = st.color_picker(
                "Éxito",
                value=base_theme_colors["success"],
                key=f"adm_dash_color_success_{target_scope}",
            )
        with cpal_2:
            color_meta = st.color_picker(
                "Meta",
                value=base_theme_colors["meta"],
                key=f"adm_dash_color_meta_{target_scope}",
            )
            color_danger = st.color_picker(
                "Alerta",
                value=base_theme_colors["danger"],
                key=f"adm_dash_color_danger_{target_scope}",
            )
        with cpal_3:
            color_accent = st.color_picker(
                "Accent",
                value=base_theme_colors["accent"],
                key=f"adm_dash_color_accent_{target_scope}",
            )
            color_neutral = st.color_picker(
                "Neutral",
                value=base_theme_colors["neutral"],
                key=f"adm_dash_color_neutral_{target_scope}",
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
            campaign_filters_norm = _normalize_campaign_filter_keys(
                campaign_filters_selected,
                DEFAULT_CAMPAIGN_FILTER_KEYS,
            )
            enabled_view_modes_norm = _normalize_view_mode_keys(
                enabled_view_modes_selected,
                list(VIEW_MODE_OPTIONS),
            )
            selected_view_mode_norm = _normalize_view_mode_option(selected_view_mode)
            if selected_view_mode_norm not in enabled_view_modes_norm:
                selected_view_mode_norm = enabled_view_modes_norm[0]
            uploaded_logo_rel: str | None = None
            if logo_upload is not None:
                scope_for_file = "defaults" if target_scope == "__defaults__" else target_scope
                uploaded_logo_rel, upload_err = _save_uploaded_logo_file(logo_upload, scope_for_file)
                if upload_err:
                    errors.append(upload_err)
            tenant_logo_norm = _normalize_logo_source(uploaded_logo_rel or tenant_logo_input)
            theme_colors_norm = _normalize_theme_colors(
                {
                    "google": color_google,
                    "meta": color_meta,
                    "accent": color_accent,
                    "neutral": color_neutral,
                    "success": color_success,
                    "danger": color_danger,
                },
                base_theme_colors,
            )
            if not overview_norm:
                errors.append("Debes seleccionar al menos 1 KPI para Overview.")
            if not traffic_norm:
                errors.append("Debes seleccionar al menos 1 KPI para Tráfico y Adquisición.")
            if not overview_sections_norm:
                errors.append("Debes seleccionar al menos 1 sección para Overview.")
            if not traffic_sections_norm:
                errors.append("Debes seleccionar al menos 1 sección para Tráfico y Adquisición.")
            if not enabled_view_modes_selected:
                errors.append("Debes habilitar al menos 1 vista del menú lateral.")
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
                    "campaign_filters": campaign_filters_norm,
                    "enabled_view_modes": enabled_view_modes_norm,
                    "default_platform": _normalize_platform_option(selected_platform),
                    "default_view_mode": selected_view_mode_norm,
                    "tenant_logo": tenant_logo_norm,
                    "theme_colors": theme_colors_norm,
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
                            "campaign_filters": campaign_filters_norm,
                            "enabled_view_modes": enabled_view_modes_norm,
                            "default_platform": _normalize_platform_option(selected_platform),
                            "default_view_mode": selected_view_mode_norm,
                            "tenant_logo": tenant_logo_norm,
                            "theme_colors": theme_colors_norm,
                        },
                    )
                    if uploaded_logo_rel:
                        st.success(f"Variables de dashboard guardadas. Logo almacenado en: {uploaded_logo_rel}")
                    else:
                        st.success("Variables de dashboard guardadas.")
                    st.rerun()

        st.caption(f"Fuente: {DASHBOARD_SETTINGS_PATH.name} | Scope: {'defaults' if target_scope == '__defaults__' else target_scope}")

    if admin_section == "audit":
        st.markdown("### Auditoría")
        st.markdown("### Salud de Configuración")
        user_issues = validate_users_integrity(users, tenants)
        settings_snapshot = load_dashboard_settings(DASHBOARD_SETTINGS_PATH, tenants)
        settings_issues = validate_dashboard_settings_integrity(settings_snapshot, tenants)
        all_issues = user_issues + settings_issues
        if all_issues:
            st.error(f"Se detectaron {len(all_issues)} issue(s) de integridad.")
            for issue in all_issues:
                st.write(f"- {issue}")
            tenant_ref_issues = [
                issue
                for issue in user_issues
                if "tenant inexistente" in issue or "scope huérfano" in issue
            ]
            if tenant_ref_issues and st.button(
                "Reparar referencias huérfanas de tenants en usuarios",
                key="repair_users_tenant_integrity_btn",
                width="stretch",
            ):
                repaired_users, repair_notes = repair_users_tenant_integrity(users, tenants)
                if repaired_users == users:
                    st.info("No se detectaron cambios para aplicar en usuarios.")
                else:
                    ok, err_msg = save_users_config(USERS_CONFIG_PATH, repaired_users)
                    if not ok:
                        st.error(f"No se pudo guardar {USERS_CONFIG_PATH.name}: {err_msg}")
                    else:
                        append_admin_audit(
                            "users_tenant_integrity_repaired",
                            str(auth_user.get("username", "unknown")),
                            target="users",
                            details={"changes": repair_notes},
                        )
                        st.success("Se aplicó reparación de referencias huérfanas en usuarios.")
                        if repair_notes:
                            for note in repair_notes:
                                st.write(f"- {note}")
                        st.rerun()
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
    ensure_dashboard_settings_runtime_file(DASHBOARD_SETTINGS_PATH, DASHBOARD_SETTINGS_TEMPLATE_PATH)
    dashboard_settings = load_dashboard_settings(DASHBOARD_SETTINGS_PATH, tenants)
    coco_cfg = _normalize_coco_ia_settings(dashboard_settings.get("coco_ia", {}), tenants)
    tenant_id, view_mode, admin_section = render_sidebar(tenants, dashboard_settings)
    tenant_cfg = tenants.get(tenant_id) or next(iter(tenants.values()))
    tenant_dash_cfg = tenant_dashboard_settings(dashboard_settings, tenant_id)
    tenant_theme_colors = _apply_theme_palette(tenant_dash_cfg.get("theme_colors", DEFAULT_THEME_COLORS))
    apply_tenant_theme_overrides(tenant_theme_colors)
    desired_view_mode = _normalize_view_mode_option(tenant_dash_cfg.get("default_view_mode", "Overview"))
    last_view_tenant = str(st.session_state.get("sidebar_view_tenant_cfg", ""))
    if not last_view_tenant:
        st.session_state["sidebar_view_tenant_cfg"] = tenant_id
    elif last_view_tenant != tenant_id:
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
        admin_report_path = Path(str(tenant_cfg.get("report_path", REPORT_PATH)))
        try:
            admin_report = load_report(admin_report_path)
        except Exception:
            admin_report = {}
        if isinstance(admin_report, dict):
            render_sidebar_meta_token_health(admin_report)
            admin_df = daily_df(admin_report)
            admin_camp = acq_df(admin_report, "meta_campaign_daily")
            if not admin_camp.empty:
                admin_camp["platform"] = "Meta"
            admin_piece = acq_df(admin_report, "paid_piece_daily")
            if not admin_piece.empty and "platform" not in admin_piece.columns:
                admin_piece["platform"] = "Meta"
            admin_gcamp = acq_df(admin_report, "google_campaign_daily")
            if not admin_gcamp.empty:
                admin_gcamp["platform"] = "Google"
                if "cost" in admin_gcamp.columns:
                    admin_gcamp["spend"] = pd.to_numeric(admin_gcamp["cost"], errors="coerce").fillna(0.0)
            if admin_camp.empty and admin_gcamp.empty:
                admin_camp_all = pd.DataFrame()
            elif admin_camp.empty:
                admin_camp_all = admin_gcamp
            elif admin_gcamp.empty:
                admin_camp_all = admin_camp
            else:
                admin_camp_all = pd.concat([admin_camp, admin_gcamp], ignore_index=True)
        else:
            admin_df = pd.DataFrame()
            admin_camp_all = pd.DataFrame()
            admin_piece = pd.DataFrame()

        if admin_df.empty or "date" not in admin_df.columns:
            admin_s = date.today()
            admin_e = date.today()
            admin_df_sel = admin_df.copy()
            admin_df_prev = admin_df.copy()
        else:
            admin_s = admin_df["date"].min()
            admin_e = admin_df["date"].max()
            admin_df_sel = admin_df[(admin_df["date"] >= admin_s) & (admin_df["date"] <= admin_e)].copy()
            admin_period_days = max((admin_e - admin_s).days + 1, 1)
            admin_prev_e = admin_s - timedelta(days=1)
            admin_prev_s = admin_prev_e - timedelta(days=admin_period_days - 1)
            admin_df_prev = admin_df[(admin_df["date"] >= admin_prev_s) & (admin_df["date"] <= admin_prev_e)].copy()

        render_coco_ia_widget(
            df_base=admin_df,
            camp_df=admin_camp_all,
            piece_df=admin_piece,
            df_sel=admin_df_sel,
            df_prev=admin_df_prev,
            platform="All",
            s=admin_s,
            e=admin_e,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            auth_user=auth_user,
            coco_cfg=coco_cfg,
        )
        render_sidebar_logout_button()
        render_admin_panel(users, tenants, auth_user, dashboard_settings, admin_section)
        return

    report_path = Path(str(tenant_cfg.get("report_path", REPORT_PATH)))
    try:
        report_cache_sig = _report_cache_signature(report_path)
    except Exception as exc:
        st.error(f"No se pudo cargar el reporte para '{tenant_name}': {exc}")
        st.stop()
    try:
        df = load_daily_df_from_report_path(report_path)
    except Exception as exc:
        st.error(f"No se pudo cargar los datos diarios para '{tenant_name}': {exc}")
        st.stop()
    if df.empty:
        st.warning("No hay datos diarios en el JSON.")
        st.stop()

    min_d, max_d = df["date"].min(), df["date"].max()
    tenant_logo_source = _resolve_logo_image_source(
        tenant_dash_cfg.get("tenant_logo", tenant_cfg.get("logo", ""))
    )
    campaign_filter_keys = _normalize_campaign_filter_keys(
        tenant_dash_cfg.get("campaign_filters", DEFAULT_CAMPAIGN_FILTER_KEYS),
        DEFAULT_CAMPAIGN_FILTER_KEYS,
    )
    s, e, platform, campaign_filters, _compare_mode, prev_s, prev_e, _compare_label = render_top_filters(
        min_d,
        max_d,
        tenant_name,
        tenant_id,
        str(tenant_dash_cfg.get("default_platform", "All")),
        tenant_logo_source,
        pd.DataFrame(),
        campaign_filter_keys,
        report_cache_sig=report_cache_sig,
    )

    df_sel = df[(df["date"] >= s) & (df["date"] <= e)].copy()
    df_prev = df[(df["date"] >= prev_s) & (df["date"] <= prev_e)].copy()
    coco_enabled_for_tenant = _is_coco_enabled_for_tenant(coco_cfg, tenant_id)
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

    if view_mode == VIEW_MODE_OPTIONS[1]:
        traffic_section_set = set(traffic_sections)
        _needs_channels = "channels" in traffic_section_set
        _needs_top_pages = "top_pages" in traffic_section_set
        _needs_campaigns = "campaigns" in traffic_section_set
        needs_campaign_data = bool(coco_enabled_for_tenant)
        needs_piece_data = bool(coco_enabled_for_tenant)
        camp_all = load_campaign_unified_df_from_report_path(report_path) if needs_campaign_data else pd.DataFrame()
        piece = load_piece_enriched_df_from_report_path(report_path) if needs_piece_data else pd.DataFrame()
        ch = pd.DataFrame()
        pg = pd.DataFrame()
        if report_cache_sig is None:
            if _needs_channels:
                ch = load_acq_df_from_report_path(report_path, "ga4_channel_daily")
            if _needs_top_pages:
                pg = load_acq_df_from_report_path(report_path, "ga4_top_pages_daily")
        render_coco_ia_widget(
            df_base=df,
            camp_df=camp_all,
            piece_df=piece,
            df_sel=df_sel,
            df_prev=df_prev,
            platform=platform,
            s=s,
            e=e,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            auth_user=auth_user,
            coco_cfg=coco_cfg,
        )
        render_traffic(
            df_sel,
            df_prev,
            ch,
            pg,
            camp_all,
            platform,
            s,
            e,
            campaign_filters,
            _normalize_kpi_keys(
                tenant_dash_cfg.get("traffic_kpis", DEFAULT_TRAFFIC_KPI_KEYS),
                DEFAULT_TRAFFIC_KPI_KEYS,
            ),
            traffic_sections,
            report_cache_sig=report_cache_sig,
        )
    else:
        overview_section_set = set(overview_sections)
        has_report_cache = report_cache_sig is not None
        needs_top_pieces_source = bool("top_pieces" in overview_section_set and not has_report_cache)
        needs_trend_hourly = "trend_chart" in overview_section_set
        needs_piece_data = bool(coco_enabled_for_tenant or needs_top_pieces_source)
        needs_campaign_data = bool(coco_enabled_for_tenant or needs_top_pieces_source)
        needs_ga4_event = "ga4_conversion" in overview_section_set
        needs_paid_device = "device_breakdown" in overview_section_set
        needs_lead_demo = "lead_demographics" in overview_section_set
        needs_lead_geo = "lead_geo_map" in overview_section_set
        camp_all = load_campaign_unified_df_from_report_path(report_path) if needs_campaign_data else pd.DataFrame()
        piece = load_piece_enriched_df_from_report_path(report_path) if needs_piece_data else pd.DataFrame()
        ga4_event_daily = load_acq_df_from_report_path(report_path, "ga4_event_daily") if needs_ga4_event else pd.DataFrame()
        paid_dev = load_paid_device_df_from_report_path(report_path) if needs_paid_device else pd.DataFrame()
        lead_demo = load_paid_lead_demographics_df_from_report_path(report_path) if needs_lead_demo else pd.DataFrame()
        lead_geo = load_paid_lead_geo_df_from_report_path(report_path) if needs_lead_geo else pd.DataFrame()
        if needs_trend_hourly:
            hourly = load_hourly_df_from_report_path(report_path)
            if hourly.empty:
                hourly_sel = hourly.copy()
            else:
                hourly_sel = hourly[(hourly["date"] >= s) & (hourly["date"] <= e)].copy()
        else:
            hourly_sel = pd.DataFrame()
        render_coco_ia_widget(
            df_base=df,
            camp_df=camp_all,
            piece_df=piece,
            df_sel=df_sel,
            df_prev=df_prev,
            platform=platform,
            s=s,
            e=e,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            auth_user=auth_user,
            coco_cfg=coco_cfg,
        )
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
            lead_demo,
            lead_geo,
            camp_all,
            piece,
            ga4_event_daily,
            ga4_conversion_event_name,
            tenant_meta_account_id,
            tenant_google_customer_id,
            campaign_filters,
            s,
            e,
            prev_s,
            prev_e,
            f"overview_chart_metric_{tenant_id}",
            hourly_sel,
            report_cache_sig=report_cache_sig,
        )

    render_sidebar_logout_button()

    st.caption(
        f"Cliente: {tenant_name} ({tenant_id}) | Vista: {view_mode} | Plataforma: {platform} | "
        f"Fuente: {report_path.name} | Datos: {min_d.isoformat()} a {max_d.isoformat()}"
    )
if __name__ == "__main__":
    main()



