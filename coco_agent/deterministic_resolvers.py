from __future__ import annotations

import calendar
import math
import re
import unicodedata
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

COCO_DEFAULT_SCOPE_MODE = "total"
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

def pct_delta(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or prev == 0:
        return None
    return ((cur - prev) / abs(prev)) * 100.0

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

def _resolve_question_range(
    question: str,
    *,
    data_min: date,
    data_max: date,
    ui_start: date,
    ui_end: date,
) -> tuple[date, date, bool]:
    q = _normalize_question_text(question)
    start = ui_start
    end = ui_end
    explicit = False
    anchor_day = data_max if isinstance(data_max, date) else ui_end
    yesterday_anchor = anchor_day - timedelta(days=1)

    if not explicit:
        asks_last_year = _question_has_last_year_intent(q)
        asks_today = "hoy" in q
        if asks_last_year and asks_today:
            parsed_start = _safe_make_date(anchor_day.year - 1, 1, 1)
            if parsed_start is not None:
                start = parsed_start
                end = anchor_day
                explicit = True

    if not explicit:
        year_current_tokens = ("este ano", "este a?o", "este anio", "ano actual", "a?o actual", "anio actual")
        if any(token in q for token in year_current_tokens):
            parsed_start = _safe_make_date(anchor_day.year, 1, 1)
            if parsed_start is not None:
                start = parsed_start
                end = anchor_day
                explicit = True

    if not explicit:
        if re.search(r"\bayer\b", q):
            start = yesterday_anchor
            end = yesterday_anchor
            explicit = True
        elif re.search(r"\bhoy\b", q):
            start = anchor_day
            end = anchor_day
            explicit = True

    from_match = re.search(r"\bdesde\s+(?:el\s+)?(.+?)(?:\s+hasta\s+(?:el\s+)?(.+))?$", q)
    if from_match:
        parsed_start = _extract_first_date_token(str(from_match.group(1)))
        parsed_end = _extract_first_date_token(str(from_match.group(2))) if from_match.group(2) else None
        if parsed_start is not None:
            start = parsed_start
            end = parsed_end if parsed_end is not None else data_max
            explicit = True

    if not explicit:
        between_match = re.search(r"\bentre\s+(.+?)\s+y\s+(.+)$", q)
        if between_match:
            parsed_start = _extract_first_date_token(str(between_match.group(1)))
            parsed_end = _extract_first_date_token(str(between_match.group(2)))
            if parsed_start is not None and parsed_end is not None:
                start = parsed_start
                end = parsed_end
                explicit = True

    if not explicit:
        month_matches = list(
            re.finditer(
                r"\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)"
                r"(?:\s+de(?:l)?\s+(20\d{2}))?\b",
                q,
            )
        )
        if month_matches:
            month_name = str(month_matches[0].group(1)).strip().lower()
            month_value = SPANISH_MONTHS.get(month_name)
            year_token = str(month_matches[0].group(2) or "").strip()
            if month_value:
                if year_token:
                    year_value = int(year_token)
                else:
                    year_value = int(ui_end.year) if isinstance(ui_end, date) else int(data_max.year)
                month_range = _month_bounds(year_value, month_value)
                if month_range is not None:
                    start, end = month_range
                    explicit = True

    if not explicit:
        year_match = re.search(r"\b(?:en|del|durante|para|de)\s+(?:el\s+)?(20\d{2})\b", q)
        if year_match:
            year_value = int(year_match.group(1))
            parsed_start = _safe_make_date(year_value, 1, 1)
            parsed_end = _safe_make_date(year_value, 12, 31)
            if parsed_start is not None and parsed_end is not None:
                start = parsed_start
                end = parsed_end
                explicit = True

    if not explicit:
        standalone_years = [int(x) for x in re.findall(r"\b(20\d{2})\b", q)]
        unique_years = sorted(set(standalone_years))
        if len(unique_years) == 1:
            parsed_start = _safe_make_date(unique_years[0], 1, 1)
            parsed_end = _safe_make_date(unique_years[0], 12, 31)
            if parsed_start is not None and parsed_end is not None:
                start = parsed_start
                end = parsed_end
                explicit = True

    if not explicit:
        asks_last_year = _question_has_last_year_intent(q)
        if asks_last_year:
            parsed_start = _safe_make_date(anchor_day.year - 1, 1, 1)
            parsed_end = _safe_make_date(anchor_day.year - 1, 12, 31)
            if parsed_start is not None and parsed_end is not None:
                start = parsed_start
                end = parsed_end
                explicit = True

    if not explicit:
        start = _coerce_date_value(start, data_min, data_max)
        end = _coerce_date_value(end, data_min, data_max)
    if start > end:
        start, end = end, start
    return start, end, explicit

def _detect_peak_day_metric(question: str) -> str:
    q = _normalize_question_text(question)
    if not q:
        return ""
    if not any(token in q for token in ("dia", "fecha")):
        return ""
    has_superlative = any(token in q for token in ("mayor", "maximo", "maxima", "mas alto", "mas alta", "pico"))
    if not has_superlative and re.search(
        (
            r"\bmas\s+("
            r"conversion(?:es)?|conv|inversion|gasto|spend|costo|"
            r"impresion(?:es)?|impr|click(?:s)?|clic(?:s)?|"
            r"cvr|ctr|cpl|cpc|cpm|tasa\s+de\s+conversion|tasa\s+de\s+clic(?:s)?|"
            r"costo\s+por\s+lead|costo\s+por\s+clic|costo\s+por\s+mil"
            r")\b"
        ),
        q,
    ):
        has_superlative = True
    if not has_superlative and "mejor" in q:
        has_superlative = True
    if not has_superlative:
        return ""
    if any(token in q for token in ("cvr", "tasa de conversion", "ratio de conversion")):
        return "cvr"
    if any(token in q for token in ("ctr", "tasa de clic", "tasa de clics")):
        return "ctr"
    if any(token in q for token in ("cpl", "costo por lead", "coste por lead")):
        return "cpl"
    if any(token in q for token in ("cpc", "costo por clic", "coste por clic")):
        return "cpc"
    if any(token in q for token in ("cpm", "costo por mil", "coste por mil")):
        return "cpm"
    if any(token in q for token in ("conversion", "conv")):
        return "conv"
    if any(token in q for token in ("inversion", "gasto", "spend", "costo")):
        return "spend"
    if any(token in q for token in ("impresion", "impr")):
        return "impr"
    if any(token in q for token in ("click", "clic")):
        return "clicks"
    return ""

def _platform_from_question(question: str) -> str:
    q = _normalize_question_text(question)
    if not q:
        return ""
    if any(token in q for token in ("google", "google ads", "adwords", "gads")):
        return "Google"
    if any(token in q for token in ("meta", "facebook", "instagram", "fb")):
        return "Meta"
    return ""

def _weekday_name_es(day_value: date) -> str:
    return WEEKDAY_NAMES_ES.get(int(day_value.weekday()), "desconocido")

def _is_weekday_followup_question(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    if "dia de la semana" in q:
        return True
    if "weekday" in q:
        return True
    if re.search(r"\bque\s+dia\b", q) and any(token in q for token in ("era", "fue", "caia", "cayo")):
        return True
    return False

def _try_resolve_weekday_followup_question(
    *,
    question: str,
    last_context: dict[str, Any] | None,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_weekday_followup_question(question):
        return "", {}
    parsed_day = _extract_first_date_token(question)
    source = "pregunta"
    ctx = last_context if isinstance(last_context, dict) else {}
    if parsed_day is None:
        parsed_day = _parse_iso_date(ctx.get("peak_day"))
        source = "contexto_previo"
    if parsed_day is None:
        return "", {}

    weekday_name = _weekday_name_es(parsed_day)
    metric_key = str(ctx.get("metric_key", "")).strip()
    metric_label = str(KPI_CATALOG.get(metric_key, {}).get("label", metric_key)).strip() if metric_key else ""
    finding_lines = [f"{parsed_day.isoformat()} fue {weekday_name}."]
    if source == "contexto_previo" and metric_label:
        finding_lines.append(f"La fecha corresponde al último resultado consultado de {metric_label.lower()}.")
    answer = _format_coco_structured_answer(
        headline=f"La fecha {parsed_day.isoformat()} fue {weekday_name}.",
        findings=finding_lines,
        actions=(
            ["Si quieres, te doy el top 3 de días por ese mismo indicador."]
            if include_actions
            else []
        ),
    )
    meta = {
        "resolver": "deterministic_weekday",
        "target_date": parsed_day.isoformat(),
        "source": source,
    }
    return answer, meta

def _is_top_piece_period_followup_question(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    asks_period = bool(
        re.search(r"\ben\s+que\s+(per.?odo|rango|fecha|lapso)\b", q)
        or re.search(r"\b(per.?odo|rango|fecha|cuando)\b", q)
    )
    references_previous = any(
        token in q
        for token in (
            "esa ",
            "ese ",
            "esas ",
            "esos ",
            "esas conversion",
            "esa conversion",
            "esa pieza",
            "esa campana",
            "ese resultado",
            "pmax",
        )
    )
    return bool(asks_period and references_previous)

def _try_resolve_top_piece_period_followup_question(
    *,
    question: str,
    last_context: dict[str, Any] | None,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_top_piece_period_followup_question(question):
        return "", {}
    ctx = last_context if isinstance(last_context, dict) else {}
    if str(ctx.get("resolver", "")).strip().lower() != "deterministic_top_piece":
        return "", {}
    range_start = _parse_iso_date(ctx.get("range_start"))
    range_end = _parse_iso_date(ctx.get("range_end"))
    if range_start is None or range_end is None:
        return "", {}

    top_piece_name = str(ctx.get("top_piece_name", "")).strip()
    metric_key = str(ctx.get("metric_key", "")).strip()
    platforms = str(ctx.get("platforms", "")).strip()
    metric_label = _piece_metric_label(metric_key) if metric_key else "conversiones"
    subject_name = top_piece_name or "la pieza/campaña consultada"

    findings = [
        f"El cálculo se hizo para `{subject_name}` en el rango {range_start.isoformat()} a {range_end.isoformat()}.",
    ]
    if platforms:
        findings.append(f"Plataformas consideradas en ese resultado: {platforms}.")
    answer = _format_coco_structured_answer(
        headline=(
            f"Esas {metric_label} corresponden al periodo {range_start.isoformat()} a {range_end.isoformat()}."
        ),
        findings=findings,
        actions=(
            [
                "Si quieres, te desgloso ese periodo por mes o por plataforma para ver en qué tramo se concentraron.",
            ]
            if include_actions
            else []
        ),
    )
    meta = {
        "resolver": "deterministic_top_piece_followup_range",
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
        "top_piece_name": subject_name,
        "metric_key": metric_key,
        "source": "contexto_previo",
    }
    return answer, meta

def _extract_comparison_years(question: str) -> list[int]:
    q = _normalize_question_text(question)
    if not q:
        return []
    found = [int(y) for y in re.findall(r"\b(20\d{2})\b", q)]
    years: list[int] = []
    seen: set[int] = set()
    for year_value in found:
        if year_value in seen:
            continue
        years.append(year_value)
        seen.add(year_value)
    return years

def _extract_first_n_months(question: str) -> int | None:
    q = _normalize_question_text(question)
    if not q:
        return None
    if "primer trimestre" in q or "q1" in q:
        return 3
    if "primer semestre" in q:
        return 6
    if re.search(r"\benero\s*(?:y|-|/)\s*febrero\b", q):
        return 2
    word_to_num = {
        "uno": 1,
        "dos": 2,
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7,
        "ocho": 8,
        "nueve": 9,
        "diez": 10,
        "once": 11,
        "doce": 12,
    }
    month_count_match = re.search(
        r"\b(?:los\s+)?(?:primeros\s+)?(\d{1,2}|uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)\s+primeros?\s+mes(?:es)?\b",
        q,
    )
    if month_count_match is None:
        month_count_match = re.search(
            r"\bprimeros?\s+(\d{1,2}|uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)\s+mes(?:es)?\b",
            q,
        )
    if month_count_match is None:
        return None
    token = str(month_count_match.group(1)).strip().lower()
    if token.isdigit():
        value = int(token)
    else:
        value = int(word_to_num.get(token, 0))
    if value < 1 or value > 12:
        return None
    return value

def _extract_first_n_days(question: str) -> int | None:
    q = _normalize_question_text(question)
    if not q:
        return None
    word_to_num = {
        "uno": 1,
        "dos": 2,
        "tres": 3,
        "cuatro": 4,
        "cinco": 5,
        "seis": 6,
        "siete": 7,
        "ocho": 8,
        "nueve": 9,
        "diez": 10,
        "once": 11,
        "doce": 12,
        "trece": 13,
        "catorce": 14,
        "quince": 15,
        "dieciseis": 16,
        "diecisiete": 17,
        "dieciocho": 18,
        "diecinueve": 19,
        "veinte": 20,
        "veintiuno": 21,
        "veintidos": 22,
        "veintitres": 23,
        "veinticuatro": 24,
        "veinticinco": 25,
        "veintiseis": 26,
        "veintisiete": 27,
        "veintiocho": 28,
        "veintinueve": 29,
        "treinta": 30,
        "treinta y uno": 31,
    }
    day_match = re.search(
        r"\b(?:los\s+)?primeros?\s+(\d{1,2}|uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce|trece|catorce|quince|dieciseis|diecisiete|dieciocho|diecinueve|veinte|veintiuno|veintidos|veintitres|veinticuatro|veinticinco|veintiseis|veintisiete|veintiocho|veintinueve|treinta(?:\s+y\s+uno)?)\s+dias?\b",
        q,
    )
    if day_match is None:
        return None
    token = str(day_match.group(1)).strip().lower()
    if token.isdigit():
        value = int(token)
    else:
        value = int(word_to_num.get(token, 0))
    if value < 1 or value > 31:
        return None
    return value

def _extract_reference_month(question: str) -> int | None:
    q = _normalize_question_text(question)
    if not q:
        return None
    seen_months: list[int] = []
    for month_name, month_idx in SPANISH_MONTHS.items():
        if re.search(rf"\b{re.escape(month_name)}\b", q):
            seen_months.append(int(month_idx))
    if not seen_months:
        return None
    return int(seen_months[0])

def _is_month_day_window_comparison_question(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    compare_tokens = ("compara", "comparar", "comparativo", "vs", "versus", "contra")
    if not any(token in q for token in compare_tokens):
        return False
    years = _extract_comparison_years(q)
    if len(years) < 2:
        return False
    day_count = _extract_first_n_days(q)
    month_idx = _extract_reference_month(q)
    return bool(day_count is not None and month_idx is not None)

def _is_year_period_comparison_question(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    compare_tokens = ("compara", "comparar", "comparativo", "vs", "versus", "contra")
    if not any(token in q for token in compare_tokens):
        return False
    years = _extract_comparison_years(q)
    if len(years) < 2:
        return False
    return _extract_first_n_months(q) is not None

def _question_requests_table(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    table_terms = (
        "tabla",
        "tabular",
        "cuadro",
        "table",
    )
    return any(term in q for term in table_terms)

def _question_requests_consolidated_comparison(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    consolidated_terms = (
        "consolidado",
        "consolidada",
        "combinado",
        "combinada",
        "global",
        "sumado",
        "sumada",
        "all",
        "no por plataforma",
        "sin separar",
        "sin desglose",
        "sin desglosar",
        "juntas",
        "juntos",
    )
    if any(term in q for term in consolidated_terms):
        return True
    return bool("total" in q and any(token in q for token in ("plataforma", "canal")))

def _comparison_platforms_to_use(question: str, selected_platform: str) -> list[str]:
    asked_platform = _platform_from_question(question)
    both_platforms = _question_mentions_both_major_platforms(question)
    wants_consolidated = _question_requests_consolidated_comparison(question)
    if wants_consolidated:
        return ["All"]
    if both_platforms:
        return ["Meta", "Google"]
    if asked_platform in {"Meta", "Google"}:
        return [asked_platform]
    if selected_platform in {"Meta", "Google"}:
        return [selected_platform]
    return ["All"]

def _year_period_heading_suffix(month_count: int) -> str:
    if int(month_count) <= 1:
        return "Ene"
    end_month = MONTH_NAMES_ES.get(int(month_count), f"Mes {month_count}")
    end_tag = str(end_month).strip().capitalize()[:3]
    return f"Ene-{end_tag}"

def _build_year_period_comparison_table(
    *,
    base_year: int,
    target_year: int,
    month_count: int,
    base_metrics: dict[str, float | None],
    target_metrics: dict[str, float | None],
) -> str:
    period_tag = _year_period_heading_suffix(month_count)
    rows: list[tuple[str, str]] = [
        ("spend", "Gasto"),
        ("conv", "Conversiones"),
        ("clicks", "Clics"),
        ("impr", "Impresiones"),
        ("ctr", "CTR"),
        ("cvr", "CVR"),
        ("cpc", "CPC"),
    ]
    lines = [
        f"| Métrica | {base_year} ({period_tag}) | {target_year} ({period_tag}) | Variación (%) |",
        "|---|---:|---:|---:|",
    ]
    for metric_key, label in rows:
        fmt_key = str(KPI_CATALOG.get(metric_key, {}).get("fmt", "int")).strip()
        base_text = _format_kpi_value(fmt_key, base_metrics.get(metric_key))
        target_text = _format_kpi_value(fmt_key, target_metrics.get(metric_key))
        delta_text = fmt_delta_compact(
            pct_delta(target_metrics.get(metric_key), base_metrics.get(metric_key))
        )
        lines.append(f"| {label} | {base_text} | {target_text} | {delta_text} |")
    return "\n".join(lines)

def _month_short_label(month_idx: int) -> str:
    month_name = MONTH_NAMES_ES.get(int(month_idx), f"mes {month_idx}")
    clean = str(month_name).strip().capitalize()
    return clean[:3] if clean else str(month_idx)

def _build_month_day_window_comparison_table(
    *,
    base_year: int,
    target_year: int,
    month_idx: int,
    day_count: int,
    base_metrics: dict[str, float | None],
    target_metrics: dict[str, float | None],
) -> str:
    period_tag = f"{_month_short_label(month_idx)} 1-{int(day_count)}"
    rows: list[tuple[str, str]] = [
        ("spend", "Gasto"),
        ("conv", "Conversiones"),
        ("cpl", "CPL"),
        ("ctr", "CTR"),
        ("impr", "Impresiones"),
        ("clicks", "Clics"),
        ("cpc", "CPC"),
        ("cpm", "CPM"),
        ("cvr", "CVR"),
        ("sessions", "Sesiones"),
        ("users", "Usuarios"),
        ("avg_sess", "Duración Prom."),
        ("bounce", "Bounce"),
    ]
    lines = [
        f"| Métrica | {base_year} ({period_tag}) | {target_year} ({period_tag}) | Variación (%) |",
        "|---|---:|---:|---:|",
    ]
    for metric_key, label in rows:
        fmt_key = str(KPI_CATALOG.get(metric_key, {}).get("fmt", "int")).strip()
        base_text = _format_kpi_value(fmt_key, base_metrics.get(metric_key))
        target_text = _format_kpi_value(fmt_key, target_metrics.get(metric_key))
        delta_text = fmt_delta_compact(
            pct_delta(target_metrics.get(metric_key), base_metrics.get(metric_key))
        )
        lines.append(f"| {label} | {base_text} | {target_text} | {delta_text} |")
    return "\n".join(lines)

def _try_resolve_year_period_comparison_question(
    *,
    question: str,
    df_base: pd.DataFrame,
    selected_platform: str,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_year_period_comparison_question(question):
        return "", {}
    if df_base.empty or "date" not in df_base.columns:
        return "", {}

    years = _extract_comparison_years(question)
    if len(years) < 2:
        return "", {}
    base_year = int(years[0])
    target_year = int(years[1])
    month_count = _extract_first_n_months(question)
    if month_count is None:
        return "", {}
    month_count = max(1, min(int(month_count), 12))

    base_start = _safe_make_date(base_year, 1, 1)
    target_start = _safe_make_date(target_year, 1, 1)
    base_month_range = _month_bounds(base_year, month_count)
    target_month_range = _month_bounds(target_year, month_count)
    if (
        base_start is None
        or target_start is None
        or base_month_range is None
        or target_month_range is None
    ):
        return "", {}
    base_end = base_month_range[1]
    target_end = target_month_range[1]

    cp_base = df_base[(df_base["date"] >= base_start) & (df_base["date"] <= base_end)].copy()
    cp_target = df_base[(df_base["date"] >= target_start) & (df_base["date"] <= target_end)].copy()
    if cp_base.empty or cp_target.empty:
        data_min = df_base["date"].min()
        data_max = df_base["date"].max()
        missing_periods: list[str] = []
        if cp_base.empty:
            missing_periods.append(
                f"Sin datos para {base_year}: {base_start.isoformat()} a {base_end.isoformat()}."
            )
        if cp_target.empty:
            missing_periods.append(
                f"Sin datos para {target_year}: {target_start.isoformat()} a {target_end.isoformat()}."
            )
        if isinstance(data_min, date) and isinstance(data_max, date):
            missing_periods.append(f"Cobertura disponible del tenant: {data_min.isoformat()} a {data_max.isoformat()}.")
        answer = _format_coco_structured_answer(
            headline="No hay cobertura completa para comparar los periodos solicitados.",
            findings=missing_periods,
            actions=(["Ajusta los años o valida la cobertura histórica del tenant."] if include_actions else []),
        )
        meta = {
            "resolver": "deterministic_year_period_comparison",
            "metric_key": "multi_kpi",
            "range_start": base_start.isoformat(),
            "range_end": target_end.isoformat(),
            "applied_platform": selected_platform,
            "comparison_years": f"{base_year},{target_year}",
            "comparison_month_count": str(month_count),
        }
        return answer, meta

    platforms_to_use = _comparison_platforms_to_use(question, selected_platform)
    table_mode = _question_requests_table(question)

    period_label_end = MONTH_NAMES_ES.get(month_count, f"mes {month_count}")
    period_label = f"enero a {period_label_end}"
    findings: list[str] = []
    insight_lines: list[str] = []
    metric_fmt = {k: str(KPI_CATALOG.get(k, {}).get("fmt", "int")).strip() for k in KPI_CATALOG.keys()}
    table_sections: list[tuple[str, str]] = []

    for platform_name in platforms_to_use:
        base_metrics = summary(cp_base, platform_name)
        target_metrics = summary(cp_target, platform_name)
        prefix = f"{platform_name}: " if len(platforms_to_use) > 1 else ""
        if table_mode:
            table_sections.append(
                (
                    platform_name,
                    _build_year_period_comparison_table(
                        base_year=base_year,
                        target_year=target_year,
                        month_count=month_count,
                        base_metrics=base_metrics,
                        target_metrics=target_metrics,
                    ),
                )
            )

        findings.append(
            f"{prefix}{base_year}: Gasto {_format_kpi_value(metric_fmt['spend'], base_metrics.get('spend'))}, "
            f"Conversiones {_format_kpi_value(metric_fmt['conv'], base_metrics.get('conv'))}, "
            f"Clics {_format_kpi_value(metric_fmt['clicks'], base_metrics.get('clicks'))}, "
            f"Impresiones {_format_kpi_value(metric_fmt['impr'], base_metrics.get('impr'))}, "
            f"CTR {_format_kpi_value(metric_fmt['ctr'], base_metrics.get('ctr'))}, "
            f"CVR {_format_kpi_value(metric_fmt['cvr'], base_metrics.get('cvr'))}, "
            f"CPC {_format_kpi_value(metric_fmt['cpc'], base_metrics.get('cpc'))}."
        )
        findings.append(
            f"{prefix}{target_year}: Gasto {_format_kpi_value(metric_fmt['spend'], target_metrics.get('spend'))}, "
            f"Conversiones {_format_kpi_value(metric_fmt['conv'], target_metrics.get('conv'))}, "
            f"Clics {_format_kpi_value(metric_fmt['clicks'], target_metrics.get('clicks'))}, "
            f"Impresiones {_format_kpi_value(metric_fmt['impr'], target_metrics.get('impr'))}, "
            f"CTR {_format_kpi_value(metric_fmt['ctr'], target_metrics.get('ctr'))}, "
            f"CVR {_format_kpi_value(metric_fmt['cvr'], target_metrics.get('cvr'))}, "
            f"CPC {_format_kpi_value(metric_fmt['cpc'], target_metrics.get('cpc'))}."
        )

        d_spend = pct_delta(target_metrics.get("spend"), base_metrics.get("spend"))
        d_conv = pct_delta(target_metrics.get("conv"), base_metrics.get("conv"))
        d_clicks = pct_delta(target_metrics.get("clicks"), base_metrics.get("clicks"))
        d_ctr = pct_delta(target_metrics.get("ctr"), base_metrics.get("ctr"))
        d_cvr = pct_delta(target_metrics.get("cvr"), base_metrics.get("cvr"))
        d_cpc = pct_delta(target_metrics.get("cpc"), base_metrics.get("cpc"))
        d_cpl = pct_delta(target_metrics.get("cpl"), base_metrics.get("cpl"))

        findings.append(
            f"{prefix}Variación {target_year} vs {base_year}: "
            f"Gasto {fmt_delta_compact(d_spend)}, "
            f"Conversiones {fmt_delta_compact(d_conv)}, "
            f"Clics {fmt_delta_compact(d_clicks)}, "
            f"CTR {fmt_delta_compact(d_ctr)}, "
            f"CVR {fmt_delta_compact(d_cvr)}, "
            f"CPC {fmt_delta_compact(d_cpc)}."
        )

        if d_conv is not None:
            conv_trend = "crecieron" if d_conv >= 0 else "cayeron"
            insight_lines.append(
                f"{prefix}Conversión: las conversiones {conv_trend} {fmt_delta_compact(d_conv)} en {target_year} vs {base_year}."
            )
        if d_cpl is not None:
            if d_cpl < 0:
                insight_lines.append(
                    f"{prefix}Eficiencia: el CPL mejoró {abs(d_cpl):.1f}% (más bajo en {target_year})."
                )
            else:
                insight_lines.append(
                    f"{prefix}Eficiencia: el CPL empeoró {fmt_delta_compact(d_cpl)} (más alto en {target_year})."
                )
        if d_ctr is not None and d_cvr is not None:
            insight_lines.append(
                f"{prefix}Calidad de tráfico: CTR {fmt_delta_compact(d_ctr)} y CVR {fmt_delta_compact(d_cvr)}."
            )

    if table_mode and table_sections:
        headline = f"Comparativo {period_label} {target_year} vs {base_year}"
        if len(platforms_to_use) == 1 and platforms_to_use[0] == "All":
            headline += " (consolidado total)"
        elif len(platforms_to_use) == 1:
            headline += f" ({platforms_to_use[0]})"
        lines = [headline + ".", ""]
        for idx, (platform_name, table_md) in enumerate(table_sections):
            if len(table_sections) > 1:
                if idx > 0:
                    lines.append("")
                lines.append(f"**{platform_name}**")
                lines.append("")
            lines.append(table_md)
        if include_actions:
            lines.extend(
                [
                    "",
                    "Si quieres, también te lo desgloso por mes para este mismo corte.",
                ]
            )
        answer = "\n".join(lines).strip()
        meta = {
            "resolver": "deterministic_year_period_comparison",
            "metric_key": "multi_kpi",
            "range_start": base_start.isoformat(),
            "range_end": target_end.isoformat(),
            "applied_platform": ",".join(platforms_to_use),
            "comparison_years": f"{base_year},{target_year}",
            "comparison_month_count": str(month_count),
            "table_mode": "1",
        }
        return answer, meta

    findings.append(f"Rango comparado ({period_label}): {base_year} vs {target_year}.")
    findings.append(f"Plataforma aplicada: {', '.join(platforms_to_use)}.")
    if insight_lines:
        findings.append("Insights clave:")
        findings.extend(insight_lines[:3] if len(platforms_to_use) == 1 else insight_lines[:4])

    answer = _format_coco_structured_answer(
        headline=(
            f"Comparativo {period_label} {target_year} vs {base_year} con datos reales del tenant."
        ),
        findings=findings,
        actions=(
            [
                "Si quieres, te lo desgloso por mes para ver en cuál se concentra la variación.",
                "También puedo separar el comparativo por plataforma (Meta vs Google).",
            ]
            if include_actions
            else []
        ),
    )
    meta = {
        "resolver": "deterministic_year_period_comparison",
        "metric_key": "multi_kpi",
        "range_start": base_start.isoformat(),
        "range_end": target_end.isoformat(),
        "applied_platform": ",".join(platforms_to_use),
        "comparison_years": f"{base_year},{target_year}",
        "comparison_month_count": str(month_count),
        "table_mode": "0",
    }
    return answer, meta

def _is_year_period_comparison_followup_question(question: str) -> bool:
    if not _question_requests_table(question):
        return False
    q = _normalize_question_text(question)
    if not q:
        return False
    follow_terms = (
        "esos datos",
        "esos resultados",
        "esa comparacion",
        "ese comparativo",
        "pon",
        "muestra",
        "pasa",
        "tabla",
    )
    return any(term in q for term in follow_terms)

def _context_platform_for_comparison(value: Any, fallback: str) -> str:
    raw = str(value or "").strip().lower()
    if "all" in raw or ("meta" in raw and "google" in raw):
        return "All"
    if "meta" in raw and "google" not in raw:
        return "Meta"
    if "google" in raw and "meta" not in raw:
        return "Google"
    return fallback if fallback in {"All", "Meta", "Google"} else "All"

def _try_resolve_year_period_comparison_followup_question(
    *,
    question: str,
    last_context: dict[str, Any] | None,
    df_base: pd.DataFrame,
    selected_platform: str,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_year_period_comparison_followup_question(question):
        return "", {}
    ctx = last_context if isinstance(last_context, dict) else {}
    resolver_name = str(ctx.get("resolver", "")).strip().lower()
    if resolver_name not in {
        "deterministic_year_period_comparison",
        "deterministic_year_period_comparison_followup_table",
    }:
        return "", {}

    ctx_years = [int(y) for y in re.findall(r"\b(20\d{2})\b", str(ctx.get("comparison_years", "")))]
    if len(ctx_years) < 2:
        return "", {}
    base_year = int(ctx_years[0])
    target_year = int(ctx_years[1])
    try:
        month_count = int(str(ctx.get("comparison_month_count", "0")).strip() or "0")
    except Exception:
        month_count = 0
    if month_count < 1 or month_count > 12:
        return "", {}

    context_platform = _context_platform_for_comparison(
        ctx.get("applied_platform"),
        selected_platform,
    )
    if _question_requests_consolidated_comparison(question):
        effective_platform = "All"
    else:
        effective_platform = context_platform

    synthetic_question = (
        f"compara {base_year} vs {target_year} primeros {month_count} meses en tabla"
    )
    if effective_platform == "All":
        synthetic_question += " consolidado total no por plataforma"
    elif effective_platform in {"Meta", "Google"}:
        synthetic_question += f" de {effective_platform}"

    answer, meta = _try_resolve_year_period_comparison_question(
        question=synthetic_question,
        df_base=df_base,
        selected_platform=effective_platform,
        include_actions=include_actions,
    )
    if not answer:
        return "", {}
    meta["resolver"] = "deterministic_year_period_comparison_followup_table"
    meta["source"] = "contexto_previo"
    return answer, meta

def _try_resolve_month_day_window_comparison_question(
    *,
    question: str,
    df_base: pd.DataFrame,
    selected_platform: str,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_month_day_window_comparison_question(question):
        return "", {}
    if df_base.empty or "date" not in df_base.columns:
        return "", {}

    years = _extract_comparison_years(question)
    if len(years) < 2:
        return "", {}
    base_year = int(years[0])
    target_year = int(years[1])
    day_count = _extract_first_n_days(question)
    month_idx = _extract_reference_month(question)
    if day_count is None or month_idx is None:
        return "", {}
    day_count = max(1, min(int(day_count), 31))
    month_idx = max(1, min(int(month_idx), 12))

    base_month_bounds = _month_bounds(base_year, month_idx)
    target_month_bounds = _month_bounds(target_year, month_idx)
    if base_month_bounds is None or target_month_bounds is None:
        return "", {}
    base_start = base_month_bounds[0]
    target_start = target_month_bounds[0]
    base_end = _safe_make_date(base_year, month_idx, min(day_count, int(base_month_bounds[1].day)))
    target_end = _safe_make_date(target_year, month_idx, min(day_count, int(target_month_bounds[1].day)))
    if base_end is None or target_end is None:
        return "", {}

    cp_base = df_base[(df_base["date"] >= base_start) & (df_base["date"] <= base_end)].copy()
    cp_target = df_base[(df_base["date"] >= target_start) & (df_base["date"] <= target_end)].copy()
    if cp_base.empty or cp_target.empty:
        data_min = df_base["date"].min()
        data_max = df_base["date"].max()
        missing_periods: list[str] = []
        if cp_base.empty:
            missing_periods.append(
                f"Sin datos para {base_year}: {base_start.isoformat()} a {base_end.isoformat()}."
            )
        if cp_target.empty:
            missing_periods.append(
                f"Sin datos para {target_year}: {target_start.isoformat()} a {target_end.isoformat()}."
            )
        if isinstance(data_min, date) and isinstance(data_max, date):
            missing_periods.append(
                f"Cobertura disponible del tenant: {data_min.isoformat()} a {data_max.isoformat()}."
            )
        answer = _format_coco_structured_answer(
            headline="No hay cobertura completa para comparar los periodos solicitados.",
            findings=missing_periods,
            actions=(["Ajusta el mes, días o años solicitados."] if include_actions else []),
        )
        meta = {
            "resolver": "deterministic_month_day_window_comparison",
            "metric_key": "multi_kpi",
            "range_start": base_start.isoformat(),
            "range_end": target_end.isoformat(),
            "applied_platform": selected_platform,
            "comparison_years": f"{base_year},{target_year}",
            "comparison_month": str(month_idx),
            "comparison_day_count": str(day_count),
            "table_mode": "0",
        }
        return answer, meta

    month_name = MONTH_NAMES_ES.get(month_idx, f"mes {month_idx}")
    period_label = f"primeros {day_count} días de {month_name}"
    platforms_to_use = _comparison_platforms_to_use(question, selected_platform)
    table_mode = _question_requests_table(question)
    metric_fmt = {k: str(KPI_CATALOG.get(k, {}).get("fmt", "int")).strip() for k in KPI_CATALOG.keys()}

    findings: list[str] = []
    insight_lines: list[str] = []
    table_sections: list[tuple[str, str]] = []
    for platform_name in platforms_to_use:
        base_metrics = summary(cp_base, platform_name)
        target_metrics = summary(cp_target, platform_name)
        prefix = f"{platform_name}: " if len(platforms_to_use) > 1 else ""
        if table_mode:
            table_sections.append(
                (
                    platform_name,
                    _build_month_day_window_comparison_table(
                        base_year=base_year,
                        target_year=target_year,
                        month_idx=month_idx,
                        day_count=day_count,
                        base_metrics=base_metrics,
                        target_metrics=target_metrics,
                    ),
                )
            )

        findings.append(
            f"{prefix}{base_year}: Gasto {_format_kpi_value(metric_fmt['spend'], base_metrics.get('spend'))}, "
            f"Conversiones {_format_kpi_value(metric_fmt['conv'], base_metrics.get('conv'))}, "
            f"Clics {_format_kpi_value(metric_fmt['clicks'], base_metrics.get('clicks'))}, "
            f"Impresiones {_format_kpi_value(metric_fmt['impr'], base_metrics.get('impr'))}, "
            f"CTR {_format_kpi_value(metric_fmt['ctr'], base_metrics.get('ctr'))}, "
            f"CVR {_format_kpi_value(metric_fmt['cvr'], base_metrics.get('cvr'))}, "
            f"CPC {_format_kpi_value(metric_fmt['cpc'], base_metrics.get('cpc'))}."
        )
        findings.append(
            f"{prefix}{target_year}: Gasto {_format_kpi_value(metric_fmt['spend'], target_metrics.get('spend'))}, "
            f"Conversiones {_format_kpi_value(metric_fmt['conv'], target_metrics.get('conv'))}, "
            f"Clics {_format_kpi_value(metric_fmt['clicks'], target_metrics.get('clicks'))}, "
            f"Impresiones {_format_kpi_value(metric_fmt['impr'], target_metrics.get('impr'))}, "
            f"CTR {_format_kpi_value(metric_fmt['ctr'], target_metrics.get('ctr'))}, "
            f"CVR {_format_kpi_value(metric_fmt['cvr'], target_metrics.get('cvr'))}, "
            f"CPC {_format_kpi_value(metric_fmt['cpc'], target_metrics.get('cpc'))}."
        )

        d_spend = pct_delta(target_metrics.get("spend"), base_metrics.get("spend"))
        d_conv = pct_delta(target_metrics.get("conv"), base_metrics.get("conv"))
        d_clicks = pct_delta(target_metrics.get("clicks"), base_metrics.get("clicks"))
        d_ctr = pct_delta(target_metrics.get("ctr"), base_metrics.get("ctr"))
        d_cvr = pct_delta(target_metrics.get("cvr"), base_metrics.get("cvr"))
        d_cpc = pct_delta(target_metrics.get("cpc"), base_metrics.get("cpc"))
        d_cpl = pct_delta(target_metrics.get("cpl"), base_metrics.get("cpl"))

        findings.append(
            f"{prefix}Variación {target_year} vs {base_year}: "
            f"Gasto {fmt_delta_compact(d_spend)}, "
            f"Conversiones {fmt_delta_compact(d_conv)}, "
            f"Clics {fmt_delta_compact(d_clicks)}, "
            f"CTR {fmt_delta_compact(d_ctr)}, "
            f"CVR {fmt_delta_compact(d_cvr)}, "
            f"CPC {fmt_delta_compact(d_cpc)}."
        )
        if d_cpl is not None:
            if d_cpl < 0:
                insight_lines.append(
                    f"{prefix}Eficiencia: el CPL mejoró {abs(d_cpl):.1f}% en {target_year}."
                )
            else:
                insight_lines.append(
                    f"{prefix}Eficiencia: el CPL empeoró {fmt_delta_compact(d_cpl)} en {target_year}."
                )

    if table_mode and table_sections:
        headline = f"Comparativo {period_label} {target_year} vs {base_year}"
        if len(platforms_to_use) == 1 and platforms_to_use[0] == "All":
            headline += " (consolidado total)"
        elif len(platforms_to_use) == 1:
            headline += f" ({platforms_to_use[0]})"
        lines = [headline + ".", ""]
        for idx, (platform_name, table_md) in enumerate(table_sections):
            if len(table_sections) > 1:
                if idx > 0:
                    lines.append("")
                lines.append(f"**{platform_name}**")
                lines.append("")
            lines.append(table_md)
        answer = "\n".join(lines).strip()
        meta = {
            "resolver": "deterministic_month_day_window_comparison",
            "metric_key": "multi_kpi",
            "range_start": base_start.isoformat(),
            "range_end": target_end.isoformat(),
            "applied_platform": ",".join(platforms_to_use),
            "comparison_years": f"{base_year},{target_year}",
            "comparison_month": str(month_idx),
            "comparison_day_count": str(day_count),
            "table_mode": "1",
        }
        return answer, meta

    findings.append(f"Rango comparado ({period_label}): {base_year} vs {target_year}.")
    findings.append(f"Plataforma aplicada: {', '.join(platforms_to_use)}.")
    if insight_lines:
        findings.append("Insights clave:")
        findings.extend(insight_lines[:3] if len(platforms_to_use) == 1 else insight_lines[:4])
    answer = _format_coco_structured_answer(
        headline=f"Comparativo {period_label} {target_year} vs {base_year} con datos reales del tenant.",
        findings=findings,
        actions=(
            ["Si quieres, también te lo pongo en tabla o lo separo por plataforma."]
            if include_actions
            else []
        ),
    )
    meta = {
        "resolver": "deterministic_month_day_window_comparison",
        "metric_key": "multi_kpi",
        "range_start": base_start.isoformat(),
        "range_end": target_end.isoformat(),
        "applied_platform": ",".join(platforms_to_use),
        "comparison_years": f"{base_year},{target_year}",
        "comparison_month": str(month_idx),
        "comparison_day_count": str(day_count),
        "table_mode": "0",
    }
    return answer, meta

def _is_month_day_window_comparison_followup_question(question: str) -> bool:
    if not _question_requests_table(question):
        return False
    q = _normalize_question_text(question)
    if not q:
        return False
    follow_terms = (
        "esos datos",
        "esos resultados",
        "esa comparacion",
        "ese comparativo",
        "pon",
        "muestra",
        "pasa",
        "tabla",
    )
    return any(term in q for term in follow_terms)

def _try_resolve_month_day_window_comparison_followup_question(
    *,
    question: str,
    last_context: dict[str, Any] | None,
    df_base: pd.DataFrame,
    selected_platform: str,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_month_day_window_comparison_followup_question(question):
        return "", {}
    ctx = last_context if isinstance(last_context, dict) else {}
    resolver_name = str(ctx.get("resolver", "")).strip().lower()
    if resolver_name not in {
        "deterministic_month_day_window_comparison",
        "deterministic_month_day_window_comparison_followup_table",
    }:
        return "", {}

    ctx_years = [int(y) for y in re.findall(r"\b(20\d{2})\b", str(ctx.get("comparison_years", "")))]
    if len(ctx_years) < 2:
        return "", {}
    base_year = int(ctx_years[0])
    target_year = int(ctx_years[1])
    try:
        month_idx = int(str(ctx.get("comparison_month", "0")).strip() or "0")
        day_count = int(str(ctx.get("comparison_day_count", "0")).strip() or "0")
    except Exception:
        return "", {}
    if month_idx < 1 or month_idx > 12 or day_count < 1 or day_count > 31:
        return "", {}

    context_platform = _context_platform_for_comparison(
        ctx.get("applied_platform"),
        selected_platform,
    )
    if _question_requests_consolidated_comparison(question):
        effective_platform = "All"
    else:
        effective_platform = context_platform
    month_name = MONTH_NAMES_ES.get(month_idx, str(month_idx))
    synthetic_question = (
        f"compara primeros {day_count} dias de {month_name} {base_year} vs {target_year} en tabla"
    )
    if effective_platform == "All":
        synthetic_question += " consolidado total no por plataforma"
    elif effective_platform in {"Meta", "Google"}:
        synthetic_question += f" de {effective_platform}"

    answer, meta = _try_resolve_month_day_window_comparison_question(
        question=synthetic_question,
        df_base=df_base,
        selected_platform=effective_platform,
        include_actions=include_actions,
    )
    if not answer:
        return "", {}
    meta["resolver"] = "deterministic_month_day_window_comparison_followup_table"
    meta["source"] = "contexto_previo"
    return answer, meta

def _is_monthly_breakdown_question(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    monthly_terms = (
        "por mes",
        "mes a mes",
        "mensual",
        "cada mes",
    )
    return any(term in q for term in monthly_terms)

def _detect_monthly_metric(question: str) -> str:
    q = _normalize_question_text(question)
    if not q:
        return ""
    if any(token in q for token in ("conversion", "conv", "lead", "leads")):
        return "conv"
    if any(token in q for token in ("inversion", "gasto", "spend", "costo", "coste")):
        return "spend"
    if any(token in q for token in ("click", "clic")):
        return "clicks"
    if any(token in q for token in ("impresion", "impr")):
        return "impr"
    if "cpl" in q:
        return "cpl"
    if "cpc" in q:
        return "cpc"
    if "cpm" in q:
        return "cpm"
    if "ctr" in q:
        return "ctr"
    if "cvr" in q:
        return "cvr"
    if any(token in q for token in ("sesion", "session")):
        return "sessions"
    if any(token in q for token in ("usuario", "users")):
        return "users"
    if "rebote" in q:
        return "bounce"
    if "duracion" in q or "tiempo" in q:
        return "avg_sess"
    return "spend" if _is_monthly_breakdown_question(question) else ""

def _iter_month_windows(start_day: date, end_day: date) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    current = date(start_day.year, start_day.month, 1)
    while current <= end_day:
        month_last = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, month_last)
        win_start = current if current >= start_day else start_day
        win_end = month_end if month_end <= end_day else end_day
        windows.append((win_start, win_end))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return windows

def _monthly_kpi_value(part: pd.DataFrame, *, platform_prefix: str, kpi_key: str) -> float | None:
    spend = float(_series_num(part, f"{platform_prefix}_spend").sum())
    clicks = float(_series_num(part, f"{platform_prefix}_clicks").sum())
    conv = float(_series_num(part, f"{platform_prefix}_conv").sum())
    impr = float(_series_num(part, f"{platform_prefix}_impr").sum())
    sessions = float(_series_num(part, "ga4_sessions").sum())
    users = float(_series_num(part, "ga4_users").sum())
    if kpi_key == "spend":
        return spend
    if kpi_key == "conv":
        return conv
    if kpi_key == "clicks":
        return clicks
    if kpi_key == "impr":
        return impr
    if kpi_key == "sessions":
        return sessions
    if kpi_key == "users":
        return users
    if kpi_key == "cpl":
        return sdiv(spend, conv)
    if kpi_key == "cpc":
        return sdiv(spend, clicks)
    if kpi_key == "cpm":
        return (spend * 1000.0 / impr) if impr > 0 else None
    if kpi_key == "ctr":
        return sdiv(clicks, impr)
    if kpi_key == "cvr":
        return sdiv(conv, clicks)
    if kpi_key == "avg_sess":
        if sessions <= 0:
            return None
        weighted = float((_series_num(part, "ga4_avg_sess") * _series_num(part, "ga4_sessions")).sum())
        return weighted / sessions
    if kpi_key == "bounce":
        if part.empty:
            return None
        bounce_series = _series_num(part, "ga4_bounce")
        return float(bounce_series.mean()) if not bounce_series.empty else None
    return None

def _try_resolve_monthly_breakdown_question(
    *,
    question: str,
    df_base: pd.DataFrame,
    selected_platform: str,
    ui_start: date,
    ui_end: date,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_monthly_breakdown_question(question):
        return "", {}
    if df_base.empty or "date" not in df_base.columns:
        return "", {}

    metric_key = _detect_monthly_metric(question)
    if not metric_key:
        return "", {}

    data_min = df_base["date"].min()
    data_max = df_base["date"].max()
    if not isinstance(data_min, date) or not isinstance(data_max, date):
        return "", {}

    range_start, range_end, explicit_range = _resolve_question_range(
        question,
        data_min=data_min,
        data_max=data_max,
        ui_start=ui_start,
        ui_end=ui_end,
    )
    cp = df_base[(df_base["date"] >= range_start) & (df_base["date"] <= range_end)].copy()
    if cp.empty:
        answer = _format_coco_structured_answer(
            headline="No encontré datos para ese rango de fechas.",
            findings=[f"Rango aplicado: {range_start.isoformat()} a {range_end.isoformat()}."],
            actions=(
                ["Ajusta el rango o valida que exista histórico para ese periodo."]
                if include_actions
                else []
            ),
        )
        meta = {
            "resolver": "deterministic_monthly_breakdown",
            "metric_key": metric_key,
            "range_start": range_start.isoformat(),
            "range_end": range_end.isoformat(),
            "range_source": "pregunta" if explicit_range else "selector",
            "applied_platform": selected_platform,
        }
        return answer, meta

    asked_platform = _platform_from_question(question)
    both_platforms = _question_mentions_both_major_platforms(question)
    if both_platforms:
        platforms_to_use = ["Meta", "Google"]
    elif asked_platform in {"Meta", "Google"}:
        platforms_to_use = [asked_platform]
    elif selected_platform in {"Meta", "Google"}:
        platforms_to_use = [selected_platform]
    else:
        platforms_to_use = ["All"]
    platform_prefix_map = {"All": "total", "Meta": "meta", "Google": "google"}

    fmt_key = str(KPI_CATALOG.get(metric_key, {}).get("fmt", "int")).strip()
    metric_label = str(KPI_CATALOG.get(metric_key, {}).get("label", metric_key)).strip()
    additive_metrics = {"spend", "conv", "clicks", "impr", "sessions", "users"}
    month_windows = _iter_month_windows(range_start, range_end)
    month_lines: list[str] = []
    for month_start, month_end in month_windows:
        month_part = cp[(cp["date"] >= month_start) & (cp["date"] <= month_end)].copy()
        month_name = MONTH_NAMES_ES.get(month_start.month, month_start.strftime("%B").lower()).capitalize()
        month_tag = f"{month_name} {month_start.year}" if range_start.year != range_end.year else month_name
        if len(platforms_to_use) == 2:
            meta_value = _monthly_kpi_value(month_part, platform_prefix="meta", kpi_key=metric_key)
            google_value = _monthly_kpi_value(month_part, platform_prefix="google", kpi_key=metric_key)
            total_value = _monthly_kpi_value(month_part, platform_prefix="total", kpi_key=metric_key)
            if metric_key in additive_metrics:
                if meta_value is None:
                    meta_value = 0.0
                if google_value is None:
                    google_value = 0.0
                if total_value is None:
                    total_value = 0.0
            meta_text = _format_kpi_value(fmt_key, meta_value if meta_value is not None else None)
            google_text = _format_kpi_value(fmt_key, google_value if google_value is not None else None)
            total_text = _format_kpi_value(fmt_key, total_value if total_value is not None else None)
            month_lines.append(f"{month_tag}: Meta {meta_text} | Google {google_text} | Total {total_text}")
            continue

        applied_platform = platforms_to_use[0]
        platform_prefix = platform_prefix_map[applied_platform]
        month_value = _monthly_kpi_value(month_part, platform_prefix=platform_prefix, kpi_key=metric_key)
        if month_value is None and metric_key in additive_metrics:
            month_value = 0.0
        month_text = _format_kpi_value(fmt_key, month_value if month_value is not None else None)
        month_lines.append(f"{month_tag}: {month_text}")

    applied_platform = ", ".join(platforms_to_use)
    full_year_mode = (
        range_start.year == range_end.year
        and range_start.month == 1
        and range_start.day == 1
        and range_end.month == 12
        and range_end.day == 31
    )
    if full_year_mode:
        headline = f"Aquí tienes {metric_label.lower()} por mes en el año {range_start.year}."
    else:
        headline = (
            f"Aquí tienes {metric_label.lower()} por mes para el periodo "
            f"{range_start.isoformat()} a {range_end.isoformat()}."
        )

    findings = list(month_lines)
    if len(platforms_to_use) == 2:
        total_meta = _monthly_kpi_value(cp, platform_prefix="meta", kpi_key=metric_key)
        total_google = _monthly_kpi_value(cp, platform_prefix="google", kpi_key=metric_key)
        total_all = _monthly_kpi_value(cp, platform_prefix="total", kpi_key=metric_key)
        if metric_key in additive_metrics:
            if total_meta is None:
                total_meta = 0.0
            if total_google is None:
                total_google = 0.0
            if total_all is None:
                total_all = 0.0
        findings.append(f"Total Meta: {_format_kpi_value(fmt_key, total_meta if total_meta is not None else None)}.")
        findings.append(f"Total Google: {_format_kpi_value(fmt_key, total_google if total_google is not None else None)}.")
        findings.append(f"Total combinado: {_format_kpi_value(fmt_key, total_all if total_all is not None else None)}.")
        findings.append("Plataformas aplicadas: Meta, Google.")
    else:
        total_value = _monthly_kpi_value(
            cp,
            platform_prefix=platform_prefix_map[platforms_to_use[0]],
            kpi_key=metric_key,
        )
        total_text = _format_kpi_value(fmt_key, total_value if total_value is not None else None)
        findings.append(f"Total del periodo: {total_text}.")
        findings.append(f"Plataforma aplicada: {applied_platform}.")
    answer = _format_coco_structured_answer(
        headline=headline,
        findings=findings,
        actions=(
            [
                "Si quieres, también te lo comparo mes contra mes vs el año anterior.",
            ]
            if include_actions
            else []
        ),
    )
    meta = {
        "resolver": "deterministic_monthly_breakdown",
        "metric_key": metric_key,
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
        "range_source": "pregunta" if explicit_range else "selector",
        "applied_platform": applied_platform,
        "platforms": applied_platform,
    }
    return answer, meta

def _try_resolve_peak_day_question(
    *,
    question: str,
    df_base: pd.DataFrame,
    selected_platform: str,
    ui_start: date,
    ui_end: date,
    include_actions: bool = True,
    include_platform_breakdown: bool = False,
) -> tuple[str, dict[str, str]]:
    metric_key = _detect_peak_day_metric(question)
    if not metric_key:
        return "", {}
    if df_base.empty or "date" not in df_base.columns:
        return "", {}

    data_min = df_base["date"].min()
    data_max = df_base["date"].max()
    if not isinstance(data_min, date) or not isinstance(data_max, date):
        return "", {}

    range_start, range_end, explicit_range = _resolve_question_range(
        question,
        data_min=data_min,
        data_max=data_max,
        ui_start=ui_start,
        ui_end=ui_end,
    )
    cp = df_base[(df_base["date"] >= range_start) & (df_base["date"] <= range_end)].copy().reset_index(drop=True)
    if cp.empty:
        answer = _format_coco_structured_answer(
            headline="No encontré datos para ese rango de fechas.",
            findings=[
                f"Rango aplicado: {range_start.isoformat()} a {range_end.isoformat()}."
            ],
            actions=(
                [
                    "Ajusta el rango de fechas o valida que el tenant tenga histórico en ese periodo.",
                ]
                if include_actions
                else []
            ),
        )
        meta = {
            "range_start": range_start.isoformat(),
            "range_end": range_end.isoformat(),
            "metric_key": metric_key,
            "resolver": "deterministic_peak",
            "range_source": "pregunta" if explicit_range else "selector",
        }
        return answer, meta

    label = str(KPI_CATALOG.get(metric_key, {}).get("label", metric_key)).strip()
    fmt_key = str(KPI_CATALOG.get(metric_key, {}).get("fmt", "int")).strip()
    asked_platform = _platform_from_question(question)
    effective_platform = asked_platform if asked_platform in {"Google", "Meta"} else selected_platform
    tie_note = ""
    applied_platform = effective_platform
    breakdown_line = ""
    total_series = _platform_kpi_series(cp, "total", metric_key)
    meta_series = _platform_kpi_series(cp, "meta", metric_key)
    google_series = _platform_kpi_series(cp, "google", metric_key)
    if total_series is None or meta_series is None or google_series is None:
        return "", {}

    total_series = pd.to_numeric(total_series, errors="coerce").fillna(0.0)
    meta_series = pd.to_numeric(meta_series, errors="coerce").fillna(0.0)
    google_series = pd.to_numeric(google_series, errors="coerce").fillna(0.0)

    def _peak_rows_for(series: pd.Series) -> pd.DataFrame:
        if series.empty:
            return cp.iloc[0:0].copy()
        peak_val = float(series.max())
        if not math.isfinite(peak_val):
            return cp.iloc[0:0].copy()
        series_num = pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)
        tolerance = 1e-12 + (1e-9 * abs(peak_val))
        peak_mask = (series_num - peak_val).abs() <= tolerance
        return cp.loc[peak_mask].sort_values("date")

    if effective_platform in {"Meta", "Google"}:
        selected_series = meta_series if effective_platform == "Meta" else google_series
        peak_rows = _peak_rows_for(selected_series)
        if peak_rows.empty:
            return "", {}
        peak_row = peak_rows.iloc[0]
        peak_idx = int(peak_row.name)
        peak_day = peak_row["date"]
        tie_days = int(len(peak_rows))
        if tie_days > 1:
            tie_note = f"Hubo empate en {tie_days} días; reporto el primero cronológico."
        platform_value = float(selected_series.iloc[peak_idx])
        total_value = platform_value
        platform_label = effective_platform
        if include_platform_breakdown:
            counterpart = "Google" if effective_platform == "Meta" else "Meta"
            counterpart_series = google_series if counterpart == "Google" else meta_series
            counterpart_value = float(counterpart_series.iloc[peak_idx])
            breakdown_line = (
                f"Desglose por plataforma ese día: {effective_platform} ({_format_kpi_value(fmt_key, platform_value)}) "
                f"| {counterpart} ({_format_kpi_value(fmt_key, counterpart_value)})."
            )
    else:
        peak_rows = _peak_rows_for(total_series)
        if peak_rows.empty:
            return "", {}
        peak_row = peak_rows.iloc[0]
        peak_idx = int(peak_row.name)
        peak_day = peak_row["date"]
        tie_days = int(len(peak_rows))
        if tie_days > 1:
            tie_note = f"Hubo empate en {tie_days} días; reporto el primero cronológico."
        total_value = float(total_series.iloc[peak_idx])
        meta_value = float(meta_series.iloc[peak_idx])
        google_value = float(google_series.iloc[peak_idx])
        if meta_value > google_value:
            platform_label = "Meta"
            platform_value = meta_value
        elif google_value > meta_value:
            platform_label = "Google"
            platform_value = google_value
        else:
            platform_label = "Empate Meta/Google"
            platform_value = meta_value
        applied_platform = "All"
        if include_platform_breakdown:
            breakdown_line = (
                f"Desglose por plataforma ese día: Meta ({_format_kpi_value(fmt_key, meta_value)}) "
                f"| Google ({_format_kpi_value(fmt_key, google_value)})."
            )

    peak_total_txt = _format_kpi_value(fmt_key, total_value)
    platform_value_txt = _format_kpi_value(fmt_key, platform_value)
    findings_lines = [
        f"Plataforma líder ese día: {platform_label} ({platform_value_txt}).",
        breakdown_line,
        f"Rango aplicado: {range_start.isoformat()} a {range_end.isoformat()}.",
        tie_note,
    ]
    answer = _format_coco_structured_answer(
        headline=f"Día pico de {label}: {peak_day.isoformat()} ({peak_total_txt}).",
        findings=findings_lines,
        actions=(
            [
                f"Revisa creativos y campañas activas en {platform_label} durante {peak_day.isoformat()}.",
                "Compara ese pico contra el promedio semanal para validar sostenibilidad.",
            ]
            if include_actions
            else []
        ),
    )
    meta = {
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
        "metric_key": metric_key,
        "resolver": "deterministic_peak",
        "range_source": "pregunta" if explicit_range else "selector",
        "applied_platform": applied_platform,
        "peak_day": peak_day.isoformat(),
        "leader_platform": platform_label,
    }
    return answer, meta

def _coco_scope_guard_message(question: str) -> str:
    q = str(question or "").strip().lower()
    if not q:
        return "Escribe una pregunta para continuar."
    if len(q) > 800:
        return "La pregunta es muy larga. Intenta con un máximo de 800 caracteres."
    restricted_terms = [
        "password",
        "contrase",
        "api key",
        "secret",
        "token de acceso",
        "access token",
        "users.json",
        "dashboard_settings.json",
        "config/",
        "credencial",
    ]
    for term in restricted_terms:
        if term in q:
            return (
                "COCO IA solo responde sobre métricas analíticas agregadas del tenant "
                "(por defecto en alcance total/histórico o en el filtro si se solicita explícitamente). "
                "No puede exponer configuración sensible o credenciales."
            )
    return ""

def _question_requests_actions(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    action_terms = (
        "recomienda",
        "recomendacion",
        "recomendaciones",
        "accion",
        "acciones",
        "sugerencia",
        "sugerencias",
        "sugerir",
        "que hago",
        "que hacemos",
        "como mejorar",
        "como optimizar",
        "plan",
        "pasos",
        "prioriza",
        "estrategia",
        "next step",
        "siguiente paso",
    )
    return any(term in q for term in action_terms)

def _question_requests_platform_breakdown(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    breakdown_terms = (
        "desglose",
        "desglosa",
        "desglosalas",
        "desglosalo",
        "desglosar",
        "por plataforma",
        "por canal",
        "breakdown",
    )
    return any(term in q for term in breakdown_terms)

def _question_requests_total_scope(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    if _question_has_last_year_intent(q):
        return True
    year_current_tokens = ("este ano", "este a?o", "este anio", "ano actual", "a?o actual", "anio actual")
    if any(token in q for token in year_current_tokens):
        return True
    if re.search(r"\b20\d{2}\b", q):
        return True
    total_terms = (
        "total",
        "totales",
        "historico",
        "historica",
        "acumulado",
        "acumulada",
        "global",
        "todo el periodo",
        "todo el rango",
        "todos los datos",
        "sin filtro",
        "desde el inicio",
        "desde que inicio",
    )
    return any(term in q for term in total_terms)

def _question_requests_filter_scope(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    filter_terms = (
        "filtro",
        "filtros",
        "rango activo",
        "periodo activo",
        "periodo seleccionado",
        "seleccion actual",
        "este rango",
        "estas fechas",
        "hoy",
        "ayer",
        "ultimos 7 dias",
        "ultimos 30 dias",
        "mes actual",
    )
    return any(term in q for term in filter_terms)

def _resolve_coco_scope_mode(
    question: str,
    *,
    last_context: dict[str, Any] | None = None,
    default_mode: str = COCO_DEFAULT_SCOPE_MODE,
) -> tuple[str, str]:
    preferred = str(default_mode).strip().lower()
    if preferred not in {"total", "filter"}:
        preferred = COCO_DEFAULT_SCOPE_MODE
    ctx = last_context if isinstance(last_context, dict) else {}
    last_scope = str(ctx.get("preferred_scope", "")).strip().lower()
    if _question_requests_total_scope(question):
        return "total", "explicit_question"
    if _question_requests_filter_scope(question):
        return "filter", "explicit_question"
    if last_scope in {"total", "filter"}:
        return last_scope, "conversation_memory"
    return preferred, "default"

def _question_mentions_both_major_platforms(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    mentions_google = "google" in q
    mentions_meta = any(token in q for token in ("meta", "facebook", "instagram", "fb"))
    return bool(mentions_google and mentions_meta)

def _detect_piece_metric(question: str) -> str:
    q = _normalize_question_text(question)
    if not q:
        return ""
    if any(token in q for token in ("conversion", "conv")):
        return "conversions"
    if any(token in q for token in ("resultado", "resultados", "rendimiento", "performance", "desempeno")):
        return "conversions"
    if any(token in q for token in ("inversion", "gasto", "spend", "costo")):
        return "spend"
    if any(token in q for token in ("impresion", "impr")):
        return "impressions"
    if any(token in q for token in ("click", "clic")):
        return "clicks"
    return ""

def _is_piece_top_question(question: str) -> bool:
    q = _normalize_question_text(question)
    if not q:
        return False
    mentions_piece = any(token in q for token in ("pieza", "piezas", "campana", "campanas", "anuncio", "creative"))
    mentions_rank = any(token in q for token in ("mayor", "maximo", "maxima", "mas", "top", "mejor"))
    return bool(mentions_piece and mentions_rank and _detect_piece_metric(q))

def _piece_metric_label(metric_key: str) -> str:
    mapping = {
        "conversions": "conversiones",
        "spend": "inversión",
        "impressions": "impresiones",
        "clicks": "clics",
    }
    return mapping.get(metric_key, metric_key)

def _piece_metric_fmt(metric_key: str, value: float | None) -> str:
    if metric_key == "spend":
        return fmt_money(value)
    if metric_key == "impressions":
        return fmt_compact(value)
    if metric_key == "clicks":
        return f"{int(round(sf(value))):,}" if value is not None else "N/A"
    return f"{int(round(sf(value))):,}" if value is not None else "N/A"

def _top_piece_for_platform(
    cp: pd.DataFrame,
    *,
    platform_name: str,
    metric_key: str,
    id_col: str = "campaign_id",
    name_col: str = "campaign_name",
) -> dict[str, Any] | None:
    if cp.empty:
        return None
    part = cp[cp["platform"] == platform_name].copy()
    if part.empty:
        return None
    agg = (
        part.groupby([id_col, name_col], as_index=False)
        .agg(
            conversions=("conversions", "sum"),
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
        )
        .sort_values([metric_key, "conversions", "clicks"], ascending=[False, False, False], na_position="last")
    )
    if agg.empty:
        return None
    row = agg.iloc[0]
    return {
        "piece_id": str(row.get(id_col, "")).strip(),
        "piece_name": str(row.get(name_col, "")).strip() or "Sin nombre",
        "metric_value": float(row.get(metric_key, 0.0)),
        "conversions": float(row.get("conversions", 0.0)),
        "spend": float(row.get("spend", 0.0)),
        "impressions": float(row.get("impressions", 0.0)),
        "clicks": float(row.get("clicks", 0.0)),
    }

def _try_resolve_top_piece_question(
    *,
    question: str,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame | None = None,
    selected_platform: str,
    ui_start: date,
    ui_end: date,
    include_actions: bool = True,
) -> tuple[str, dict[str, str]]:
    if not _is_piece_top_question(question):
        return "", {}

    use_piece_data = bool(
        isinstance(piece_df, pd.DataFrame)
        and not piece_df.empty
        and "date" in piece_df.columns
    )
    source_df = piece_df if use_piece_data else camp_df
    if source_df is None or source_df.empty or "date" not in source_df.columns:
        return "", {}

    metric_key = _detect_piece_metric(question)
    if not metric_key:
        metric_key = "conversions"

    cp = source_df.copy()
    cp["date"] = pd.to_datetime(cp["date"], errors="coerce").dt.date
    cp = cp.dropna(subset=["date"])
    required_defaults: dict[str, Any] = {
        "platform": "",
        "spend": 0.0,
        "impressions": 0.0,
        "clicks": 0.0,
        "conversions": 0.0,
    }
    if use_piece_data:
        required_defaults.update(
            {
                "piece_id": "",
                "piece_name": "",
                "ad_id": "",
                "ad_name": "",
                "campaign_id": "",
                "campaign_name": "",
            }
        )
    else:
        required_defaults.update(
            {
                "campaign_id": "",
                "campaign_name": "",
            }
        )
    for col, default in required_defaults.items():
        if col not in cp.columns:
            cp[col] = default
    if use_piece_data:
        cp = cp.reset_index(drop=True)
        cp["piece_id"] = cp.apply(
            lambda r: (
                str(r.get("piece_id", "")).strip()
                or str(r.get("ad_id", "")).strip()
                or f"piece::{str(r.get('campaign_id', '')).strip() or 'na'}::{r.name}"
            ),
            axis=1,
        )
        cp["piece_name"] = cp.apply(
            lambda r: (
                str(r.get("piece_name", "")).strip()
                or str(r.get("ad_name", "")).strip()
                or str(r.get("campaign_name", "")).strip()
                or "Sin nombre"
            ),
            axis=1,
        )
        fallback_cp = pd.DataFrame()
        if isinstance(camp_df, pd.DataFrame) and not camp_df.empty and "date" in camp_df.columns:
            fallback_cp = camp_df.copy()
            for col, default in required_defaults.items():
                if col not in fallback_cp.columns:
                    fallback_cp[col] = default
            piece_platforms = (
                set(cp["platform"].astype(str).str.strip())
                if "platform" in cp.columns
                else set()
            )
            if "platform" in fallback_cp.columns:
                fallback_cp = fallback_cp[
                    ~fallback_cp["platform"].astype(str).str.strip().isin(piece_platforms)
                ]
            fallback_cp = fallback_cp.reset_index(drop=True)
            if not fallback_cp.empty:
                fallback_cp["piece_id"] = fallback_cp.apply(
                    lambda r: (
                        str(r.get("campaign_id", "")).strip()
                        or f"piece::{str(r.get('platform', '')).strip() or 'na'}::{r.name}"
                    ),
                    axis=1,
                )
                fallback_cp["piece_name"] = fallback_cp.apply(
                    lambda r: (
                        str(r.get("campaign_name", "")).strip()
                        or "Sin nombre"
                    ),
                    axis=1,
                )
                for col in cp.columns:
                    if col not in fallback_cp.columns:
                        fallback_cp[col] = ""
                cp = pd.concat([cp, fallback_cp[cp.columns]], ignore_index=True)
        id_col = "piece_id"
        name_col = "piece_name"
    else:
        cp["piece_id"] = cp["campaign_id"].astype(str)
        cp["piece_name"] = cp["campaign_name"].astype(str)
        id_col = "piece_id"
        name_col = "piece_name"
    cp["platform"] = cp["platform"].astype(str).str.strip().replace({"": "N/A"})
    for num_col in ("spend", "impressions", "clicks", "conversions"):
        cp[num_col] = pd.to_numeric(cp[num_col], errors="coerce").fillna(0.0)
    if cp.empty:
        return "", {}

    data_min = cp["date"].min()
    data_max = cp["date"].max()
    if not isinstance(data_min, date) or not isinstance(data_max, date):
        return "", {}
    range_start, range_end, explicit_range = _resolve_question_range(
        question,
        data_min=data_min,
        data_max=data_max,
        ui_start=ui_start,
        ui_end=ui_end,
    )
    cp = cp[(cp["date"] >= range_start) & (cp["date"] <= range_end)].copy()
    if cp.empty:
        return "", {}

    asked_platform = _platform_from_question(question)
    both_platforms = _question_mentions_both_major_platforms(question)

    platforms_to_use: list[str]
    if both_platforms:
        platforms_to_use = ["Meta", "Google"]
    elif asked_platform in {"Meta", "Google"}:
        platforms_to_use = [asked_platform]
    elif selected_platform in {"Meta", "Google"}:
        platforms_to_use = [selected_platform]
    else:
        platforms_to_use = ["Meta", "Google"]

    metric_label = _piece_metric_label(metric_key)
    findings: list[str] = []
    top_rows: dict[str, dict[str, Any] | None] = {}
    for platform_name in platforms_to_use:
        top_rows[platform_name] = _top_piece_for_platform(
            cp,
            platform_name=platform_name,
            metric_key=metric_key,
            id_col=id_col,
            name_col=name_col,
        )
        row = top_rows[platform_name]
        if row is None:
            findings.append(f"{platform_name}: sin piezas/campañas con datos para el rango.")
            continue
        findings.append(
            f"{platform_name}: {row.get('piece_name')} "
            f"({_piece_metric_fmt(metric_key, float(row.get('metric_value', 0.0)))} {metric_label})."
        )

    if not findings:
        return "", {}

    overall_row: dict[str, Any] | None = None
    if len(platforms_to_use) == 1:
        overall_row = top_rows.get(platforms_to_use[0])
    else:
        valid_rows = [row for row in top_rows.values() if isinstance(row, dict)]
        if valid_rows:
            overall_row = sorted(valid_rows, key=lambda row: float(row.get("metric_value", 0.0)), reverse=True)[0]
    if overall_row is None:
        return "", {}

    headline = (
        f"Top pieza/campaña por {metric_label}: {overall_row.get('piece_name')} "
        f"({_piece_metric_fmt(metric_key, float(overall_row.get('metric_value', 0.0)))})."
    )
    findings.append(f"Rango aplicado: {range_start.isoformat()} a {range_end.isoformat()}.")
    answer = _format_coco_structured_answer(
        headline=headline,
        findings=findings,
        actions=(
            [
                "Compara el top creativo/campaña con el segundo lugar para validar concentración de resultados.",
            ]
            if include_actions
            else []
        ),
    )
    meta = {
        "resolver": "deterministic_top_piece",
        "metric_key": metric_key,
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
        "range_source": "pregunta" if explicit_range else "selector",
        "platforms": ",".join(platforms_to_use),
        "top_piece_name": str(overall_row.get("piece_name", "")).strip(),
        "entity_source": "paid_piece_daily" if use_piece_data else "campaign_daily_fallback",
    }
    return answer, meta

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
