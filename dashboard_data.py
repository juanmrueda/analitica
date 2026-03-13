from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_COUNTRY_CODE_TO_NAME: dict[str, str] = {
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

EMPTY_TEXT_TOKENS = {"nan", "none", "null", "nat", "<na>", "n/a", "na"}


def safe_float(v: Any) -> float:
    if v in (None, ""):
        return 0.0
    if isinstance(v, list):
        return sum(safe_float(x) for x in v)
    if isinstance(v, dict):
        return safe_float(v.get("value", 0.0))
    if isinstance(v, str):
        v = v.strip().replace(",", "")
    return float(v)


def series_safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    den = pd.to_numeric(denominator, errors="coerce")
    return pd.to_numeric(numerator, errors="coerce").div(den.where(den != 0)).fillna(0.0)


def report_cache_signature(path: Path) -> tuple[str, int, int]:
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"No se encontro: {resolved}")
    stat = resolved.stat()
    return str(resolved), int(stat.st_mtime_ns), int(stat.st_size)


def report_parquet_dataset_path(report_path: Path, dataset_key: str, parquet_dirname: str) -> Path:
    dataset = str(dataset_key or "").strip()
    return report_path.resolve().parent / str(parquet_dirname) / f"{dataset}.parquet"


def parquet_cache_signature(
    report_path: Path, dataset_key: str, parquet_dirname: str
) -> tuple[str, int, int] | None:
    parquet_path = report_parquet_dataset_path(report_path, dataset_key, parquet_dirname)
    if not parquet_path.exists():
        return None
    stat = parquet_path.stat()
    return str(parquet_path), int(stat.st_mtime_ns), int(stat.st_size)


def parquet_bundle_health(
    report_path: Path,
    *,
    parquet_dirname: str,
    core_datasets: tuple[str, ...],
    stale_tolerance_ns: int,
) -> dict[str, Any]:
    resolved_report = report_path.resolve()
    report_stat = resolved_report.stat()
    bundle_dir = resolved_report.parent / str(parquet_dirname)
    health: dict[str, Any] = {
        "bundle_dir": bundle_dir,
        "missing": [],
        "stale": [],
        "ok": True,
    }
    if not bundle_dir.exists():
        health["missing"] = list(core_datasets)
        health["ok"] = False
        return health
    missing: list[str] = []
    stale: list[str] = []
    for dataset_key in core_datasets:
        parquet_path = bundle_dir / f"{dataset_key}.parquet"
        if not parquet_path.exists():
            missing.append(dataset_key)
            continue
        try:
            parquet_stat = parquet_path.stat()
        except Exception:
            stale.append(dataset_key)
            continue
        if int(parquet_stat.st_mtime_ns) + int(stale_tolerance_ns) < int(report_stat.st_mtime_ns):
            stale.append(dataset_key)
    health["missing"] = missing
    health["stale"] = stale
    health["ok"] = not missing and not stale
    return health


def load_report_json(path_str: str) -> dict[str, Any]:
    return json.loads(Path(path_str).read_text(encoding="utf-8"))


def load_parquet_df(path_str: str) -> pd.DataFrame:
    return pd.read_parquet(path_str)


def normalize_daily_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    else:
        out["date"] = pd.NaT
    for col in [c for c in out.columns if c != "date"]:
        series = out[col]
        if pd.api.types.is_numeric_dtype(series):
            if series.isna().any():
                out[col] = series.fillna(0.0)
            continue
        out[col] = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def normalize_hourly_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    else:
        out["timestamp"] = pd.NaT
    if "date" in out.columns:
        parsed_date = pd.to_datetime(out["date"], errors="coerce")
        out["date"] = parsed_date.dt.date
    else:
        out["date"] = out["timestamp"].dt.date
    if "hour" not in out.columns:
        out["hour"] = out["timestamp"].dt.hour
    else:
        hour_series = out["hour"]
        if pd.api.types.is_numeric_dtype(hour_series):
            out["hour"] = hour_series.fillna(out["timestamp"].dt.hour).fillna(0).astype(int)
        else:
            out["hour"] = pd.to_numeric(hour_series, errors="coerce").fillna(out["timestamp"].dt.hour).fillna(0).astype(int)
    for col in [c for c in out.columns if c not in {"timestamp", "date"}]:
        series = out[col]
        if pd.api.types.is_numeric_dtype(series):
            if series.isna().any():
                out[col] = series.fillna(0.0)
            continue
        out[col] = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return out.dropna(subset=["timestamp", "date"]).sort_values(["timestamp"]).reset_index(drop=True)


def normalize_acq_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    for col in out.columns:
        if col != "date" and out[col].dtype == object:
            try:
                out[col] = pd.to_numeric(out[col])
            except Exception:
                pass
    return out.dropna(subset=["date"]).reset_index(drop=True) if "date" in out.columns else out


def daily_df(report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in report.get("daily", []):
        m = row.get("meta", {})
        g = row.get("google_ads", {})
        ga4 = row.get("ga4", {})
        n = row.get("normalization", {})
        ms, gs = safe_float(m.get("spend")), safe_float(g.get("cost"))
        mc, gc = safe_float(m.get("clicks")), safe_float(g.get("clicks"))
        mv, gv = safe_float(m.get("conversions")), safe_float(g.get("conversions"))
        mi, gi = safe_float(m.get("impressions")), safe_float(g.get("impressions"))
        rows.append(
            {
                "date": row.get("date"),
                "meta_spend": ms,
                "google_spend": gs,
                "total_spend": safe_float(n.get("total_spend")) or (ms + gs),
                "meta_clicks": mc,
                "google_clicks": gc,
                "total_clicks": safe_float(n.get("total_clicks")) or (mc + gc),
                "meta_conv": mv,
                "google_conv": gv,
                "total_conv": safe_float(n.get("total_conversions")) or (mv + gv),
                "meta_impr": mi,
                "google_impr": gi,
                "total_impr": safe_float(n.get("total_impressions")) or (mi + gi),
                "ga4_sessions": safe_float(ga4.get("sessions")),
                "ga4_users": safe_float(ga4.get("totalUsers")),
                "ga4_avg_sess": safe_float(ga4.get("averageSessionDuration")),
                "ga4_bounce": safe_float(ga4.get("bounceRate")),
            }
        )
    return normalize_daily_table(pd.DataFrame(rows))


def hourly_df(report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in report.get("hourly", []):
        m = row.get("meta", {})
        g = row.get("google_ads", {})
        n = row.get("normalization", {})
        dt_raw = str(row.get("datetime", "")).strip()
        if not dt_raw:
            raw_day = str(row.get("date", "")).strip()
            raw_hour = int(safe_float(row.get("hour")))
            if raw_day:
                dt_raw = f"{raw_day} {raw_hour:02d}:00:00"
        ms = safe_float(m.get("spend"))
        gs = safe_float(g.get("cost"))
        mc = safe_float(m.get("clicks"))
        gc = safe_float(g.get("clicks"))
        mv = safe_float(m.get("conversions"))
        gv = safe_float(g.get("conversions"))
        mi = safe_float(m.get("impressions"))
        gi = safe_float(g.get("impressions"))
        rows.append(
            {
                "timestamp": dt_raw,
                "date": row.get("date"),
                "hour": safe_float(row.get("hour")),
                "meta_spend": ms,
                "google_spend": gs,
                "total_spend": safe_float(n.get("total_spend")) or (ms + gs),
                "meta_clicks": mc,
                "google_clicks": gc,
                "total_clicks": safe_float(n.get("total_clicks")) or (mc + gc),
                "meta_conv": mv,
                "google_conv": gv,
                "total_conv": safe_float(n.get("total_conversions")) or (mv + gv),
                "meta_impr": mi,
                "google_impr": gi,
                "total_impr": safe_float(n.get("total_impressions")) or (mi + gi),
                "ga4_sessions": 0.0,
                "ga4_users": 0.0,
                "ga4_avg_sess": 0.0,
                "ga4_bounce": 0.0,
            }
        )
    return normalize_hourly_table(pd.DataFrame(rows))


def acq_df(report: dict[str, Any], key: str) -> pd.DataFrame:
    rows = report.get("traffic_acquisition", {}).get(key, [])
    return normalize_acq_table(pd.DataFrame(rows))


def clean_text_value(raw_value: Any, default: str = "") -> str:
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
    if txt.casefold() in EMPTY_TEXT_TOKENS:
        return default
    return txt


def clean_text_series(series: pd.Series, default: str = "") -> pd.Series:
    out = series.fillna("").astype(str).str.strip()
    out = out.mask(out.str.casefold().isin(EMPTY_TEXT_TOKENS), "")
    if default:
        out = out.mask(out == "", default)
    return out


def numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series([float(default)] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(float(default))


def normalize_paid_device_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
        out = out.dropna(subset=["date"])
    else:
        out["date"] = pd.NaT
    required = ["platform", "device", "spend", "impressions", "clicks", "conversions"]
    for col in required:
        if col not in out.columns:
            out[col] = "" if col in ("platform", "device") else 0.0
    for col in ("spend", "impressions", "clicks", "conversions"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["device"] = (
        out["device"].astype(str).str.strip().replace({"desktop": "Desktop", "mobile": "Mobile", "other": "Other"})
    )
    return out


def paid_device_df(report: dict[str, Any]) -> pd.DataFrame:
    return normalize_paid_device_table(acq_df(report, "paid_device_daily"))


def normalize_age_bucket(raw_value: Any) -> str:
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


def normalize_gender_bucket(raw_value: Any) -> str:
    txt = str(raw_value or "").strip().lower()
    if txt in {"female", "f"}:
        return "Female"
    if txt in {"male", "m"}:
        return "Male"
    return "Unknown"


def country_name_from_code(raw_value: Any, country_code_to_name: dict[str, str] | None = None) -> str:
    code = clean_text_value(raw_value).upper()
    mapping = country_code_to_name or DEFAULT_COUNTRY_CODE_TO_NAME
    return mapping.get(code, "")


def normalize_campaign_unified_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    else:
        out["date"] = pd.NaT
    required_text_cols = [
        "platform",
        "source",
        "campaign_id",
        "campaign_name",
        "campaign_goal",
        "objective",
        "advertising_channel_type",
        "advertising_channel_sub_type",
        "bidding_strategy_type",
    ]
    for col in required_text_cols:
        if col not in out.columns:
            out[col] = ""
        out[col] = clean_text_series(out[col])

    spend_series = numeric_series(out, "spend")
    if "spend" not in out.columns and "cost" in out.columns:
        spend_series = numeric_series(out, "cost")
    out["spend"] = spend_series
    for col in ("impressions", "clicks", "conversions", "reach", "frequency"):
        out[col] = numeric_series(out, col)
    out["platform"] = out["platform"].replace({"": "Meta"})
    out["campaign_name"] = out["campaign_name"].replace({"": "Sin nombre"})
    out["ctr"] = series_safe_div(out["clicks"], out["impressions"])
    out["cpc"] = series_safe_div(out["spend"], out["clicks"])
    out["cpl"] = series_safe_div(out["spend"], out["conversions"])
    return out.dropna(subset=["date"]).sort_values(["date", "platform", "campaign_id"]).reset_index(drop=True)


def normalize_paid_piece_enriched_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    else:
        out["date"] = pd.NaT

    required_text_cols = [
        "platform",
        "campaign_id",
        "campaign_name",
        "piece_id",
        "piece_name",
        "ad_id",
        "ad_name",
        "image_url",
        "thumbnail_url",
        "preview_url",
        "preview_image",
    ]
    for col in required_text_cols:
        if col not in out.columns:
            out[col] = ""
        out[col] = clean_text_series(out[col])

    for col in ("spend", "impressions", "clicks", "conversions"):
        out[col] = numeric_series(out, col)

    out["platform"] = out["platform"].replace({"": "Meta"})
    out["campaign_name"] = out["campaign_name"].replace({"": "Sin nombre"})
    fallback_campaign = out["campaign_id"].replace({"": "na"})
    fallback_index = pd.Series(out.index, index=out.index).astype(str)
    fallback_piece = "piece::" + fallback_campaign + "::" + fallback_index
    out["piece_id"] = out["piece_id"].mask(out["piece_id"] == "", out["ad_id"])
    out["piece_id"] = out["piece_id"].mask(out["piece_id"] == "", fallback_piece)
    out["piece_name"] = out["piece_name"].mask(out["piece_name"] == "", out["ad_name"])
    out["piece_name"] = out["piece_name"].mask(out["piece_name"] == "", out["campaign_name"])
    out["piece_name"] = out["piece_name"].replace({"": "Sin nombre"})
    out["preview_image"] = out["preview_image"].mask(out["preview_image"] == "", out["preview_url"])
    out["preview_image"] = out["preview_image"].mask(out["preview_image"] == "", out["image_url"])
    out["preview_image"] = out["preview_image"].mask(out["preview_image"] == "", out["thumbnail_url"])
    out["ctr"] = series_safe_div(out["clicks"], out["impressions"])
    out["cpc"] = series_safe_div(out["spend"], out["clicks"])
    out["cpl"] = series_safe_div(out["spend"], out["conversions"])
    return out.dropna(subset=["date"]).sort_values(["date", "platform", "campaign_id", "piece_id"]).reset_index(drop=True)


def normalize_paid_lead_demographics_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    required = ["platform", "breakdown", "age_range", "gender", "leads", "spend", "impressions", "clicks"]
    for col in required:
        if col not in out.columns:
            out[col] = "" if col in ("platform", "breakdown", "age_range", "gender") else 0.0
    for col in ("leads", "spend", "impressions", "clicks"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["platform"] = out["platform"].astype(str).str.strip().replace({"": "Meta"})
    out["breakdown"] = out["breakdown"].astype(str).str.strip().replace({"": "age_gender"}).str.lower()
    valid_breakdowns = {"age_gender", "age", "gender"}
    out.loc[~out["breakdown"].isin(valid_breakdowns), "breakdown"] = "age_gender"
    out["age_range"] = out["age_range"].apply(normalize_age_bucket)
    out["gender"] = out["gender"].apply(normalize_gender_bucket)
    return out


def paid_lead_demographics_df(report: dict[str, Any]) -> pd.DataFrame:
    return normalize_paid_lead_demographics_table(acq_df(report, "paid_lead_demographics_daily"))


def normalize_paid_lead_geo_table(
    df: pd.DataFrame, country_code_to_name: dict[str, str] | None = None
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "country_code" not in out.columns and "country" in out.columns:
        out["country_code"] = out["country"]
    required = ["platform", "country_code", "country_name", "region", "leads", "spend", "impressions", "clicks"]
    for col in required:
        if col not in out.columns:
            out[col] = "" if col in ("platform", "country_code", "country_name", "region") else 0.0
    for col in ("leads", "spend", "impressions", "clicks"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["platform"] = out["platform"].apply(lambda v: clean_text_value(v, "Meta") or "Meta")
    out["country_code"] = out["country_code"].apply(clean_text_value).str.upper()
    out["country_name"] = out["country_name"].apply(clean_text_value)
    out["country_name"] = out.apply(
        lambda row: clean_text_value(row.get("country_name"))
        or country_name_from_code(row.get("country_code"), country_code_to_name),
        axis=1,
    )
    out["region"] = out["region"].apply(lambda v: clean_text_value(v, "Unknown") or "Unknown")
    return out


def paid_lead_geo_df(report: dict[str, Any], country_code_to_name: dict[str, str] | None = None) -> pd.DataFrame:
    return normalize_paid_lead_geo_table(acq_df(report, "paid_lead_geo_daily"), country_code_to_name)


def build_campaign_unified_from_raw_tables(meta_df: pd.DataFrame, google_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if isinstance(meta_df, pd.DataFrame) and not meta_df.empty:
        meta = meta_df.copy()
        meta["platform"] = "Meta"
        if "spend" not in meta.columns:
            meta["spend"] = 0.0
        frames.append(meta)
    if isinstance(google_df, pd.DataFrame) and not google_df.empty:
        google = google_df.copy()
        google["platform"] = "Google"
        if "spend" not in google.columns:
            google["spend"] = pd.to_numeric(google.get("cost"), errors="coerce").fillna(0.0)
        frames.append(google)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
