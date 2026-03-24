#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import importlib.util
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
PIPELINE_SCRIPT_PATH = ROOT_DIR / "scripts" / "yap_daily_cpl_report.py"
DEFAULT_TENANTS_CONFIG_PATH = ROOT_DIR / "config" / "tenants.json"


def _load_pipeline_module() -> Any:
    spec = importlib.util.spec_from_file_location("yap_daily_cpl_report", PIPELINE_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load pipeline module from {PIPELINE_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PIPELINE = _load_pipeline_module()


@dataclass(frozen=True)
class Window:
    label: str
    start: date
    end: date


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a month-by-month paid backfill for a tenant and validate each cut against source APIs."
    )
    parser.add_argument("--tenant-id", required=True, help="Tenant id from config/tenants.json")
    parser.add_argument(
        "--tenants-config-path",
        default=str(DEFAULT_TENANTS_CONFIG_PATH),
        help="Path to tenants JSON config.",
    )
    parser.add_argument(
        "--bootstrap-start",
        default="2025-01-01",
        help="Historical floor / initial bootstrap start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--monthly-through",
        default="2025-12-31",
        help="Last date to process month by month (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--catchup-end",
        default=date.today().isoformat(),
        help="Final catch-up end date after monthly 2025 backfill (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--python-executable",
        default=sys.executable,
        help="Python executable used to invoke yap_daily_cpl_report.py",
    )
    parser.add_argument(
        "--refresh-lookback-days",
        type=int,
        default=0,
        help="Passed through to yap_daily_cpl_report.py",
    )
    return parser.parse_args()


def _parse_iso_date(raw: str) -> date:
    return datetime.strptime(str(raw).strip(), "%Y-%m-%d").date()


def _month_end(day: date) -> date:
    last_day = calendar.monthrange(day.year, day.month)[1]
    return date(day.year, day.month, last_day)


def _iter_month_windows(start_day: date, end_day: date) -> list[Window]:
    windows: list[Window] = []
    cursor = start_day
    while cursor <= end_day:
        window_end = min(_month_end(cursor), end_day)
        windows.append(
            Window(
                label=f"{cursor.year:04d}-{cursor.month:02d}",
                start=cursor,
                end=window_end,
            )
        )
        cursor = window_end + timedelta(days=1)
    return windows


def _resolve_report_path(tenant_cfg: dict[str, Any]) -> Path:
    return PIPELINE._resolve_repo_path(tenant_cfg.get("report_path", PIPELINE.DEFAULT_OUTPUT_PATH))


def _load_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"Expected report file was not created: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _date_in_window(raw: Any, start_day: date, end_day: date) -> bool:
    try:
        current = _parse_iso_date(str(raw))
    except Exception:
        return False
    return start_day <= current <= end_day


def _sum_report_totals(report: dict[str, Any], start_day: date, end_day: date) -> dict[str, dict[str, float]]:
    totals = {
        "meta": {"spend": 0.0, "clicks": 0.0, "impressions": 0.0, "conversions": 0.0},
        "google": {"spend": 0.0, "clicks": 0.0, "impressions": 0.0, "conversions": 0.0},
    }
    for row in report.get("daily", []):
        if not _date_in_window(row.get("date"), start_day, end_day):
            continue
        meta = row.get("meta", {})
        google = row.get("google_ads", {})
        totals["meta"]["spend"] += float(meta.get("spend", 0.0) or 0.0)
        totals["meta"]["clicks"] += float(meta.get("clicks", 0.0) or 0.0)
        totals["meta"]["impressions"] += float(meta.get("impressions", 0.0) or 0.0)
        totals["meta"]["conversions"] += float(meta.get("conversions", 0.0) or 0.0)
        totals["google"]["spend"] += float(google.get("cost", 0.0) or 0.0)
        totals["google"]["clicks"] += float(google.get("clicks", 0.0) or 0.0)
        totals["google"]["impressions"] += float(google.get("impressions", 0.0) or 0.0)
        totals["google"]["conversions"] += float(google.get("conversions", 0.0) or 0.0)
    return totals


def _sum_source_daily(rows_by_day: dict[str, dict[str, float]], start_day: date, end_day: date, spend_key: str) -> dict[str, float]:
    out = {"spend": 0.0, "clicks": 0.0, "impressions": 0.0, "conversions": 0.0}
    for raw_day, metrics in rows_by_day.items():
        if not _date_in_window(raw_day, start_day, end_day):
            continue
        out["spend"] += float(metrics.get(spend_key, 0.0) or 0.0)
        out["clicks"] += float(metrics.get("clicks", 0.0) or 0.0)
        out["impressions"] += float(metrics.get("impressions", 0.0) or 0.0)
        out["conversions"] += float(metrics.get("conversions", 0.0) or 0.0)
    return out


def _has_activity(metrics: dict[str, float]) -> bool:
    return any(abs(float(metrics.get(key, 0.0) or 0.0)) > 0.0 for key in ("spend", "clicks", "impressions", "conversions"))


def _validate_tolerance(platform_name: str, metric_name: str, actual: float, expected: float) -> None:
    if metric_name == "conversions":
        allowed = max(1.0, abs(expected) * 0.05)
        if abs(actual - expected) > allowed:
            raise RuntimeError(
                f"{platform_name} {metric_name} out of tolerance: actual={actual:.6f} expected={expected:.6f} allowed_abs={allowed:.6f}"
            )
        return

    if abs(expected) == 0.0:
        if abs(actual) > 0.0:
            raise RuntimeError(
                f"{platform_name} {metric_name} expected zero activity but report has {actual:.6f}"
            )
        return

    relative_error = abs(actual - expected) / abs(expected)
    if relative_error > 0.01:
        raise RuntimeError(
            f"{platform_name} {metric_name} out of tolerance: actual={actual:.6f} expected={expected:.6f} rel_error={relative_error:.4%}"
        )


def _validate_report_structure(
    report: dict[str, Any],
    *,
    tenant_id: str,
    expected_meta_id: str,
    expected_google_id: str,
    bootstrap_start: date,
    expected_end: date,
) -> None:
    metadata = report.get("metadata", {})
    tenant_meta = metadata.get("tenant", {})
    ids = metadata.get("ids", {})
    if str(tenant_meta.get("id", "")).strip().lower() != tenant_id:
        raise RuntimeError(f"Unexpected tenant id in report metadata: {tenant_meta}")
    if str(ids.get("meta_ad_account_id", "")).strip() != expected_meta_id:
        raise RuntimeError(f"Unexpected Meta account id in report metadata: {ids}")
    if str(ids.get("google_ads_customer_id", "")).strip() != expected_google_id:
        raise RuntimeError(f"Unexpected Google Ads customer id in report metadata: {ids}")

    daily_rows = report.get("daily", [])
    if not isinstance(daily_rows, list) or not daily_rows:
        raise RuntimeError("Report has no daily rows.")

    actual_dates = sorted(
        _parse_iso_date(str(row.get("date")))
        for row in daily_rows
        if str(row.get("date", "")).strip()
    )
    if not actual_dates:
        raise RuntimeError("Report daily rows have no valid dates.")
    if actual_dates[0] != bootstrap_start:
        raise RuntimeError(f"Unexpected min daily date: {actual_dates[0]} != {bootstrap_start}")
    if actual_dates[-1] != expected_end:
        raise RuntimeError(f"Unexpected max daily date: {actual_dates[-1]} != {expected_end}")

    expected_count = (expected_end - bootstrap_start).days + 1
    unique_dates = {d.isoformat() for d in actual_dates}
    if len(unique_dates) != expected_count:
        raise RuntimeError(
            f"Daily coverage gap detected: expected {expected_count} unique dates from {bootstrap_start} to {expected_end}, got {len(unique_dates)}"
        )
    current = bootstrap_start
    while current <= expected_end:
        if current.isoformat() not in unique_dates:
            raise RuntimeError(f"Missing daily row for {current.isoformat()}")
        current += timedelta(days=1)


def _validate_breakdowns(report: dict[str, Any], start_day: date, end_day: date, *, source_meta: dict[str, float], source_google: dict[str, float]) -> None:
    acquisition = report.get("traffic_acquisition", {})
    meta_rows = acquisition.get("meta_campaign_daily", [])
    google_rows = acquisition.get("google_campaign_daily", [])

    meta_count = sum(1 for row in meta_rows if _date_in_window(row.get("date"), start_day, end_day))
    google_count = sum(1 for row in google_rows if _date_in_window(row.get("date"), start_day, end_day))

    if _has_activity(source_meta) and meta_count == 0:
        raise RuntimeError(
            f"Meta campaign breakdown missing for {start_day.isoformat()}..{end_day.isoformat()} despite source activity."
        )
    if _has_activity(source_google) and google_count == 0:
        raise RuntimeError(
            f"Google campaign breakdown missing for {start_day.isoformat()}..{end_day.isoformat()} despite source activity."
        )


def _print_comparison(label: str, report_totals: dict[str, float], source_totals: dict[str, float]) -> None:
    print(
        f"[validate:{label}] "
        f"report spend={report_totals['spend']:.2f} clicks={report_totals['clicks']:.2f} "
        f"impr={report_totals['impressions']:.2f} conv={report_totals['conversions']:.2f} | "
        f"source spend={source_totals['spend']:.2f} clicks={source_totals['clicks']:.2f} "
        f"impr={source_totals['impressions']:.2f} conv={source_totals['conversions']:.2f}"
    )


def _validate_month(
    report: dict[str, Any],
    *,
    label: str,
    start_day: date,
    end_day: date,
    source_meta: dict[str, float],
    source_google: dict[str, float],
) -> None:
    report_totals = _sum_report_totals(report, start_day, end_day)

    _print_comparison(f"{label}:meta", report_totals["meta"], source_meta)
    _print_comparison(f"{label}:google", report_totals["google"], source_google)

    for metric in ("spend", "clicks", "impressions", "conversions"):
        _validate_tolerance("Meta", metric, report_totals["meta"][metric], source_meta[metric])
        _validate_tolerance("Google", metric, report_totals["google"][metric], source_google[metric])

    _validate_breakdowns(
        report,
        start_day,
        end_day,
        source_meta=source_meta,
        source_google=source_google,
    )


def _run_pipeline(
    *,
    python_executable: str,
    tenant_id: str,
    tenants_config_path: Path,
    bootstrap_start: date,
    end_day: date,
    refresh_lookback_days: int,
) -> None:
    cmd = [
        str(python_executable),
        str(PIPELINE_SCRIPT_PATH),
        "--tenant-id",
        tenant_id,
        "--tenants-config-path",
        str(tenants_config_path),
        "--mode",
        "auto",
        "--bootstrap-start",
        bootstrap_start.isoformat(),
        "--end-date",
        end_day.isoformat(),
        "--refresh-lookback-days",
        str(refresh_lookback_days),
    ]
    print(f"[run] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT_DIR))
    if result.returncode != 0:
        raise RuntimeError(f"Pipeline failed with exit code {result.returncode} for end date {end_day.isoformat()}")


def _build_sources(tenant_cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = PIPELINE._load_codex_config(PIPELINE.CODEX_CONFIG_PATH)

    meta_env = cfg.get("mcp_servers", {}).get("meta-ads-mcp", {}).get("env", {})
    meta_token = str(meta_env.get("META_ADS_ACCESS_TOKEN", "")).strip()
    if not meta_token:
        raise RuntimeError("META_ADS_ACCESS_TOKEN not found in ~/.codex/config.toml")
    meta_ad_account_id = PIPELINE._normalize_meta_ad_account_id(
        tenant_cfg.get("meta_ad_account_id", PIPELINE.META_AD_ACCOUNT_ID)
    )

    google_env = cfg.get("mcp_servers", {}).get("google-ads-mcp", {}).get("env", {})
    ga_client_id = str(google_env.get("GOOGLE_ADS_CLIENT_ID", "")).strip()
    ga_client_secret = str(google_env.get("GOOGLE_ADS_CLIENT_SECRET", "")).strip()
    ga_refresh_token = str(google_env.get("GOOGLE_ADS_REFRESH_TOKEN", "")).strip()
    ga_developer_token = str(google_env.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")).strip()
    if not all([ga_client_id, ga_client_secret, ga_refresh_token, ga_developer_token]):
        raise RuntimeError("Missing Google Ads OAuth/developer token values in ~/.codex/config.toml")

    google_customer_id = str(
        tenant_cfg.get("google_ads_customer_id", tenant_cfg.get("google_customer_id", PIPELINE.GOOGLE_ADS_CUSTOMER_ID))
    ).strip()
    google_login_customer_id = str(
        tenant_cfg.get("google_ads_login_customer_id")
        or google_env.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        or google_customer_id
    ).strip()
    ga_quota_project = str(google_env.get("GOOGLE_ADS_QUOTA_PROJECT", "")).strip() or None
    google_access_token = PIPELINE._google_access_token(
        ga_client_id,
        ga_client_secret,
        ga_refresh_token,
    )

    return {
        "meta_token": meta_token,
        "meta_ad_account_id": meta_ad_account_id,
        "google_access_token": google_access_token,
        "google_developer_token": ga_developer_token,
        "google_customer_id": google_customer_id,
        "google_login_customer_id": google_login_customer_id,
        "google_quota_project": ga_quota_project,
    }


def _fetch_source_totals(sources: dict[str, Any], start_day: date, end_day: date) -> tuple[dict[str, float], dict[str, float]]:
    meta_by_day = PIPELINE._fetch_meta_range(
        str(sources["meta_token"]),
        str(sources["meta_ad_account_id"]),
        start_day,
        end_day,
    )
    google_by_day = PIPELINE._fetch_google_ads_range(
        access_token=str(sources["google_access_token"]),
        developer_token=str(sources["google_developer_token"]),
        customer_id=str(sources["google_customer_id"]),
        login_customer_id=str(sources["google_login_customer_id"]),
        quota_project=sources["google_quota_project"],
        start_day=start_day,
        end_day=end_day,
    )
    return (
        _sum_source_daily(meta_by_day, start_day, end_day, spend_key="spend"),
        _sum_source_daily(google_by_day, start_day, end_day, spend_key="cost"),
    )


def main() -> int:
    args = _parse_args()
    bootstrap_start = _parse_iso_date(args.bootstrap_start)
    monthly_through = _parse_iso_date(args.monthly_through)
    catchup_end = _parse_iso_date(args.catchup_end)
    tenants_config_path = PIPELINE._resolve_repo_path(args.tenants_config_path)

    if monthly_through < bootstrap_start:
        raise RuntimeError("monthly-through must be on or after bootstrap-start")
    if catchup_end < monthly_through:
        raise RuntimeError("catchup-end must be on or after monthly-through")

    tenant_cfg = PIPELINE._resolve_tenant_config(
        tenant_id=str(args.tenant_id).strip().lower(),
        tenants_config_path=tenants_config_path,
    )
    tenant_id = str(tenant_cfg.get("id", args.tenant_id)).strip().lower()
    expected_meta_id = PIPELINE._normalize_meta_ad_account_id(tenant_cfg.get("meta_ad_account_id"))
    expected_google_id = str(
        tenant_cfg.get("google_ads_customer_id", tenant_cfg.get("google_customer_id", ""))
    ).strip()
    report_path = _resolve_report_path(tenant_cfg)
    sources = _build_sources(tenant_cfg)

    print(f"[start] tenant={tenant_id} monthly_through={monthly_through} catchup_end={catchup_end}")
    for window in _iter_month_windows(bootstrap_start, monthly_through):
        print(f"[window] {window.label} -> {window.start.isoformat()}..{window.end.isoformat()}")
        _run_pipeline(
            python_executable=str(args.python_executable),
            tenant_id=tenant_id,
            tenants_config_path=tenants_config_path,
            bootstrap_start=bootstrap_start,
            end_day=window.end,
            refresh_lookback_days=int(args.refresh_lookback_days),
        )
        report = _load_report(report_path)
        _validate_report_structure(
            report,
            tenant_id=tenant_id,
            expected_meta_id=expected_meta_id,
            expected_google_id=expected_google_id,
            bootstrap_start=bootstrap_start,
            expected_end=window.end,
        )
        source_meta, source_google = _fetch_source_totals(sources, window.start, window.end)
        _validate_month(
            report,
            label=window.label,
            start_day=window.start,
            end_day=window.end,
            source_meta=source_meta,
            source_google=source_google,
        )
        print(f"[ok] {window.label} validated")

    catchup_start = monthly_through + timedelta(days=1)
    if catchup_start <= catchup_end:
        catchup_label = f"{catchup_start.isoformat()}..{catchup_end.isoformat()}"
        print(f"[catchup] {catchup_label}")
        _run_pipeline(
            python_executable=str(args.python_executable),
            tenant_id=tenant_id,
            tenants_config_path=tenants_config_path,
            bootstrap_start=bootstrap_start,
            end_day=catchup_end,
            refresh_lookback_days=int(args.refresh_lookback_days),
        )
        report = _load_report(report_path)
        _validate_report_structure(
            report,
            tenant_id=tenant_id,
            expected_meta_id=expected_meta_id,
            expected_google_id=expected_google_id,
            bootstrap_start=bootstrap_start,
            expected_end=catchup_end,
        )
        source_meta, source_google = _fetch_source_totals(sources, catchup_start, catchup_end)
        _validate_month(
            report,
            label="catchup",
            start_day=catchup_start,
            end_day=catchup_end,
            source_meta=source_meta,
            source_google=source_google,
        )
        print(f"[ok] catchup validated: {catchup_label}")

    print(f"[done] tenant={tenant_id} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
