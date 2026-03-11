from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

COCO_DEFAULT_SCOPE_MODE = "total"
COCO_MEMORY_SUMMARY_MAX_CHARS = 2200
COCO_ALLOWED_METRIC_KEYS: tuple[str, ...] = (
    "spend",
    "conv",
    "cpl",
    "ctr",
    "impr",
    "clicks",
    "cpc",
    "cpm",
    "cvr",
    "sessions",
    "users",
    "avg_sess",
    "bounce",
)


def _pct_delta(cur: float | None, prev: float | None) -> float | None:
    if cur is None or prev is None or prev == 0:
        return None
    return ((cur - prev) / abs(prev)) * 100.0


def _coerce_bool(raw_value: Any, default: bool = False) -> bool:
    if isinstance(raw_value, bool):
        return raw_value
    if raw_value is None:
        return bool(default)
    txt = str(raw_value).strip().lower()
    if txt in {"1", "true", "yes", "y", "si", "on"}:
        return True
    if txt in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


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


def _trim_text(value: Any, max_chars: int = 4_000) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text[: max(int(max_chars), 1)]


def _normalize_non_negative_int(
    value: Any,
    fallback: int,
    *,
    minimum: int = 0,
    maximum: int = 100_000,
) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(fallback)
    return max(int(minimum), min(int(maximum), parsed))


def build_coco_metrics_context(
    tenant_name: str,
    tenant_id: str,
    platform: str,
    s: date,
    e: date,
    cur_summary: dict[str, float | None],
    prev_summary: dict[str, float | None],
    *,
    scope_mode: str = COCO_DEFAULT_SCOPE_MODE,
    scope_source: str = "default",
    total_start: date | None = None,
    total_end: date | None = None,
    active_start: date | None = None,
    active_end: date | None = None,
    total_summary_all: dict[str, float | None] | None = None,
    total_summary_selected: dict[str, float | None] | None = None,
    active_summary: dict[str, float | None] | None = None,
    active_prev_summary: dict[str, float | None] | None = None,
    requested_range_explicit: bool = False,
    has_data_in_requested_range: bool | None = None,
) -> dict[str, Any]:
    metric_keys = list(COCO_ALLOWED_METRIC_KEYS)
    current_metrics: dict[str, Any] = {}
    previous_metrics: dict[str, Any] = {}
    deltas: dict[str, Any] = {}
    active_cur = active_summary if isinstance(active_summary, dict) else cur_summary
    active_prev = active_prev_summary if isinstance(active_prev_summary, dict) else prev_summary
    total_all = total_summary_all if isinstance(total_summary_all, dict) else cur_summary
    total_sel = total_summary_selected if isinstance(total_summary_selected, dict) else cur_summary
    safe_scope_mode = str(scope_mode).strip().lower()
    if safe_scope_mode not in {"total", "filter"}:
        safe_scope_mode = COCO_DEFAULT_SCOPE_MODE
    scope_start = s
    scope_end = e
    data_start = total_start if isinstance(total_start, date) else scope_start
    data_end = total_end if isinstance(total_end, date) else scope_end
    for key in metric_keys:
        cur_val = cur_summary.get(key)
        prev_val = prev_summary.get(key)
        current_metrics[key] = cur_val
        previous_metrics[key] = prev_val
        deltas[key] = _pct_delta(cur_val, prev_val)
    return {
        "tenant_name": tenant_name,
        "tenant_id": tenant_id,
        "platform": platform,
        "date_range": {"start": scope_start.isoformat(), "end": scope_end.isoformat()},
        "current_period": current_metrics,
        "previous_period": previous_metrics,
        "delta_vs_previous_pct": deltas,
        "scope": {
            "mode": safe_scope_mode,
            "source": str(scope_source).strip() or "default",
            "default_mode": COCO_DEFAULT_SCOPE_MODE,
        },
        "data_coverage": {
            "tenant_data_start": data_start.isoformat(),
            "tenant_data_end": data_end.isoformat(),
            "requested_start": scope_start.isoformat(),
            "requested_end": scope_end.isoformat(),
            "requested_range_explicit": bool(requested_range_explicit),
            "has_data_in_requested_range": (
                bool(has_data_in_requested_range)
                if has_data_in_requested_range is not None
                else True
            ),
        },
        "total_lifetime": {
            "date_range": {
                "start": (total_start.isoformat() if isinstance(total_start, date) else s.isoformat()),
                "end": (total_end.isoformat() if isinstance(total_end, date) else e.isoformat()),
            },
            "all_platforms": {k: total_all.get(k) for k in metric_keys},
            "selected_platform": {k: total_sel.get(k) for k in metric_keys},
        },
        "active_filter": {
            "platform": platform,
            "date_range": {
                "start": (active_start.isoformat() if isinstance(active_start, date) else s.isoformat()),
                "end": (active_end.isoformat() if isinstance(active_end, date) else e.isoformat()),
            },
            "current_period": {k: active_cur.get(k) for k in metric_keys},
            "previous_period": {k: active_prev.get(k) for k in metric_keys},
            "delta_vs_previous_pct": {k: _pct_delta(active_cur.get(k), active_prev.get(k)) for k in metric_keys},
        },
    }


def sanitize_coco_metric_section(
    section_raw: Any,
    allowed_metric_keys: set[str],
) -> dict[str, float | None]:
    if not isinstance(section_raw, dict):
        return {}
    safe_section: dict[str, float | None] = {}
    for metric_key in allowed_metric_keys:
        if metric_key not in section_raw:
            continue
        value = section_raw.get(metric_key)
        if value is None:
            safe_section[metric_key] = None
            continue
        try:
            parsed = float(value)
        except Exception:
            continue
        if math.isfinite(parsed):
            safe_section[metric_key] = parsed
    return safe_section


def sanitize_coco_context(
    context: dict[str, Any],
    *,
    tenant_id: str,
    platform: str,
    start_day: date,
    end_day: date,
) -> dict[str, Any]:
    safe_tenant = str(tenant_id).strip().lower()
    safe_platform = str(platform).strip()
    safe_start = start_day.isoformat()
    safe_end = end_day.isoformat()
    safe_context = {
        "tenant_name": str(context.get("tenant_name", "")).strip(),
        "tenant_id": safe_tenant,
        "platform": safe_platform,
        "date_range": {"start": safe_start, "end": safe_end},
        "current_period": {},
        "previous_period": {},
        "delta_vs_previous_pct": {},
        "scope": {
            "mode": COCO_DEFAULT_SCOPE_MODE,
            "source": "default",
            "default_mode": COCO_DEFAULT_SCOPE_MODE,
        },
        "data_coverage": {
            "tenant_data_start": safe_start,
            "tenant_data_end": safe_end,
            "requested_start": safe_start,
            "requested_end": safe_end,
            "requested_range_explicit": False,
            "has_data_in_requested_range": True,
        },
        "total_lifetime": {
            "date_range": {"start": safe_start, "end": safe_end},
            "all_platforms": {},
            "selected_platform": {},
        },
        "active_filter": {
            "platform": safe_platform,
            "date_range": {"start": safe_start, "end": safe_end},
            "current_period": {},
            "previous_period": {},
            "delta_vs_previous_pct": {},
        },
        "conversation_memory": {
            "summary": "",
            "turn_count": 0,
        },
    }
    allowed_metric_keys = set(COCO_ALLOWED_METRIC_KEYS)
    for section_key in ("current_period", "previous_period", "delta_vs_previous_pct"):
        safe_context[section_key] = sanitize_coco_metric_section(
            context.get(section_key, {}),
            allowed_metric_keys,
        )

    scope_raw = context.get("scope", {})
    if isinstance(scope_raw, dict):
        mode_raw = str(scope_raw.get("mode", COCO_DEFAULT_SCOPE_MODE)).strip().lower()
        safe_context["scope"]["mode"] = mode_raw if mode_raw in {"total", "filter"} else COCO_DEFAULT_SCOPE_MODE
        safe_context["scope"]["source"] = str(scope_raw.get("source", "default")).strip() or "default"
        safe_context["scope"]["default_mode"] = COCO_DEFAULT_SCOPE_MODE

    coverage_raw = context.get("data_coverage", {})
    if isinstance(coverage_raw, dict):
        tenant_data_start = _parse_iso_date(coverage_raw.get("tenant_data_start")) or start_day
        tenant_data_end = _parse_iso_date(coverage_raw.get("tenant_data_end")) or end_day
        requested_start = _parse_iso_date(coverage_raw.get("requested_start")) or start_day
        requested_end = _parse_iso_date(coverage_raw.get("requested_end")) or end_day
        safe_context["data_coverage"] = {
            "tenant_data_start": tenant_data_start.isoformat(),
            "tenant_data_end": tenant_data_end.isoformat(),
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "requested_range_explicit": _coerce_bool(
                coverage_raw.get("requested_range_explicit"),
                False,
            ),
            "has_data_in_requested_range": _coerce_bool(
                coverage_raw.get("has_data_in_requested_range"),
                True,
            ),
        }

    total_raw = context.get("total_lifetime", {})
    if isinstance(total_raw, dict):
        total_date_raw = total_raw.get("date_range", {})
        if isinstance(total_date_raw, dict):
            total_start = _parse_iso_date(total_date_raw.get("start")) or start_day
            total_end = _parse_iso_date(total_date_raw.get("end")) or end_day
            safe_context["total_lifetime"]["date_range"] = {
                "start": total_start.isoformat(),
                "end": total_end.isoformat(),
            }
        safe_context["total_lifetime"]["all_platforms"] = sanitize_coco_metric_section(
            total_raw.get("all_platforms", {}),
            allowed_metric_keys,
        )
        safe_context["total_lifetime"]["selected_platform"] = sanitize_coco_metric_section(
            total_raw.get("selected_platform", {}),
            allowed_metric_keys,
        )

    active_raw = context.get("active_filter", {})
    if isinstance(active_raw, dict):
        safe_context["active_filter"]["platform"] = str(
            active_raw.get("platform", safe_platform)
        ).strip() or safe_platform
        active_date_raw = active_raw.get("date_range", {})
        if isinstance(active_date_raw, dict):
            active_start = _parse_iso_date(active_date_raw.get("start")) or start_day
            active_end = _parse_iso_date(active_date_raw.get("end")) or end_day
            safe_context["active_filter"]["date_range"] = {
                "start": active_start.isoformat(),
                "end": active_end.isoformat(),
            }
        safe_context["active_filter"]["current_period"] = sanitize_coco_metric_section(
            active_raw.get("current_period", {}),
            allowed_metric_keys,
        )
        safe_context["active_filter"]["previous_period"] = sanitize_coco_metric_section(
            active_raw.get("previous_period", {}),
            allowed_metric_keys,
        )
        safe_context["active_filter"]["delta_vs_previous_pct"] = sanitize_coco_metric_section(
            active_raw.get("delta_vs_previous_pct", {}),
            allowed_metric_keys,
        )

    memory_raw = context.get("conversation_memory", {})
    if isinstance(memory_raw, dict):
        safe_context["conversation_memory"] = {
            "summary": _trim_text(
                memory_raw.get("summary", ""),
                max_chars=COCO_MEMORY_SUMMARY_MAX_CHARS,
            ),
            "turn_count": _normalize_non_negative_int(
                memory_raw.get("turn_count", 0),
                0,
                minimum=0,
                maximum=5_000,
            ),
        }
    return safe_context
