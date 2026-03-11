from __future__ import annotations

import json
from typing import Any

COCO_METRIC_KEYS: tuple[str, ...] = (
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

METRIC_LABELS: dict[str, str] = {
    "spend": "Gasto",
    "conv": "Conversiones",
    "cpl": "CPL",
    "ctr": "CTR",
    "impr": "Impresiones",
    "clicks": "Clics",
    "cpc": "CPC",
    "cpm": "CPM",
    "cvr": "CVR",
    "sessions": "Sesiones",
    "users": "Usuarios",
    "avg_sess": "Promedio de sesion",
    "bounce": "Bounce rate",
}

SECTION_KEYS: tuple[str, ...] = (
    "current_period",
    "previous_period",
    "delta_vs_previous_pct",
    "total_lifetime_all_platforms",
    "total_lifetime_selected_platform",
    "active_filter_current_period",
    "active_filter_previous_period",
    "active_filter_delta_vs_previous_pct",
)


def build_tool_specs() -> list[dict[str, Any]]:
    metric_enum = list(COCO_METRIC_KEYS)
    section_enum = list(SECTION_KEYS)
    return [
        {
            "type": "function",
            "function": {
                "name": "list_available_metrics",
                "description": (
                    "Lista las metricas disponibles y el alcance actual "
                    "de tenant/plataforma/rango."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_scope_and_coverage",
                "description": (
                    "Devuelve metadata del alcance actual, cobertura y "
                    "rangos de datos disponibles."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_metric_values",
                "description": (
                    "Obtiene valores de metricas para una seccion especifica del "
                    "contexto analitico."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "section": {
                            "type": "string",
                            "enum": section_enum,
                        },
                        "metrics": {
                            "type": "array",
                            "items": {"type": "string", "enum": metric_enum},
                        },
                    },
                    "required": ["section"],
                    "additionalProperties": False,
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "compare_metrics",
                "description": (
                    "Entrega current/previous/delta por metrica para comparar "
                    "rendimiento rapidamente."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "metrics": {
                            "type": "array",
                            "items": {"type": "string", "enum": metric_enum},
                        },
                    },
                    "additionalProperties": False,
                },
            },
        },
    ]


def _normalize_metrics(raw_metrics: Any) -> list[str]:
    if not isinstance(raw_metrics, list):
        return list(COCO_METRIC_KEYS)
    selected: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_metrics:
        metric = str(raw_value or "").strip().lower()
        if metric not in COCO_METRIC_KEYS or metric in seen:
            continue
        selected.append(metric)
        seen.add(metric)
    return selected or list(COCO_METRIC_KEYS)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _section_payload(context: dict[str, Any], section: str) -> dict[str, Any]:
    if section == "current_period":
        return _as_dict(context.get("current_period"))
    if section == "previous_period":
        return _as_dict(context.get("previous_period"))
    if section == "delta_vs_previous_pct":
        return _as_dict(context.get("delta_vs_previous_pct"))
    if section == "total_lifetime_all_platforms":
        total = _as_dict(context.get("total_lifetime"))
        return _as_dict(total.get("all_platforms"))
    if section == "total_lifetime_selected_platform":
        total = _as_dict(context.get("total_lifetime"))
        return _as_dict(total.get("selected_platform"))
    if section == "active_filter_current_period":
        active = _as_dict(context.get("active_filter"))
        return _as_dict(active.get("current_period"))
    if section == "active_filter_previous_period":
        active = _as_dict(context.get("active_filter"))
        return _as_dict(active.get("previous_period"))
    if section == "active_filter_delta_vs_previous_pct":
        active = _as_dict(context.get("active_filter"))
        return _as_dict(active.get("delta_vs_previous_pct"))
    return {}


def _tool_list_available_metrics(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": str(context.get("tenant_id", "")),
        "tenant_name": str(context.get("tenant_name", "")),
        "platform": str(context.get("platform", "")),
        "date_range": _as_dict(context.get("date_range")),
        "metrics": [
            {"key": metric, "label": METRIC_LABELS.get(metric, metric)}
            for metric in COCO_METRIC_KEYS
        ],
        "sections": list(SECTION_KEYS),
    }


def _tool_get_scope_and_coverage(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": str(context.get("tenant_id", "")),
        "tenant_name": str(context.get("tenant_name", "")),
        "platform": str(context.get("platform", "")),
        "date_range": _as_dict(context.get("date_range")),
        "scope": _as_dict(context.get("scope")),
        "data_coverage": _as_dict(context.get("data_coverage")),
        "total_lifetime_date_range": _as_dict(
            _as_dict(context.get("total_lifetime")).get("date_range")
        ),
        "active_filter_date_range": _as_dict(
            _as_dict(context.get("active_filter")).get("date_range")
        ),
    }


def _tool_get_metric_values(context: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    section = str(arguments.get("section", "")).strip()
    if section not in SECTION_KEYS:
        return {
            "ok": False,
            "error": "invalid_section",
            "allowed_sections": list(SECTION_KEYS),
        }
    section_payload = _section_payload(context, section)
    selected_metrics = _normalize_metrics(arguments.get("metrics"))
    values = {metric: section_payload.get(metric) for metric in selected_metrics}
    return {
        "ok": True,
        "section": section,
        "metrics": values,
    }


def _tool_compare_metrics(context: dict[str, Any], arguments: dict[str, Any]) -> dict[str, Any]:
    selected_metrics = _normalize_metrics(arguments.get("metrics"))
    current = _as_dict(context.get("current_period"))
    previous = _as_dict(context.get("previous_period"))
    delta = _as_dict(context.get("delta_vs_previous_pct"))
    active = _as_dict(context.get("active_filter"))
    active_current = _as_dict(active.get("current_period"))
    active_previous = _as_dict(active.get("previous_period"))
    active_delta = _as_dict(active.get("delta_vs_previous_pct"))
    total = _as_dict(context.get("total_lifetime"))
    total_selected = _as_dict(total.get("selected_platform"))
    rows: list[dict[str, Any]] = []
    for metric in selected_metrics:
        rows.append(
            {
                "metric": metric,
                "label": METRIC_LABELS.get(metric, metric),
                "current_period": current.get(metric),
                "previous_period": previous.get(metric),
                "delta_vs_previous_pct": delta.get(metric),
                "active_filter_current_period": active_current.get(metric),
                "active_filter_previous_period": active_previous.get(metric),
                "active_filter_delta_vs_previous_pct": active_delta.get(metric),
                "total_lifetime_selected_platform": total_selected.get(metric),
            }
        )
    return {
        "ok": True,
        "rows": rows,
    }


def execute_tool(
    *,
    tool_name: str,
    arguments: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    if tool_name == "list_available_metrics":
        return _tool_list_available_metrics(context)
    if tool_name == "get_scope_and_coverage":
        return _tool_get_scope_and_coverage(context)
    if tool_name == "get_metric_values":
        return _tool_get_metric_values(context, arguments)
    if tool_name == "compare_metrics":
        return _tool_compare_metrics(context, arguments)
    return {"ok": False, "error": "unknown_tool", "tool_name": tool_name}


def safe_parse_tool_arguments(raw_arguments: Any) -> dict[str, Any]:
    if isinstance(raw_arguments, dict):
        return raw_arguments
    if not isinstance(raw_arguments, str):
        return {}
    try:
        parsed = json.loads(raw_arguments)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}
