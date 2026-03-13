from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

# Umbral de confianza por defecto: por debajo de este valor COCO pide aclaración
CONFIDENCE_THRESHOLD = 0.70

# Intents disponibles con sus parámetros esperados
INTENT_CATALOG: dict[str, dict[str, Any]] = {
    "year_period_comparison": {
        "description": "Comparar los primeros N meses de un año vs otro.",
        "params": ["base_year", "target_year", "months", "platform", "table_mode"],
        "examples": [
            "compara enero-febrero 2025 vs 2026",
            "primeros 2 meses de 2025 contra 2026",
            "ene-mar 2025 vs 2026",
            "cómo vamos vs el año pasado en los primeros 3 meses",
        ],
    },
    "month_day_window": {
        "description": "Comparar los primeros N días de un mes específico entre dos años.",
        "params": ["base_year", "target_year", "month", "days", "platform", "table_mode"],
        "examples": [
            "primeros 10 días de marzo 2025 vs 2026",
            "compara los primeros 15 días de enero entre 2025 y 2026",
            "cómo van los primeros 7 días de febrero vs el año pasado",
        ],
    },
    "top_piece": {
        "description": "Consulta sobre la pieza o campaña con mejor o peor rendimiento en una métrica.",
        "params": ["metric", "platform", "date_range_hint"],
        "examples": [
            "cuál fue la pieza con más conversiones",
            "top anuncios por gasto",
            "qué campaña tuvo mejor CTR en enero",
            "las 5 mejores piezas de Meta",
        ],
    },
    "peak_day": {
        "description": "Consulta sobre el día con el valor más alto o más bajo de una métrica.",
        "params": ["metric", "platform", "date_range_hint"],
        "examples": [
            "qué día tuvo más gasto en enero",
            "cuándo fue el pico de conversiones",
            "el día con mayor CPL",
            "qué fecha tuvo más clics",
        ],
    },
    "monthly_breakdown": {
        "description": "Desglose o resumen de métricas agrupado por mes.",
        "params": ["metric", "platform", "date_range_hint"],
        "examples": [
            "dame el desglose mensual",
            "cómo fue mes a mes el gasto",
            "resumen por mes de conversiones",
            "breakdown mensual de Meta",
        ],
    },
    "weekday_followup": {
        "description": "Pregunta sobre qué día de la semana fue una fecha específica mencionada antes.",
        "params": [],
        "examples": [
            "qué día de la semana fue eso",
            "era lunes o martes",
            "qué día de la semana cae esa fecha",
        ],
    },
    "comparison_followup_table": {
        "description": "Solicitud de reformatear o mostrar en tabla un resultado de comparación previo.",
        "params": [],
        "examples": [
            "ponlo en tabla",
            "muéstrame eso en tabla",
            "en formato cuadro",
            "dame eso como tabla",
        ],
    },
    "general_question": {
        "description": "Cualquier otra pregunta de análisis abierto, explicación, recomendación o que no encaje en las anteriores.",
        "params": [],
        "examples": [
            "por qué bajaron las conversiones",
            "qué recomendas hacer",
            "cómo mejorar el CTR",
            "explícame la diferencia entre CPL y CPC",
        ],
    },
}

_SYSTEM_PROMPT = """\
Eres un clasificador de intenciones para un asistente de analytics de marketing digital llamado COCO.
Tu única tarea es analizar la pregunta del usuario y devolver un JSON con la intención detectada.

INTENCIONES DISPONIBLES:
- year_period_comparison: Comparar los primeros N meses de un año vs otro.
  Parámetros: base_year (int), target_year (int), months (int 1-12)
  Ejemplos: "compara ene-feb 2025 vs 2026", "primeros 3 meses 2025 contra 2026", "cómo vamos vs año pasado en ene-mar"

- month_day_window: Comparar los primeros N días de un mes específico entre dos años.
  Parámetros: base_year (int), target_year (int), month (int 1-12), days (int 1-31)
  Ejemplos: "primeros 10 días de marzo 2025 vs 2026", "compara primeros 15 días de enero 2025 y 2026"

- top_piece: Pieza o campaña con mejor/peor rendimiento en una métrica.
  Parámetros: metric (str: spend/conv/ctr/cvr/cpc/cpm/cpl/clicks/impr), platform (str: Google/Meta/All), date_range_hint (str, puede ser vacío)
  Ejemplos: "cuál fue la pieza con más conversiones", "top campañas por gasto en enero", "mejores anuncios de Meta"

- peak_day: Día con el valor más alto o más bajo de una métrica.
  Parámetros: metric (str), platform (str: Google/Meta/All), date_range_hint (str, puede ser vacío)
  Ejemplos: "qué día tuvo más gasto", "cuándo fue el pico de conversiones", "el día con mayor CPL"

- monthly_breakdown: Desglose de métricas agrupado por mes.
  Parámetros: metric (str), platform (str: Google/Meta/All), date_range_hint (str, puede ser vacío)
  Ejemplos: "dame el desglose mensual", "cómo fue mes a mes el gasto", "resumen mensual de conversiones"

- weekday_followup: Pregunta sobre qué día de la semana fue una fecha del contexto anterior.
  Parámetros: ninguno (usa contexto previo)
  Ejemplos: "qué día de la semana fue eso", "era lunes o martes", "qué día de la semana cae"

- comparison_followup_table: Pide reformatear la respuesta anterior como tabla Markdown.
  Parámetros: ninguno (usa contexto previo)
  Ejemplos: "ponlo en tabla", "muéstrame eso en tabla", "en cuadro", "dame eso tabulado"

- general_question: Cualquier otra pregunta de análisis abierto, recomendación o explicación.
  Parámetros: ninguno
  Ejemplos: "por qué bajaron las conversiones", "qué recomendas", "cómo mejorar el CTR"

REGLAS:
1. Si la pregunta menciona "año pasado" sin año explícito, infiere base_year = año_actual - 1 y target_year = año_actual.
2. Para year_period_comparison: si menciona "enero y febrero" o "ene-feb" → months=2. "primer trimestre" o "Q1" → months=3. "primer semestre" → months=6.
3. Para month_day_window: extrae el número de días y el mes de la pregunta.
4. Si hay contexto previo y la pregunta es muy corta ("ponlo en tabla", "qué día fue"), verifica si encaja en weekday_followup o comparison_followup_table antes de usar general_question.
5. confidence debe reflejar qué tan seguro estás: 0.95+ si es muy claro, 0.80-0.94 si es claro pero con algo de ambigüedad, 0.70-0.79 si es incierto, <0.70 si no puedes determinar la intención.
6. Responde SIEMPRE en JSON válido con exactamente estos campos: intent, confidence, params, clarification_hint.
   - clarification_hint: string vacío si confidence >= 0.70, o una pregunta corta al usuario si confidence < 0.70.

FORMATO DE RESPUESTA (JSON):
{
  "intent": "nombre_del_intent",
  "confidence": 0.95,
  "params": {
    "base_year": 2025,
    "target_year": 2026,
    "months": 2
  },
  "clarification_hint": ""
}
"""


def _call_openai_json(
    *,
    api_key: str,
    model: str,
    system_prompt: str,
    user_message: str,
    timeout_seconds: int,
) -> tuple[dict[str, Any], str]:
    payload = {
        "model": model,
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    }
    req = urllib.request.Request(
        url="https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    last_error = ""
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=max(int(timeout_seconds), 10)) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw) if raw else {}
            if isinstance(parsed, dict):
                return parsed, ""
            last_error = "Respuesta no compatible del clasificador."
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            last_error = f"HTTP {exc.code}: {body[:200]}"
            if exc.code in {429, 500, 502, 503, 504} and attempt < 1:
                time.sleep(1.0)
                continue
            return {}, last_error
        except Exception as exc:
            last_error = f"Error clasificador: {exc}"
            if attempt < 1:
                time.sleep(1.0)
                continue
            return {}, last_error
    return {}, last_error or "Sin respuesta del clasificador."


def _extract_json_from_response(response: dict[str, Any]) -> dict[str, Any]:
    choices = response.get("choices", [])
    if not isinstance(choices, list) or not choices:
        return {}
    message = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
    content = message.get("content", "") if isinstance(message, dict) else ""
    if not isinstance(content, str) or not content.strip():
        return {}
    try:
        parsed = json.loads(content)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _validate_and_normalize(raw: dict[str, Any], current_year: int) -> dict[str, Any]:
    intent = str(raw.get("intent", "general_question")).strip()
    if intent not in INTENT_CATALOG:
        intent = "general_question"

    try:
        confidence = float(raw.get("confidence", 0.0))
        confidence = max(0.0, min(1.0, confidence))
    except Exception:
        confidence = 0.0

    params = raw.get("params", {})
    if not isinstance(params, dict):
        params = {}

    clarification_hint = str(raw.get("clarification_hint", "")).strip()

    # Normalizar parámetros según el intent
    if intent == "year_period_comparison":
        params = _normalize_year_period_params(params, current_year)
    elif intent == "month_day_window":
        params = _normalize_day_window_params(params, current_year)
    elif intent in {"top_piece", "peak_day", "monthly_breakdown"}:
        params = _normalize_metric_params(params)

    # Si faltan parámetros críticos, bajar la confianza
    missing = _check_missing_params(intent, params)
    if missing:
        confidence = min(confidence, 0.60)
        if not clarification_hint:
            clarification_hint = f"No pude determinar: {', '.join(missing)}. ¿Puedes aclararlo?"

    return {
        "intent": intent,
        "confidence": confidence,
        "params": params,
        "clarification_hint": clarification_hint,
    }


def _normalize_year_period_params(params: dict[str, Any], current_year: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        result["base_year"] = int(params.get("base_year", current_year - 1))
    except Exception:
        result["base_year"] = current_year - 1
    try:
        result["target_year"] = int(params.get("target_year", current_year))
    except Exception:
        result["target_year"] = current_year
    try:
        months = int(params.get("months", 0))
        result["months"] = max(1, min(12, months)) if months > 0 else 0
    except Exception:
        result["months"] = 0
    raw_platform = params.get("platform")
    if raw_platform is None or not str(raw_platform).strip():
        result["platform"] = ""
    else:
        result["platform"] = _normalize_platform_value(raw_platform, fallback="All")
    result["table_mode"] = _coerce_bool_flag(params.get("table_mode", False))
    return result


def _normalize_day_window_params(params: dict[str, Any], current_year: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        result["base_year"] = int(params.get("base_year", current_year - 1))
    except Exception:
        result["base_year"] = current_year - 1
    try:
        result["target_year"] = int(params.get("target_year", current_year))
    except Exception:
        result["target_year"] = current_year
    try:
        month = int(params.get("month", 0))
        result["month"] = max(1, min(12, month)) if month > 0 else 0
    except Exception:
        result["month"] = 0
    try:
        days = int(params.get("days", 0))
        result["days"] = max(1, min(31, days)) if days > 0 else 0
    except Exception:
        result["days"] = 0
    raw_platform = params.get("platform")
    if raw_platform is None or not str(raw_platform).strip():
        result["platform"] = ""
    else:
        result["platform"] = _normalize_platform_value(raw_platform, fallback="All")
    result["table_mode"] = _coerce_bool_flag(params.get("table_mode", False))
    return result


def _normalize_platform_value(value: Any, fallback: str = "All") -> str:
    raw = str(value or "").strip().lower()
    if raw in {"google", "google ads", "adwords", "gads"}:
        return "Google"
    if raw in {"meta", "facebook", "instagram", "fb"}:
        return "Meta"
    if raw in {"all", "total", "consolidado", "global", "ambas", "todos"}:
        return "All"
    if str(fallback or "").strip() in {"Google", "Meta", "All"}:
        return str(fallback).strip()
    return "All"


def _coerce_bool_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "si", "sí", "tabla", "table"}


def _normalize_metric_params(params: dict[str, Any]) -> dict[str, Any]:
    valid_metrics = {"spend", "conv", "ctr", "cvr", "cpc", "cpm", "cpl", "clicks", "impr", "sessions", "users", "avg_sess", "bounce"}
    metric = str(params.get("metric", "conv")).strip().lower()
    if metric not in valid_metrics:
        metric = "conv"
    platform = _normalize_platform_value(params.get("platform", "All"), fallback="All")
    return {
        "metric": metric,
        "platform": platform,
        "date_range_hint": str(params.get("date_range_hint", "")).strip(),
    }


def _check_missing_params(intent: str, params: dict[str, Any]) -> list[str]:
    required: dict[str, list[str]] = {
        "year_period_comparison": ["base_year", "target_year", "months"],
        "month_day_window": ["base_year", "target_year", "month", "days"],
    }
    missing: list[str] = []
    for field in required.get(intent, []):
        value = params.get(field)
        if value is None or value == 0:
            missing.append(field)
    return missing


def _build_user_message(question: str, last_context: dict[str, Any] | None, current_year: int) -> str:
    parts = [f"Año actual: {current_year}"]

    if isinstance(last_context, dict) and last_context:
        ctx_parts: list[str] = []
        resolver = str(last_context.get("resolver", "")).strip()
        if resolver:
            ctx_parts.append(f"resolver_anterior={resolver}")
        comparison_years = str(last_context.get("comparison_years", "")).strip()
        if comparison_years:
            ctx_parts.append(f"años_comparados={comparison_years}")
        peak_day = str(last_context.get("peak_day", "")).strip()
        if peak_day:
            ctx_parts.append(f"fecha_pico={peak_day}")
        metric_key = str(last_context.get("metric_key", "")).strip()
        if metric_key:
            ctx_parts.append(f"metrica_anterior={metric_key}")
        if ctx_parts:
            parts.append("Contexto de la respuesta anterior: " + ", ".join(ctx_parts))

    parts.append(f"Pregunta del usuario: {str(question or '').strip()}")
    return "\n".join(parts)


def _fallback_result() -> dict[str, Any]:
    return {
        "intent": "general_question",
        "confidence": 0.0,
        "params": {},
        "clarification_hint": "",
    }


def classify_intent(
    *,
    api_key: str,
    model: str,
    question: str,
    last_context: dict[str, Any] | None = None,
    current_year: int | None = None,
    timeout_seconds: int = 15,
) -> dict[str, Any]:
    """Clasifica la intención de la pregunta del usuario.

    Retorna siempre un dict con:
        intent (str): uno de los 8 intents definidos
        confidence (float): 0.0 a 1.0
        params (dict): parámetros extraídos según el intent
        clarification_hint (str): pregunta de aclaración si confidence < threshold
    """
    if not str(api_key or "").strip():
        return _fallback_result()
    if not str(question or "").strip():
        return _fallback_result()

    from datetime import date
    _current_year = int(current_year) if current_year else date.today().year

    user_message = _build_user_message(question, last_context, _current_year)

    response, err = _call_openai_json(
        api_key=api_key,
        model=model,
        system_prompt=_SYSTEM_PROMPT,
        user_message=user_message,
        timeout_seconds=timeout_seconds,
    )
    if err or not response:
        return _fallback_result()

    raw_json = _extract_json_from_response(response)
    if not raw_json:
        return _fallback_result()

    return _validate_and_normalize(raw_json, _current_year)
