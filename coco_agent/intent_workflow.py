"""Orquestador del flujo basado en intent classifier.

Reemplaza la cadena if/elif de workflow.py con un flujo en dos pasos:
  1. LLM clasifica la intención y extrae parámetros (liviano, ~400 tokens)
  2. Resolver determinista computa con parámetros exactos

Retorna la misma firma que run_deterministic_resolver_chain para ser
intercambiable sin modificar dashboard.py.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from .intent_classifier import CONFIDENCE_THRESHOLD, classify_intent
from . import compute_resolvers as cr
from . import deterministic_resolvers as det


# --------------------------------------------------------------------------- #
# Helpers internos
# --------------------------------------------------------------------------- #

def _clarification_response(intent_result: dict[str, Any]) -> str:
    hint = str(intent_result.get("clarification_hint", "")).strip()
    if hint:
        return hint
    intent = str(intent_result.get("intent", "")).strip()
    _labels = {
        "year_period_comparison": "una comparativa entre años",
        "month_day_window": "una comparativa de días de mes entre años",
        "top_piece": "las mejores piezas o campañas",
        "peak_day": "el día con mayor rendimiento",
        "monthly_breakdown": "un desglose mensual",
    }
    label = _labels.get(intent, "tu consulta")
    return (
        f"Entendí que quieres ver {label}, pero me falta información para calcularlo con precisión. "
        "¿Puedes especificar el rango de fechas o las plataformas que quieres analizar?"
    )


def _build_note(tenant_id: str, meta: dict[str, Any], query_platform: str) -> str:
    resolver = meta.get("resolver", "intent-dispatch")
    range_start = meta.get("range_start", "")
    range_end = meta.get("range_end", "")
    applied = meta.get("applied_platform", query_platform)
    parts = [f"tenant `{tenant_id}`"]
    if range_start and range_end:
        parts.append(f"rango `{range_start}` a `{range_end}`")
    if applied:
        parts.append(f"plataforma `{applied}`")
    parts.append(f"resolver `{resolver}`")
    return "Contexto aplicado: " + ", ".join(parts) + "."


def _normalize_platform(value: Any, fallback: str = "All") -> str:
    raw = str(value or "").strip().lower()
    if raw in {"google", "google ads", "adwords", "gads"}:
        return "Google"
    if raw in {"meta", "facebook", "instagram", "fb"}:
        return "Meta"
    if raw in {"all", "total", "consolidado", "global", "ambas", "todos"}:
        return "All"
    if str(fallback or "").strip() == "":
        return ""
    if str(fallback or "").strip() in {"Google", "Meta", "All"}:
        return str(fallback).strip()
    return "All"


def _resolve_comparison_scope(
    *,
    question: str,
    params: dict[str, Any],
    query_platform: str,
) -> tuple[str, list[str] | None]:
    explicit_platform = _normalize_platform(params.get("platform", ""), fallback="")
    selected = explicit_platform or _normalize_platform(query_platform, fallback="All")

    if det._question_requests_consolidated_comparison(question):
        return "All", ["All"]
    if det._question_mentions_both_major_platforms(question):
        return "All", ["Meta", "Google"]
    if explicit_platform in {"Meta", "Google"}:
        return explicit_platform, [explicit_platform]
    if selected in {"Meta", "Google"}:
        return selected, [selected]
    return "All", ["All"]


def _resolve_table_mode(question: str, params: dict[str, Any]) -> bool:
    if det._question_requests_table(question):
        return True
    return bool(params.get("table_mode", False))


# --------------------------------------------------------------------------- #
# Despachador por intent
# --------------------------------------------------------------------------- #

def _dispatch(
    intent: str,
    params: dict[str, Any],
    *,
    question: str,
    tenant_id: str,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    df_base: pd.DataFrame,
    query_platform: str,
    default_start: date,
    default_end: date,
    include_actions: bool,
    include_platform_breakdown: bool,
    last_context: dict[str, Any],
) -> tuple[str, str, dict[str, Any], str]:
    """Despacha al compute_resolver correcto según el intent."""

    answer, meta = "", {}

    if intent == "year_period_comparison":
        selected_platform, platforms = _resolve_comparison_scope(
            question=question,
            params=params,
            query_platform=query_platform,
        )
        answer, meta = cr.compute_year_period_comparison(
            df_base=df_base,
            base_year=int(params.get("base_year", date.today().year - 1)),
            target_year=int(params.get("target_year", date.today().year)),
            months=int(params.get("months", 1)),
            selected_platform=selected_platform,
            platforms=platforms,
            table_mode=_resolve_table_mode(question, params),
            include_actions=include_actions,
        )

    elif intent == "month_day_window":
        selected_platform, platforms = _resolve_comparison_scope(
            question=question,
            params=params,
            query_platform=query_platform,
        )
        answer, meta = cr.compute_month_day_window(
            df_base=df_base,
            base_year=int(params.get("base_year", date.today().year - 1)),
            target_year=int(params.get("target_year", date.today().year)),
            month=int(params.get("month", 1)),
            days=int(params.get("days", 10)),
            selected_platform=selected_platform,
            platforms=platforms,
            table_mode=_resolve_table_mode(question, params),
            include_actions=include_actions,
        )

    elif intent == "top_piece":
        selected_platform = _normalize_platform(
            params.get("platform", query_platform),
            fallback=_normalize_platform(query_platform, fallback="All"),
        )
        answer, meta = cr.compute_top_piece(
            camp_df=camp_df,
            piece_df=piece_df,
            selected_platform=selected_platform,
            metric=str(params.get("metric", "conv")),
            ui_start=default_start,
            ui_end=default_end,
            date_range_hint=str(params.get("date_range_hint", "")),
            include_actions=include_actions,
        )

    elif intent == "peak_day":
        selected_platform = _normalize_platform(
            params.get("platform", query_platform),
            fallback=_normalize_platform(query_platform, fallback="All"),
        )
        answer, meta = cr.compute_peak_day(
            df_base=df_base,
            metric=str(params.get("metric", "conv")),
            selected_platform=selected_platform,
            ui_start=default_start,
            ui_end=default_end,
            date_range_hint=str(params.get("date_range_hint", "")),
            include_actions=include_actions,
            include_platform_breakdown=include_platform_breakdown,
        )

    elif intent == "monthly_breakdown":
        selected_platform = _normalize_platform(
            params.get("platform", query_platform),
            fallback=_normalize_platform(query_platform, fallback="All"),
        )
        answer, meta = cr.compute_monthly_breakdown(
            df_base=df_base,
            metric=str(params.get("metric", "spend")),
            selected_platform=selected_platform,
            ui_start=default_start,
            ui_end=default_end,
            date_range_hint=str(params.get("date_range_hint", "")),
            include_actions=include_actions,
        )

    elif intent == "comparison_followup_table":
        answer, meta = cr.compute_comparison_followup_table(
            question=question,
            last_context=last_context,
            df_base=df_base,
            selected_platform=query_platform,
            include_actions=include_actions,
        )

    elif intent == "weekday_followup":
        answer, meta = cr.compute_weekday_followup(
            question=question,
            last_context=last_context,
            include_actions=include_actions,
        )

    # Si el resolver no devolvió nada (sin datos, parámetros inválidos, etc.)
    # retornamos "" para que dashboard escale a OpenAI completo
    if not answer:
        return "", "llm_required", {}, ""

    note = _build_note(tenant_id, meta, query_platform)
    resolver_tag = f"intent-{intent}"
    return answer, resolver_tag, meta, note


# --------------------------------------------------------------------------- #
# Función principal
# --------------------------------------------------------------------------- #

def run_intent_resolver_chain(
    *,
    api_key: str,
    model: str,
    question: str,
    tenant_id: str,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    df_base: pd.DataFrame,
    query_platform: str,
    default_start: date,
    default_end: date,
    include_actions: bool,
    include_platform_breakdown: bool,
    last_context: dict[str, Any] | None,
) -> tuple[str, str, dict[str, Any], str]:
    """Flujo de resolución basado en intent classifier.

    Retorna: (answer, model_used, meta, note)
    - Si answer == "" → dashboard debe escalar a OpenAI completo.
    - model_used identifica el resolver usado para logging.
    """
    ctx = last_context if isinstance(last_context, dict) else {}

    # Paso 1: clasificar intención (llamada LLM liviana)
    intent_result = classify_intent(
        api_key=api_key,
        model=model,
        question=question,
        last_context=ctx,
        current_year=default_end.year,
    )

    intent = str(intent_result.get("intent", "general_question")).strip()
    confidence = float(intent_result.get("confidence", 0.0))
    params = intent_result.get("params", {})
    if not isinstance(params, dict):
        params = {}

    # Guardrail: si la pregunta trae un rango expl?cito del tipo
    # "del 1 de enero al 28 de febrero de 2025 ... 2026",
    # priorizamos ese resolver determinista para evitar clasificaciones
    # ambiguas (por ejemplo, month_day_window) con c?lculos parciales.
    if det._is_explicit_range_year_comparison_question(question):
        selected_platform, _ = _resolve_comparison_scope(
            question=question,
            params=params,
            query_platform=query_platform,
        )
        explicit_answer, explicit_meta = det._try_resolve_explicit_range_year_comparison_question(
            question=question,
            df_base=df_base,
            selected_platform=selected_platform,
            include_actions=include_actions,
        )
        if explicit_answer:
            note = _build_note(tenant_id, explicit_meta, query_platform)
            return (
                explicit_answer,
                "intent-explicit_range_year_comparison",
                explicit_meta,
                note,
            )

    # Guardrail: detect explicit month/day windows between years
    # (e.g. "los 10 primeros dias de marzo 2025 y 2026")
    # so they do not fall through to OpenAI due intent ambiguity.
    if det._is_month_day_window_comparison_question(question):
        selected_platform, _ = _resolve_comparison_scope(
            question=question,
            params=params,
            query_platform=query_platform,
        )
        day_window_answer, day_window_meta = det._try_resolve_month_day_window_comparison_question(
            question=question,
            df_base=df_base,
            selected_platform=selected_platform,
            include_actions=include_actions,
        )
        if day_window_answer:
            note = _build_note(tenant_id, day_window_meta, query_platform)
            return (
                day_window_answer,
                "intent-month_day_window_guardrail",
                day_window_meta,
                note,
            )

    # Paso 2a: preguntas abiertas ? escalar a OpenAI completo
    if intent == "general_question":
        return "", "llm_required", {}, ""

    # Paso 2b: confianza insuficiente → pedir aclaración al usuario
    if confidence < CONFIDENCE_THRESHOLD:
        clarification = _clarification_response(intent_result)
        meta = {
            "resolver": "intent-clarification",
            "original_intent": intent,
            "confidence": str(round(confidence, 2)),
        }
        return clarification, "intent-clarification", meta, ""

    # Paso 3: despachar al compute_resolver correspondiente
    return _dispatch(
        intent=intent,
        params=params,
        question=question,
        tenant_id=tenant_id,
        camp_df=camp_df,
        piece_df=piece_df,
        df_base=df_base,
        query_platform=query_platform,
        default_start=default_start,
        default_end=default_end,
        include_actions=include_actions,
        include_platform_breakdown=include_platform_breakdown,
        last_context=ctx,
    )
