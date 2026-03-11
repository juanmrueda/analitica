from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from . import deterministic_resolvers as coco_det


def run_deterministic_resolver_chain(
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
    last_context: dict[str, Any] | None,
) -> tuple[str, str, dict[str, Any], str]:
    context_payload = last_context if isinstance(last_context, dict) else {}
    deterministic_answer, deterministic_meta = coco_det._try_resolve_top_piece_question(
        question=question,
        camp_df=camp_df,
        piece_df=piece_df,
        selected_platform=query_platform,
        ui_start=default_start,
        ui_end=default_end,
        include_actions=include_actions,
    )
    if deterministic_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{deterministic_meta.get('range_start')}` a `{deterministic_meta.get('range_end')}`, "
            f"plataformas `{deterministic_meta.get('platforms', query_platform)}`, "
            f"resolver `{deterministic_meta.get('resolver', 'deterministic')}`."
        )
        return deterministic_answer, "deterministic-top-piece", deterministic_meta, note

    followup_answer, followup_meta = coco_det._try_resolve_top_piece_period_followup_question(
        question=question,
        last_context=context_payload,
        include_actions=include_actions,
    )
    if followup_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{followup_meta.get('range_start')}` a `{followup_meta.get('range_end')}`, "
            f"resolver `{followup_meta.get('resolver', 'deterministic')}`, "
            f"fuente `{followup_meta.get('source', 'contexto_previo')}`."
        )
        return followup_answer, "deterministic-top-piece-followup-range", followup_meta, note

    comparison_answer, comparison_meta = coco_det._try_resolve_year_period_comparison_question(
        question=question,
        df_base=df_base,
        selected_platform=query_platform,
        include_actions=include_actions,
    )
    if comparison_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{comparison_meta.get('range_start')}` a `{comparison_meta.get('range_end')}`, "
            f"plataforma `{comparison_meta.get('applied_platform', query_platform)}`, "
            f"resolver `{comparison_meta.get('resolver', 'deterministic')}`."
        )
        return comparison_answer, "deterministic-year-period-comparison", comparison_meta, note

    day_window_answer, day_window_meta = coco_det._try_resolve_month_day_window_comparison_question(
        question=question,
        df_base=df_base,
        selected_platform=query_platform,
        include_actions=include_actions,
    )
    if day_window_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{day_window_meta.get('range_start')}` a `{day_window_meta.get('range_end')}`, "
            f"plataforma `{day_window_meta.get('applied_platform', query_platform)}`, "
            f"resolver `{day_window_meta.get('resolver', 'deterministic')}`."
        )
        return day_window_answer, "deterministic-month-day-window-comparison", day_window_meta, note

    comparison_followup_answer, comparison_followup_meta = (
        coco_det._try_resolve_year_period_comparison_followup_question(
            question=question,
            last_context=context_payload,
            df_base=df_base,
            selected_platform=query_platform,
            include_actions=include_actions,
        )
    )
    if comparison_followup_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{comparison_followup_meta.get('range_start')}` a `{comparison_followup_meta.get('range_end')}`, "
            f"plataforma `{comparison_followup_meta.get('applied_platform', query_platform)}`, "
            f"resolver `{comparison_followup_meta.get('resolver', 'deterministic')}`, "
            f"fuente `{comparison_followup_meta.get('source', 'contexto_previo')}`."
        )
        return (
            comparison_followup_answer,
            "deterministic-year-period-comparison-followup-table",
            comparison_followup_meta,
            note,
        )

    day_window_followup_answer, day_window_followup_meta = (
        coco_det._try_resolve_month_day_window_comparison_followup_question(
            question=question,
            last_context=context_payload,
            df_base=df_base,
            selected_platform=query_platform,
            include_actions=include_actions,
        )
    )
    if day_window_followup_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{day_window_followup_meta.get('range_start')}` a `{day_window_followup_meta.get('range_end')}`, "
            f"plataforma `{day_window_followup_meta.get('applied_platform', query_platform)}`, "
            f"resolver `{day_window_followup_meta.get('resolver', 'deterministic')}`, "
            f"fuente `{day_window_followup_meta.get('source', 'contexto_previo')}`."
        )
        return (
            day_window_followup_answer,
            "deterministic-month-day-window-comparison-followup-table",
            day_window_followup_meta,
            note,
        )

    monthly_answer, monthly_meta = coco_det._try_resolve_monthly_breakdown_question(
        question=question,
        df_base=df_base,
        selected_platform=query_platform,
        ui_start=default_start,
        ui_end=default_end,
        include_actions=include_actions,
    )
    if monthly_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{monthly_meta.get('range_start')}` a `{monthly_meta.get('range_end')}`, "
            f"plataforma `{monthly_meta.get('applied_platform', query_platform)}`, "
            f"resolver `{monthly_meta.get('resolver', 'deterministic')}`."
        )
        return monthly_answer, "deterministic-monthly-breakdown", monthly_meta, note

    peak_answer, peak_meta = coco_det._try_resolve_peak_day_question(
        question=question,
        df_base=df_base,
        selected_platform=query_platform,
        ui_start=default_start,
        ui_end=default_end,
        include_actions=include_actions,
        include_platform_breakdown=include_platform_breakdown,
    )
    if peak_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, rango "
            f"`{peak_meta.get('range_start')}` a `{peak_meta.get('range_end')}`, "
            f"plataforma `{peak_meta.get('applied_platform', query_platform)}`, "
            f"resolver `{peak_meta.get('resolver', 'deterministic')}`."
        )
        return peak_answer, "deterministic-peak", peak_meta, note

    weekday_answer, weekday_meta = coco_det._try_resolve_weekday_followup_question(
        question=question,
        last_context=context_payload,
        include_actions=include_actions,
    )
    if weekday_answer:
        note = (
            f"Contexto aplicado: tenant `{tenant_id}`, fecha `{weekday_meta.get('target_date')}`, "
            f"resolver `{weekday_meta.get('resolver', 'deterministic')}`, "
            f"fuente `{weekday_meta.get('source', 'pregunta')}`."
        )
        return weekday_answer, "deterministic-weekday", weekday_meta, note

    return "", "deterministic", {}, ""
