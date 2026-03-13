"""Capa de despacho entre el intent classifier y los resolvers deterministas.

En lugar de duplicar la lógica de cálculo, construye preguntas sintéticas
controladas que los resolvers existentes parsean de forma 100% determinista.
Los parámetros llegan ya extraídos y validados por el intent classifier.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from . import deterministic_resolvers as det

# Alias para legibilidad
_MONTH_NAMES = det.MONTH_NAMES_ES
_METRIC_PHRASES: dict[str, str] = {
    "spend": "gasto",
    "conv": "conversiones",
    "cpl": "cpl",
    "ctr": "ctr",
    "cvr": "cvr",
    "cpc": "cpc",
    "cpm": "cpm",
    "clicks": "clics",
    "impr": "impresiones",
    "sessions": "sesiones",
    "users": "usuarios",
    "avg_sess": "duracion",
    "bounce": "rebote",
}


def _platform_suffix(platform: str, platforms: list[str] | None = None) -> str:
    """Genera el sufijo de plataforma para la pregunta sintética."""
    effective = platform
    if platforms:
        if len(platforms) > 1:
            effective = "All"
        elif platforms[0] in {"Meta", "Google", "All"}:
            effective = platforms[0]
    if effective == "All":
        return " consolidado total no por plataforma"
    if effective in {"Meta", "Google"}:
        return f" de {effective}"
    return ""


def compute_year_period_comparison(
    *,
    df_base: pd.DataFrame,
    base_year: int,
    target_year: int,
    months: int,
    selected_platform: str,
    platforms: list[str] | None = None,
    table_mode: bool = False,
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Comparativa primeros N meses año base vs año objetivo."""
    months = max(1, min(12, int(months)))
    synthetic = f"compara {base_year} vs {target_year} primeros {months} meses"
    if table_mode:
        synthetic += " en tabla"
    synthetic += _platform_suffix(selected_platform, platforms)
    return det._try_resolve_year_period_comparison_question(
        question=synthetic,
        df_base=df_base,
        selected_platform=selected_platform,
        include_actions=include_actions,
    )


def compute_month_day_window(
    *,
    df_base: pd.DataFrame,
    base_year: int,
    target_year: int,
    month: int,
    days: int,
    selected_platform: str,
    platforms: list[str] | None = None,
    table_mode: bool = False,
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Comparativa primeros N días de un mes entre dos años."""
    month = max(1, min(12, int(month)))
    days = max(1, min(31, int(days)))
    month_name = _MONTH_NAMES.get(month, str(month))
    synthetic = f"compara primeros {days} dias de {month_name} {base_year} vs {target_year}"
    if table_mode:
        synthetic += " en tabla"
    synthetic += _platform_suffix(selected_platform, platforms)
    return det._try_resolve_month_day_window_comparison_question(
        question=synthetic,
        df_base=df_base,
        selected_platform=selected_platform,
        include_actions=include_actions,
    )


def compute_monthly_breakdown(
    *,
    df_base: pd.DataFrame,
    metric: str,
    selected_platform: str,
    ui_start: date,
    ui_end: date,
    date_range_hint: str = "",
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Desglose mensual de una métrica en el periodo activo."""
    phrase = _METRIC_PHRASES.get(metric, metric)
    synthetic = f"dame el desglose mensual de {phrase}"
    if selected_platform in {"Meta", "Google"}:
        synthetic += f" de {selected_platform}"
    # Si hay hint de rango, incorporarlo para que _resolve_question_range lo pique
    if date_range_hint:
        synthetic += f" {date_range_hint}"
    return det._try_resolve_monthly_breakdown_question(
        question=synthetic,
        df_base=df_base,
        selected_platform=selected_platform,
        ui_start=ui_start,
        ui_end=ui_end,
        include_actions=include_actions,
    )


def compute_peak_day(
    *,
    df_base: pd.DataFrame,
    metric: str,
    selected_platform: str,
    ui_start: date,
    ui_end: date,
    date_range_hint: str = "",
    include_actions: bool = True,
    include_platform_breakdown: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Día con el valor más alto de una métrica."""
    phrase = _METRIC_PHRASES.get(metric, metric)
    synthetic = f"qué dia tuvo mas {phrase}"
    if selected_platform in {"Meta", "Google"}:
        synthetic += f" de {selected_platform}"
    if date_range_hint:
        synthetic += f" en {date_range_hint}"
    return det._try_resolve_peak_day_question(
        question=synthetic,
        df_base=df_base,
        selected_platform=selected_platform,
        ui_start=ui_start,
        ui_end=ui_end,
        include_actions=include_actions,
        include_platform_breakdown=include_platform_breakdown,
    )


def compute_top_piece(
    *,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    selected_platform: str,
    metric: str = "conv",
    ui_start: date | None = None,
    ui_end: date | None = None,
    date_range_hint: str = "",
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Pieza o campaña con mejor rendimiento en una métrica."""
    phrase = _METRIC_PHRASES.get(metric, metric)
    synthetic = f"cual fue la pieza con mas {phrase}"
    if selected_platform in {"Meta", "Google"}:
        synthetic += f" de {selected_platform}"
    if date_range_hint:
        synthetic += f" en {date_range_hint}"
    # ui_start/ui_end deben ser date; si no llegan, se usan los datos máximos disponibles
    fallback_start = ui_start or date(2000, 1, 1)
    fallback_end = ui_end or date.today()
    return det._try_resolve_top_piece_question(
        question=synthetic,
        camp_df=camp_df,
        piece_df=piece_df,
        selected_platform=selected_platform,
        ui_start=fallback_start,
        ui_end=fallback_end,
        include_actions=include_actions,
    )


def compute_comparison_followup_table(
    *,
    question: str,
    last_context: dict[str, Any],
    df_base: pd.DataFrame,
    selected_platform: str,
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Reformatea como tabla el comparativo de la respuesta anterior."""
    resolver = str(last_context.get("resolver", "")).strip().lower()

    # Determinar qué tipo de comparativa fue la anterior
    if "month_day_window" in resolver:
        return det._try_resolve_month_day_window_comparison_followup_question(
            question=question,
            last_context=last_context,
            df_base=df_base,
            selected_platform=selected_platform,
            include_actions=include_actions,
        )
    if "explicit_range_year_comparison" in resolver:
        return det._try_resolve_explicit_range_year_comparison_followup_question(
            question=question,
            last_context=last_context,
            df_base=df_base,
            selected_platform=selected_platform,
            include_actions=include_actions,
        )

    # Por defecto intenta year_period
    return det._try_resolve_year_period_comparison_followup_question(
        question=question,
        last_context=last_context,
        df_base=df_base,
        selected_platform=selected_platform,
        include_actions=include_actions,
    )


def compute_weekday_followup(
    *,
    question: str,
    last_context: dict[str, Any],
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Informa qué día de la semana fue la fecha del contexto previo."""
    return det._try_resolve_weekday_followup_question(
        question=question,
        last_context=last_context,
        include_actions=include_actions,
    )


def compute_top_piece_followup(
    *,
    question: str,
    last_context: dict[str, Any],
    include_actions: bool = True,
) -> tuple[str, dict[str, Any]]:
    """Responde preguntas de follow-up sobre el rango de la top piece anterior."""
    return det._try_resolve_top_piece_period_followup_question(
        question=question,
        last_context=last_context,
        include_actions=include_actions,
    )
