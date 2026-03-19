from __future__ import annotations

import calendar
import html
from datetime import date, timedelta
from typing import Any, Callable

import pandas as pd
import streamlit as st

NormalizePlatformOptionFn = Callable[[Any], str]
NormalizeCampaignFilterKeysFn = Callable[[Any, list[str]], list[str]]
CampaignFilterValuesFn = Callable[..., list[str]]
CachedCampaignFilterValuesFn = Callable[[str, int, int, str, str, str, str], list[str]]


def _coerce_date_value(value: Any, min_d: date, max_d: date) -> date:
    if hasattr(value, "to_pydatetime"):
        try:
            value = value.to_pydatetime().date()
        except Exception:
            pass
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
            start_day = _coerce_date_value(vals[0], min_d, max_d)
            end_day = _coerce_date_value(vals[1], min_d, max_d)
        elif len(vals) == 1:
            start_day = end_day = _coerce_date_value(vals[0], min_d, max_d)
        else:
            start_day = end_day = max_d
    else:
        start_day = end_day = _coerce_date_value(sel, min_d, max_d)
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    return start_day, end_day


def _default_business_date_range(min_d: date, max_d: date) -> tuple[date, date]:
    today = date.today()
    if today.day == 1:
        end_candidate = today - timedelta(days=1)
        start_candidate = end_candidate.replace(day=1)
    else:
        start_candidate = today.replace(day=1)
        end_candidate = today - timedelta(days=1)

    start_day = _coerce_date_value(start_candidate, min_d, max_d)
    end_day = _coerce_date_value(end_candidate, min_d, max_d)
    if start_day > end_day:
        start_day = min_d
    return start_day, end_day


def _safe_shift_year(base_day: date, years: int) -> date:
    target_year = int(base_day.year) + int(years)
    target_month = int(base_day.month)
    target_day = int(base_day.day)
    last_day = calendar.monthrange(target_year, target_month)[1]
    safe_day = min(target_day, last_day)
    return date(target_year, target_month, safe_day)


def _week_start_sunday(base_day: date) -> date:
    days_since_sunday = (base_day.weekday() + 1) % 7
    return base_day - timedelta(days=days_since_sunday)


def _resolve_date_preset_range(
    preset: str,
    min_d: date,
    max_d: date,
    *,
    date_preset_all_options: tuple[str, ...],
) -> tuple[date, date]:
    normalized = str(preset or "").strip().lower()
    if normalized not in date_preset_all_options:
        normalized = "custom"
    anchor = _coerce_date_value(date.today(), min_d, max_d)

    if normalized == "today":
        start_day, end_day = anchor, anchor
    elif normalized == "yesterday":
        end_day = _coerce_date_value(anchor - timedelta(days=1), min_d, max_d)
        start_day = end_day
    elif normalized == "this_week_to_date":
        start_day = _coerce_date_value(_week_start_sunday(anchor), min_d, max_d)
        end_day = anchor
    elif normalized == "last_7_days":
        end_day = anchor
        start_day = _coerce_date_value(end_day - timedelta(days=6), min_d, max_d)
    elif normalized == "last_week":
        this_week_start = _week_start_sunday(anchor)
        end_day = _coerce_date_value(this_week_start - timedelta(days=1), min_d, max_d)
        start_day = _coerce_date_value(end_day - timedelta(days=6), min_d, max_d)
    elif normalized == "last_28_days":
        end_day = anchor
        start_day = _coerce_date_value(end_day - timedelta(days=27), min_d, max_d)
    elif normalized == "last_30_days":
        end_day = anchor
        start_day = _coerce_date_value(end_day - timedelta(days=29), min_d, max_d)
    elif normalized == "this_month_to_date":
        start_day = _coerce_date_value(anchor.replace(day=1), min_d, max_d)
        end_day = anchor
    elif normalized == "last_month":
        current_month_start = anchor.replace(day=1)
        prev_month_end = current_month_start - timedelta(days=1)
        start_day = _coerce_date_value(prev_month_end.replace(day=1), min_d, max_d)
        end_day = _coerce_date_value(prev_month_end, min_d, max_d)
    elif normalized == "last_90_days":
        end_day = anchor
        start_day = _coerce_date_value(end_day - timedelta(days=89), min_d, max_d)
    elif normalized == "quarter_to_date":
        quarter_start_month = ((anchor.month - 1) // 3) * 3 + 1
        start_day = _coerce_date_value(date(anchor.year, quarter_start_month, 1), min_d, max_d)
        end_day = anchor
    elif normalized == "year_to_date":
        start_day = _coerce_date_value(date(anchor.year, 1, 1), min_d, max_d)
        end_day = anchor
    elif normalized == "last_calendar_year":
        prev_year = anchor.year - 1
        start_day = _coerce_date_value(date(prev_year, 1, 1), min_d, max_d)
        end_day = _coerce_date_value(date(prev_year, 12, 31), min_d, max_d)
    else:
        return _default_business_date_range(min_d, max_d)

    return _normalize_date_range((start_day, end_day), min_d, max_d)


def _resolve_compare_range(
    *,
    mode: str,
    current_start: date,
    current_end: date,
    data_min: date,
    data_max: date,
    custom_range: Any = None,
    compare_mode_options: tuple[str, ...],
) -> tuple[date, date, str]:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in compare_mode_options:
        normalized_mode = "previous_period"

    if normalized_mode == "custom":
        custom_start, custom_end = _normalize_date_range(custom_range, data_min, data_max)
        label = f"{custom_start.isoformat()} - {custom_end.isoformat()} (personalizado)"
        return custom_start, custom_end, label

    if normalized_mode == "year_over_year":
        compare_start_raw = _safe_shift_year(current_start, -1)
        compare_end_raw = _safe_shift_year(current_end, -1)
        compare_start = _coerce_date_value(compare_start_raw, data_min, data_max)
        compare_end = _coerce_date_value(compare_end_raw, data_min, data_max)
        if compare_start > compare_end:
            compare_start, compare_end = compare_end, compare_start
        label = f"{compare_start.isoformat()} - {compare_end.isoformat()} (año anterior)"
        return compare_start, compare_end, label

    span_days = max((current_end - current_start).days + 1, 1)
    compare_end_raw = current_start - timedelta(days=1)
    compare_start_raw = compare_end_raw - timedelta(days=span_days - 1)
    compare_start = _coerce_date_value(compare_start_raw, data_min, data_max)
    compare_end = _coerce_date_value(compare_end_raw, data_min, data_max)
    if compare_start > compare_end:
        compare_start, compare_end = compare_end, compare_start
    label = f"{compare_start.isoformat()} - {compare_end.isoformat()} (periodo anterior)"
    return compare_start, compare_end, label


def render_top_filters(
    min_d: date,
    max_d: date,
    tenant_name: str,
    tenant_id: str,
    default_platform: str,
    tenant_logo_source: str,
    camp_df: pd.DataFrame,
    campaign_filter_keys: list[str],
    *,
    report_cache_sig: tuple[str, int, int] | None,
    platform_options: tuple[str, ...],
    date_preset_options: tuple[str, ...],
    date_preset_labels: dict[str, str],
    date_preset_all_options: tuple[str, ...],
    compare_mode_options: tuple[str, ...],
    compare_mode_labels: dict[str, str],
    campaign_filter_options: dict[str, str],
    normalize_platform_option: NormalizePlatformOptionFn,
    normalize_campaign_filter_keys: NormalizeCampaignFilterKeysFn,
    campaign_filter_values: CampaignFilterValuesFn,
    cached_campaign_filter_values_from_report: CachedCampaignFilterValuesFn,
) -> tuple[date, date, str, dict[str, str], str, date, date, str]:
    _ = tenant_logo_source
    _ = compare_mode_labels
    default_platform_value = normalize_platform_option(default_platform)
    last_tenant = str(st.session_state.get("platform_filter_tenant_id", ""))
    if last_tenant != tenant_id or st.session_state.get("platform_filter_radio_v2") not in platform_options:
        st.session_state["platform_filter_radio_v2"] = default_platform_value
        st.session_state["platform_filter_tenant_id"] = tenant_id
    wrapper_left, wrapper_right = st.columns([2.05, 1.95], gap="large")
    with wrapper_left:
        st.markdown(
            f"""
            <div class='hero'>
              <div class='hero-kicker'>iPalmera IA Analitica</div>
              <div class='hero-sub'><span class='hero-tenant-name'>{html.escape(tenant_name)}</span> Marketing Performance</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with wrapper_right:
        pcol, dcol = st.columns([1.72, 1.28], gap="small")
        with pcol:
            platform = st.radio(
                "Plataforma",
                list(platform_options),
                key="platform_filter_radio_v2",
                horizontal=True,
                label_visibility="collapsed",
            )
            if platform not in platform_options:
                platform = default_platform_value
        with dcol:
            st.markdown("<div class='app-filter-title app-filter-title-range'>Rango</div>", unsafe_allow_html=True)
            range_value_key = f"top_date_range_{tenant_id}"
            range_preset_key = f"top_date_preset_{tenant_id}"
            range_draft_key = f"{range_value_key}_draft"
            range_preset_draft_key = f"{range_preset_key}_draft"
            range_preset_draft_last_key = f"{range_preset_draft_key}_last"
            compare_enabled_key = f"top_compare_enabled_{tenant_id}"
            compare_enabled_draft_key = f"{compare_enabled_key}_draft"
            compare_mode_key = f"top_compare_mode_{tenant_id}"
            compare_mode_draft_key = f"{compare_mode_key}_draft"
            compare_custom_key = f"top_compare_custom_range_{tenant_id}"
            compare_custom_draft_key = f"{compare_custom_key}_draft"
            # Avoid Streamlit-reserved suffixes (__start_input/__end_input) used by range date_input widgets.
            range_draft_start_input_key = f"{range_draft_key}__custom_start_input"
            range_draft_end_input_key = f"{range_draft_key}__custom_end_input"
            compare_custom_start_input_key = f"{compare_custom_draft_key}__custom_start_input"
            compare_custom_end_input_key = f"{compare_custom_draft_key}__custom_end_input"
            draft_restore_key = f"{range_value_key}_draft_restore"
            range_popover_mount_version_key = f"{range_value_key}_popover_mount_version"
            legacy_range_draft_start_key = f"{range_draft_key}_start"
            legacy_range_draft_end_key = f"{range_draft_key}_end"
            legacy_compare_draft_start_key = f"{compare_custom_draft_key}_start"
            legacy_compare_draft_end_key = f"{compare_custom_draft_key}_end"

            default_start, default_end = _default_business_date_range(min_d, max_d)
            if range_value_key not in st.session_state:
                st.session_state[range_value_key] = (default_start, default_end)
            if st.session_state.get(range_preset_key) not in date_preset_options:
                st.session_state[range_preset_key] = "custom"
            if compare_enabled_key not in st.session_state:
                st.session_state[compare_enabled_key] = False
            if st.session_state.get(compare_mode_key) not in compare_mode_options:
                st.session_state[compare_mode_key] = "previous_period"
            if range_popover_mount_version_key not in st.session_state:
                st.session_state[range_popover_mount_version_key] = 0

            start_day, end_day = _normalize_date_range(
                st.session_state.get(range_value_key, (default_start, default_end)),
                min_d,
                max_d,
            )
            st.session_state[range_value_key] = (start_day, end_day)

            span_days = max((end_day - start_day).days + 1, 1)
            compare_default_end = _coerce_date_value(start_day - timedelta(days=1), min_d, max_d)
            compare_default_start = _coerce_date_value(compare_default_end - timedelta(days=span_days - 1), min_d, max_d)
            if compare_custom_key not in st.session_state:
                st.session_state[compare_custom_key] = (compare_default_start, compare_default_end)

            if st.session_state.get(range_preset_draft_key) not in date_preset_options:
                st.session_state[range_preset_draft_key] = str(st.session_state.get(range_preset_key, "custom"))
            if range_draft_key not in st.session_state:
                st.session_state[range_draft_key] = (start_day, end_day)
            else:
                st.session_state[range_draft_key] = _normalize_date_range(
                    st.session_state.get(range_draft_key, (start_day, end_day)),
                    min_d,
                    max_d,
                )
            if range_preset_draft_last_key not in st.session_state:
                st.session_state[range_preset_draft_last_key] = st.session_state.get(range_preset_draft_key, "custom")
            if compare_enabled_draft_key not in st.session_state:
                st.session_state[compare_enabled_draft_key] = bool(st.session_state.get(compare_enabled_key, False))
            if st.session_state.get(compare_mode_draft_key) not in compare_mode_options:
                st.session_state[compare_mode_draft_key] = str(st.session_state.get(compare_mode_key, "previous_period"))
            if compare_custom_draft_key not in st.session_state:
                st.session_state[compare_custom_draft_key] = st.session_state.get(
                    compare_custom_key,
                    (compare_default_start, compare_default_end),
                )
            st.session_state[compare_custom_draft_key] = _normalize_date_range(
                st.session_state.get(compare_custom_draft_key, (compare_default_start, compare_default_end)),
                min_d,
                max_d,
            )
            st.session_state.pop(legacy_range_draft_start_key, None)
            st.session_state.pop(legacy_range_draft_end_key, None)
            st.session_state.pop(legacy_compare_draft_start_key, None)
            st.session_state.pop(legacy_compare_draft_end_key, None)
            restore_payload = st.session_state.pop(draft_restore_key, None)
            if isinstance(restore_payload, dict):
                restored_preset = str(restore_payload.get("range_preset", "custom")).strip().lower()
                if restored_preset not in date_preset_options:
                    restored_preset = "custom"
                restored_range = _normalize_date_range(restore_payload.get("range", (start_day, end_day)), min_d, max_d)
                restored_compare_enabled = bool(restore_payload.get("compare_enabled", False))
                restored_compare_mode = str(restore_payload.get("compare_mode", "previous_period")).strip().lower()
                if restored_compare_mode not in compare_mode_options:
                    restored_compare_mode = "previous_period"
                restored_compare_custom = _normalize_date_range(
                    restore_payload.get("compare_custom", (compare_default_start, compare_default_end)),
                    min_d,
                    max_d,
                )
                st.session_state[range_preset_draft_key] = restored_preset
                st.session_state[range_draft_key] = restored_range
                st.session_state[range_preset_draft_last_key] = restored_preset
                st.session_state[range_draft_start_input_key] = restored_range[0]
                st.session_state[range_draft_end_input_key] = restored_range[1]
                st.session_state[compare_enabled_draft_key] = restored_compare_enabled
                st.session_state[compare_mode_draft_key] = restored_compare_mode
                st.session_state[compare_custom_draft_key] = restored_compare_custom
                st.session_state[compare_custom_start_input_key] = restored_compare_custom[0]
                st.session_state[compare_custom_end_input_key] = restored_compare_custom[1]

            range_display_label = f"{start_day.strftime('%Y/%m/%d')} - {end_day.strftime('%Y/%m/%d')}"
            popover_mount_version = int(st.session_state.get(range_popover_mount_version_key, 0))
            if popover_mount_version % 2 == 1:
                # Add a hidden node to force a fresh popover mount after apply.
                st.markdown("<span style='display:none'></span>", unsafe_allow_html=True)

            with st.popover(range_display_label, use_container_width=True):
                draft_start, draft_end = _normalize_date_range(
                    st.session_state.get(range_draft_key, (start_day, end_day)),
                    min_d,
                    max_d,
                )
                if range_draft_start_input_key not in st.session_state:
                    st.session_state[range_draft_start_input_key] = draft_start
                if range_draft_end_input_key not in st.session_state:
                    st.session_state[range_draft_end_input_key] = draft_end

                compare_draft_start, compare_draft_end = _normalize_date_range(
                    st.session_state.get(compare_custom_draft_key, (compare_default_start, compare_default_end)),
                    min_d,
                    max_d,
                )
                if compare_custom_start_input_key not in st.session_state:
                    st.session_state[compare_custom_start_input_key] = compare_draft_start
                if compare_custom_end_input_key not in st.session_state:
                    st.session_state[compare_custom_end_input_key] = compare_draft_end

                with st.form(key=f"top_range_form_{tenant_id}", clear_on_submit=False):
                    st.radio(
                        "Preset",
                        options=list(date_preset_options),
                        index=list(date_preset_options).index(str(st.session_state.get(range_preset_draft_key, "custom"))),
                        format_func=lambda value: str(date_preset_labels.get(str(value), value)),
                        key=range_preset_draft_key,
                        label_visibility="collapsed",
                    )

                    range_start_col, range_end_col = st.columns(2, gap="small")
                    with range_start_col:
                        st.date_input(
                            "Inicio",
                            min_value=min_d,
                            max_value=max_d,
                            key=range_draft_start_input_key,
                        )
                    with range_end_col:
                        st.date_input(
                            "Fin",
                            min_value=min_d,
                            max_value=max_d,
                            key=range_draft_end_input_key,
                        )

                    st.divider()
                    st.toggle(
                        "Comparar",
                        value=bool(st.session_state.get(compare_enabled_draft_key, False)),
                        key=compare_enabled_draft_key,
                    )
                    st.selectbox(
                        "Modo de comparacion",
                        options=list(compare_mode_options),
                        index=list(compare_mode_options).index(str(st.session_state.get(compare_mode_draft_key, "previous_period"))),
                        format_func=lambda value: str(compare_mode_labels.get(str(value), value)),
                        key=compare_mode_draft_key,
                    )
                    compare_start_col, compare_end_col = st.columns(2, gap="small")
                    with compare_start_col:
                        st.date_input(
                            "Inicio comparativo",
                            min_value=min_d,
                            max_value=max_d,
                            key=compare_custom_start_input_key,
                        )
                    with compare_end_col:
                        st.date_input(
                            "Fin comparativo",
                            min_value=min_d,
                            max_value=max_d,
                            key=compare_custom_end_input_key,
                        )

                    apply_clicked = st.form_submit_button("Aplicar", width="stretch", type="primary")

                if apply_clicked:
                    applied_preset = str(st.session_state.get(range_preset_draft_key, "custom")).strip().lower()
                    if applied_preset not in date_preset_options:
                        applied_preset = "custom"

                    custom_range = _normalize_date_range(
                        (
                            st.session_state.get(range_draft_start_input_key, draft_start),
                            st.session_state.get(range_draft_end_input_key, draft_end),
                        ),
                        min_d,
                        max_d,
                    )
                    if applied_preset == "custom":
                        applied_start, applied_end = custom_range
                    else:
                        applied_start, applied_end = _resolve_date_preset_range(
                            applied_preset,
                            min_d,
                            max_d,
                            date_preset_all_options=date_preset_all_options,
                        )

                    st.session_state[range_draft_key] = (applied_start, applied_end)
                    st.session_state[range_preset_draft_last_key] = applied_preset
                    st.session_state[range_preset_key] = applied_preset
                    st.session_state[range_value_key] = (applied_start, applied_end)

                    applied_compare_enabled = bool(st.session_state.get(compare_enabled_draft_key, False))
                    st.session_state[compare_enabled_key] = applied_compare_enabled
                    applied_compare_mode = str(st.session_state.get(compare_mode_draft_key, "previous_period")).strip().lower()
                    if applied_compare_mode not in compare_mode_options:
                        applied_compare_mode = "previous_period"
                    st.session_state[compare_mode_key] = applied_compare_mode

                    applied_compare_custom = _normalize_date_range(
                        (
                            st.session_state.get(compare_custom_start_input_key, compare_draft_start),
                            st.session_state.get(compare_custom_end_input_key, compare_draft_end),
                        ),
                        min_d,
                        max_d,
                    )
                    st.session_state[compare_custom_draft_key] = applied_compare_custom
                    if applied_compare_mode == "custom":
                        st.session_state[compare_custom_key] = applied_compare_custom
                    else:
                        applied_compare_custom = _normalize_date_range(
                            st.session_state.get(compare_custom_key, (compare_default_start, compare_default_end)),
                            min_d,
                            max_d,
                        )

                    st.session_state[draft_restore_key] = {
                        "range_preset": applied_preset,
                        "range": (applied_start, applied_end),
                        "compare_enabled": applied_compare_enabled,
                        "compare_mode": applied_compare_mode,
                        "compare_custom": applied_compare_custom,
                    }
                    st.session_state[range_popover_mount_version_key] = popover_mount_version + 1
                    st.rerun()

    start_day, end_day = _normalize_date_range(
        st.session_state.get(f"top_date_range_{tenant_id}", _default_business_date_range(min_d, max_d)),
        min_d,
        max_d,
    )
    compare_enabled = bool(st.session_state.get(f"top_compare_enabled_{tenant_id}", False))
    compare_mode = str(st.session_state.get(f"top_compare_mode_{tenant_id}", "previous_period")).strip().lower()
    if compare_mode not in compare_mode_options:
        compare_mode = "previous_period"
    custom_compare_range: Any = (
        st.session_state.get(f"top_compare_custom_range_{tenant_id}") if compare_mode == "custom" else None
    )
    compare_start, compare_end, compare_label = _resolve_compare_range(
        mode=compare_mode,
        current_start=start_day,
        current_end=end_day,
        data_min=min_d,
        data_max=max_d,
        custom_range=custom_compare_range,
        compare_mode_options=compare_mode_options,
    )
    if not compare_enabled:
        compare_label = ""

    campaign_filters: dict[str, str] = {}
    filter_keys = normalize_campaign_filter_keys(campaign_filter_keys, [])
    available_filters: list[tuple[str, list[str]]] = []
    for filter_key in filter_keys:
        if report_cache_sig is not None:
            path_str, modified_ns, size_bytes = report_cache_sig
            values = cached_campaign_filter_values_from_report(
                path_str,
                modified_ns,
                size_bytes,
                filter_key,
                platform,
                start_day.isoformat(),
                end_day.isoformat(),
            )
        else:
            values = campaign_filter_values(
                camp_df,
                field=filter_key,
                platform=platform,
                start_day=start_day,
                end_day=end_day,
            )
        if len(values) > 1:
            available_filters.append((filter_key, values))
    if available_filters:
        st.markdown("<div class='app-filter-title'>Filtros de Campana</div>", unsafe_allow_html=True)
        filter_cols = st.columns(len(available_filters), gap="small")
        for idx, (filter_key, values) in enumerate(available_filters):
            options = ["Todos"] + values
            session_key = f"campaign_filter_{tenant_id}_{filter_key}"
            current = str(st.session_state.get(session_key, "Todos")).strip() or "Todos"
            if current not in options:
                current = "Todos"
            selected = filter_cols[idx].selectbox(
                str(campaign_filter_options.get(filter_key, filter_key)),
                options=options,
                index=options.index(current),
                key=session_key,
            )
            if selected != "Todos":
                campaign_filters[filter_key] = selected

    return start_day, end_day, platform, campaign_filters, compare_mode, compare_start, compare_end, compare_label


__all__ = [
    "_coerce_date_value",
    "_normalize_date_range",
    "_default_business_date_range",
    "_resolve_date_preset_range",
    "_resolve_compare_range",
    "render_top_filters",
]
