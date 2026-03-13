from __future__ import annotations

import html
import textwrap
from typing import Any, Callable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

SafeDivideFn = Callable[[float, float], float | None]
FmtCompactFn = Callable[[float | int | None], str]
FmtMoneyFn = Callable[[float | int | None], str]


def render_funnel_and_ga4(
    *,
    st_module: Any,
    section_set: set[str],
    df_sel: pd.DataFrame,
    metric_cols: dict[str, str],
    platform: str,
    ga4_event_df: pd.DataFrame,
    ga4_conversion_event_name: str,
    default_ga4_event_name: str,
    start_date: Any,
    end_date: Any,
    sdiv_fn: SafeDivideFn,
    fmt_compact_fn: FmtCompactFn,
    fmt_money_fn: FmtMoneyFn,
    c_google: str,
    c_meta: str,
    c_accent: str,
    c_mute: str,
) -> None:
    if "funnel" in section_set:
        impr = float(df_sel[metric_cols["impr"]].sum())
        clicks = float(df_sel[metric_cols["clicks"]].sum())
        conv = float(df_sel[metric_cols["conv"]].sum())
        sess_total = float(df_sel["ga4_sessions"].sum())
        sess = (
            sess_total
            if platform == "All"
            else sess_total * (sdiv_fn(clicks, float(df_sel["total_clicks"].sum())) or 0.0)
        )
        if impr <= 0 and clicks > 0:
            impr = clicks / 0.03

        funnel_vals = [
            ("Impresiones", max(impr, 0.0)),
            ("Clics", max(min(clicks, impr), 0.0)),
            ("Sesiones", max(min(sess, clicks), 0.0)),
            ("Conversiones", max(min(conv, sess), 0.0)),
        ]
        top_val = max(float(funnel_vals[0][1]), 1.0)
        funnel_stage_colors = [c_google, c_accent, c_mute, c_meta]
        rows_html: list[str] = []
        prev_val: float | None = None
        for idx, (name, value) in enumerate(funnel_vals):
            pct = 0.0 if value <= 0 else min(max((value / top_val) * 100.0, 0.0), 100.0)
            stage_color = funnel_stage_colors[idx % len(funnel_stage_colors)]
            if prev_val is None or prev_val <= 0:
                drop_html = "<span class='funnel-drop funnel-drop-base' title='Etapa base'>Base</span>"
            else:
                drop_pct = max(0.0, min(100.0, (1.0 - sdiv_fn(value, prev_val)) * 100.0))
                drop_html = (
                    "<span class='funnel-drop' title='Ca\u00edda vs etapa anterior'>"
                    f"&darr; {drop_pct:.1f}%</span>"
                )
            rows_html.append(
                f"<div class='funnel-row'>"
                f"<div class='funnel-fill' style='width:{pct:.2f}%; background:{stage_color}33; border:1px solid {stage_color}66;'></div>"
                f"<div class='funnel-content'>"
                f"<span class='funnel-name'>{name}</span>"
                f"<span class='funnel-metrics'><span class='funnel-value'>{fmt_compact_fn(value)}</span>{drop_html}</span>"
                f"</div>"
                f"</div>"
            )
            prev_val = value

        st_module.markdown(
            textwrap.dedent(
                f"""
                <div class='funnel-card'>
                  <div class='funnel-title'>Funnel de Conversi\u00f3n</div>
                  <div class='funnel-stack'>{''.join(rows_html)}</div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    if "ga4_conversion" in section_set:
        ga4_event_name = (
            str(ga4_conversion_event_name or default_ga4_event_name).strip() or default_ga4_event_name
        )
        ga4_filtered = ga4_event_df.copy() if not ga4_event_df.empty else pd.DataFrame()
        ga4_conv_total = 0.0
        if not ga4_filtered.empty:
            if "date" in ga4_filtered.columns:
                ga4_filtered = ga4_filtered[
                    (ga4_filtered["date"] >= start_date) & (ga4_filtered["date"] <= end_date)
                ]
            event_col = (
                "eventName"
                if "eventName" in ga4_filtered.columns
                else ("event_name" if "event_name" in ga4_filtered.columns else None)
            )
            if event_col:
                ga4_filtered = ga4_filtered[
                    ga4_filtered[event_col].astype(str).str.strip().str.lower()
                    == ga4_event_name.lower()
                ]
            if platform in ("Google", "Meta") and "platform" in ga4_filtered.columns:
                ga4_filtered = ga4_filtered[
                    ga4_filtered["platform"].astype(str).str.strip().str.lower()
                    == platform.lower()
                ]
            conv_col = (
                "conversions"
                if "conversions" in ga4_filtered.columns
                else (
                    "eventCount"
                    if "eventCount" in ga4_filtered.columns
                    else ("event_count" if "event_count" in ga4_filtered.columns else None)
                )
            )
            if conv_col:
                ga4_conv_total = float(
                    pd.to_numeric(ga4_filtered[conv_col], errors="coerce").fillna(0.0).sum()
                )

        spend_total = float(df_sel[metric_cols["spend"]].sum()) if not df_sel.empty else 0.0
        ga4_cpl = sdiv_fn(spend_total, ga4_conv_total)
        st_module.markdown(
            textwrap.dedent(
                f"""
                <div class='ga4-conv-card'>
                  <div class='ga4-conv-title'>Conversiones GA4</div>
                  <div class='ga4-conv-event'>Evento: {html.escape(ga4_event_name)}</div>
                  <div class='ga4-conv-grid'>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>Conversiones</span>
                      <span class='ga4-conv-value'>{fmt_compact_fn(ga4_conv_total)}</span>
                    </div>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>Inversi\u00f3n</span>
                      <span class='ga4-conv-value'>{fmt_money_fn(spend_total)}</span>
                    </div>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>Plataforma</span>
                      <span class='ga4-conv-value'>{html.escape(platform)}</span>
                    </div>
                    <div class='ga4-conv-item'>
                      <span class='ga4-conv-label'>CPL GA4</span>
                      <span class='ga4-conv-value'>{fmt_money_fn(ga4_cpl)}</span>
                    </div>
                  </div>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


def render_media_mix(
    *,
    st_module: Any,
    platform: str,
    df_sel: pd.DataFrame,
    sdiv_fn: SafeDivideFn,
    fmt_pct_fn: Callable[[float | None], str],
    fmt_money_fn: FmtMoneyFn,
    c_accent: str,
    c_mute: str,
    c_meta: str,
    c_text: str,
    c_grid: str,
) -> None:
    rows: list[dict[str, float | str | None]] = []
    if platform in ("All", "Meta"):
        meta_spend = float(df_sel["meta_spend"].sum()) if not df_sel.empty else 0.0
        meta_clicks = float(df_sel["meta_clicks"].sum()) if not df_sel.empty else 0.0
        meta_impr = float(df_sel["meta_impr"].sum()) if not df_sel.empty else 0.0
        meta_conv = float(df_sel["meta_conv"].sum()) if not df_sel.empty else 0.0
        if meta_spend > 0 or meta_clicks > 0 or meta_impr > 0:
            rows.append(
                {
                    "platform": "Meta",
                    "spend": meta_spend,
                    "clicks": meta_clicks,
                    "impressions": meta_impr,
                    "conversions": meta_conv,
                    "cpc": sdiv_fn(meta_spend, meta_clicks),
                    "cpm": (meta_spend * 1000.0 / meta_impr) if meta_impr > 0 else None,
                    "cvr": sdiv_fn(meta_conv, meta_clicks),
                }
            )
    if platform in ("All", "Google"):
        google_spend = float(df_sel["google_spend"].sum()) if not df_sel.empty else 0.0
        google_clicks = float(df_sel["google_clicks"].sum()) if not df_sel.empty else 0.0
        google_impr = float(df_sel["google_impr"].sum()) if not df_sel.empty else 0.0
        google_conv = float(df_sel["google_conv"].sum()) if not df_sel.empty else 0.0
        if google_spend > 0 or google_clicks > 0 or google_impr > 0:
            rows.append(
                {
                    "platform": "Google",
                    "spend": google_spend,
                    "clicks": google_clicks,
                    "impressions": google_impr,
                    "conversions": google_conv,
                    "cpc": sdiv_fn(google_spend, google_clicks),
                    "cpm": (google_spend * 1000.0 / google_impr) if google_impr > 0 else None,
                    "cvr": sdiv_fn(google_conv, google_clicks),
                }
            )
    mix = pd.DataFrame(rows)
    if mix.empty:
        st_module.info("No hay datos suficientes para Mix y Eficiencia Paid.")
        return
    mix["spend"] = pd.to_numeric(mix["spend"], errors="coerce").fillna(0.0)
    total_spend = float(mix["spend"].sum())
    if total_spend > 0:
        mix["spend_share"] = mix["spend"] / total_spend
    else:
        mix["spend_share"] = 0.0

    st_module.markdown(
        "<div class='viz-title' style='margin-bottom:0.35rem;'>4) Mix y Eficiencia Paid (CPC / CPM / CVR)</div>",
        unsafe_allow_html=True,
    )
    mix_rows_html: list[str] = []
    for _, row in mix.iterrows():
        share_pct = max(min(float(row.get("spend_share", 0.0)) * 100.0, 100.0), 0.0)
        mix_rows_html.append(
            "<div class='mix-row'>"
            f"<div class='mix-head'><span class='mix-platform'>{html.escape(str(row['platform']))}</span>"
            f"<span class='mix-share'>{fmt_pct_fn(float(row['spend_share']))} share spend</span></div>"
            "<div class='mix-bar-wrap'>"
            f"<div class='mix-bar' style='width:{share_pct:.2f}%; background:{c_accent};'></div>"
            "</div>"
            f"<div class='mix-kpis'>CPC {fmt_money_fn(row['cpc'] if pd.notna(row['cpc']) else None)}"
            f" · CPM {fmt_money_fn(row['cpm'] if pd.notna(row['cpm']) else None)}"
            f" · CVR {fmt_pct_fn(row['cvr'] if pd.notna(row['cvr']) else None)}</div>"
            "</div>"
        )
    st_module.markdown(
        f"""
        <style>
          .mix-card {{
            border: 1px solid rgba(32,29,29,0.08);
            border-radius: 14px;
            padding: 0.75rem 0.9rem 0.8rem 0.9rem;
            background: rgba(255,255,255,0.62);
            margin-bottom: 0.5rem;
          }}
          .mix-row + .mix-row {{
            margin-top: 0.72rem;
            padding-top: 0.72rem;
            border-top: 1px solid rgba(32,29,29,0.08);
          }}
          .mix-head {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 0.5rem;
          }}
          .mix-platform {{
            font-weight: 700;
            color: {c_text};
          }}
          .mix-share {{
            font-size: 0.85rem;
            color: {c_mute};
          }}
          .mix-bar-wrap {{
            margin-top: 0.4rem;
            width: 100%;
            height: 8px;
            border-radius: 999px;
            background: rgba(32,29,29,0.10);
            overflow: hidden;
          }}
          .mix-bar {{
            height: 100%;
            border-radius: 999px;
          }}
          .mix-kpis {{
            margin-top: 0.4rem;
            color: {c_text};
            font-size: 0.88rem;
          }}
        </style>
        <div class="mix-card">{''.join(mix_rows_html)}</div>
        """,
        unsafe_allow_html=True,
    )
    if total_spend > 0:
        share_parts = [
            f"{platform_name}: {fmt_pct_fn(float(share))}"
            for platform_name, share in zip(mix["platform"], mix["spend_share"])
        ]
        if share_parts:
            st_module.caption("Share spend | " + " | ".join(share_parts))

    mix_view = mix.rename(
        columns={
            "platform": "Plataforma",
            "spend": "Inversi\u00f3n",
            "spend_share": "Share Spend",
            "cpc": "CPC",
            "cpm": "CPM",
            "cvr": "CVR",
            "clicks": "Clics",
            "impressions": "Impresiones",
            "conversions": "Conversiones",
        }
    )[["Plataforma", "Inversi\u00f3n", "Share Spend", "CPC", "CPM", "CVR", "Clics", "Impresiones", "Conversiones"]]
    mix_display = mix_view.copy()
    mix_display["Inversi\u00f3n"] = mix_display["Inversi\u00f3n"].apply(lambda v: fmt_money_fn(float(v)))
    mix_display["Share Spend"] = mix_display["Share Spend"].apply(lambda v: fmt_pct_fn(float(v)))
    mix_display["CPC"] = mix_display["CPC"].apply(lambda v: fmt_money_fn(v if pd.notna(v) else None))
    mix_display["CPM"] = mix_display["CPM"].apply(lambda v: fmt_money_fn(v if pd.notna(v) else None))
    mix_display["CVR"] = mix_display["CVR"].apply(lambda v: fmt_pct_fn(v if pd.notna(v) else None))
    mix_display["Clics"] = mix_display["Clics"].apply(lambda v: f"{float(v):,.0f}")
    mix_display["Impresiones"] = mix_display["Impresiones"].apply(lambda v: f"{float(v):,.0f}")
    mix_display["Conversiones"] = mix_display["Conversiones"].apply(lambda v: f"{float(v):,.2f}")
    st_module.table(mix_display.set_index("Plataforma"))


def render_lead_demographics(
    *,
    st_module: Any,
    lead_demo_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    prev_start_date: Any,
    prev_end_date: Any,
    age_bucket_order: list[str] | tuple[str, ...],
    sdiv_fn: SafeDivideFn,
    pct_delta_fn: Callable[[float, float], float | None],
    fmt_delta_compact_fn: Callable[[float | None], str],
    fmt_pct_fn: Callable[[float | None], str],
    fmt_money_fn: FmtMoneyFn,
    c_google: str,
    c_meta: str,
    c_mute: str,
    c_accent: str,
    c_grid: str,
    c_text: str,
) -> None:
    st_module.markdown(
        "<div class='viz-title' style='margin-bottom:0.35rem;'>5) Distribuci\u00f3n de Leads Paid por Edad y G\u00e9nero</div>",
        unsafe_allow_html=True,
    )
    st_module.caption(
        "M\u00e9trica de an\u00e1lisis (breakdown de plataformas). No es comparable 1:1 con el KPI global de Conversiones."
    )
    if lead_demo_df.empty:
        st_module.info("No hay datos de leads por edad y g\u00e9nero para el tenant.")
        return

    dcur = lead_demo_df[(lead_demo_df["date"] >= start_date) & (lead_demo_df["date"] <= end_date)].copy()
    dprev = lead_demo_df[(lead_demo_df["date"] >= prev_start_date) & (lead_demo_df["date"] <= prev_end_date)].copy()
    if platform in ("Google", "Meta"):
        dcur = dcur[dcur["platform"] == platform]
        dprev = dprev[dprev["platform"] == platform]
    if dcur.empty:
        st_module.info("Sin datos demogr\u00e1ficos de leads para el rango/plataforma seleccionados.")
        return

    age_cur = dcur[dcur["breakdown"].isin(["age", "age_gender"])].copy()
    age_prev = dprev[dprev["breakdown"].isin(["age", "age_gender"])].copy()
    gender_cur = dcur[dcur["breakdown"].isin(["gender", "age_gender"])].copy()
    gender_prev = dprev[dprev["breakdown"].isin(["gender", "age_gender"])].copy()
    if age_cur.empty and gender_cur.empty:
        st_module.info("Sin datos demogr\u00e1ficos para el rango/plataforma seleccionados.")
        return

    age_totals = (
        age_cur.groupby("age_range", as_index=False)
        .agg(
            leads=("leads", "sum"),
            spend=("spend", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
        if not age_cur.empty
        else pd.DataFrame(columns=["age_range", "leads", "spend", "clicks", "impressions"])
    )
    age_prev_totals = (
        age_prev.groupby("age_range", as_index=False).agg(leads_prev=("leads", "sum"))
        if not age_prev.empty
        else pd.DataFrame(columns=["age_range", "leads_prev"])
    )
    age_totals = age_totals.merge(age_prev_totals, on="age_range", how="left").fillna({"leads_prev": 0.0})
    total_leads = float(age_totals["leads"].sum()) if not age_totals.empty else 0.0
    total_prev_leads = float(age_totals["leads_prev"].sum()) if not age_totals.empty else 0.0

    gender_totals = (
        gender_cur.groupby("gender", as_index=False)
        .agg(leads=("leads", "sum"))
        .sort_values("leads", ascending=False, na_position="last")
        if not gender_cur.empty
        else pd.DataFrame(columns=["gender", "leads"])
    )
    gender_prev_totals = (
        gender_prev.groupby("gender", as_index=False).agg(leads_prev=("leads", "sum"))
        if not gender_prev.empty
        else pd.DataFrame(columns=["gender", "leads_prev"])
    )
    gender_totals = gender_totals.merge(gender_prev_totals, on="gender", how="left").fillna({"leads_prev": 0.0})
    if total_leads <= 0 and not gender_totals.empty:
        total_leads = float(gender_totals["leads"].sum())
        total_prev_leads = float(gender_totals["leads_prev"].sum())

    if total_leads <= 0:
        st_module.info("No se detectaron leads para construir el desglose de edad y g\u00e9nero.")
        return

    if not age_totals.empty:
        age_totals["share"] = age_totals["leads"].apply(lambda v: sdiv_fn(float(v), total_leads) or 0.0)
        age_totals["age_order"] = age_totals["age_range"].apply(
            lambda v: age_bucket_order.index(v) if v in age_bucket_order else len(age_bucket_order)
        )
        age_totals = age_totals.sort_values(["age_order", "leads"], ascending=[True, False]).drop(
            columns=["age_order"]
        )
    top_age = str(age_totals.iloc[0]["age_range"]) if not age_totals.empty else "N/A"
    top_age_share = float(age_totals.iloc[0]["share"]) if not age_totals.empty else 0.0
    gender_totals["share"] = gender_totals["leads"].apply(lambda v: sdiv_fn(float(v), total_leads) or 0.0)
    top_gender = str(gender_totals.iloc[0]["gender"]) if not gender_totals.empty else "N/A"
    top_gender_share = float(gender_totals.iloc[0]["share"]) if not gender_totals.empty else 0.0

    m1, m2, m3 = st_module.columns(3)
    m1.metric(
        "Leads (breakdown demogr\u00e1fico)",
        f"{total_leads:,.0f}",
        fmt_delta_compact_fn(pct_delta_fn(total_leads, total_prev_leads)),
    )
    m2.metric("Top Edad (share)", top_age, fmt_pct_fn(top_age_share))
    m3.metric("Top G\u00e9nero (share)", top_gender, fmt_pct_fn(top_gender_share))

    cross_roll = (
        dcur[dcur["breakdown"] == "age_gender"]
        .groupby(["age_range", "gender"], as_index=False)
        .agg(
            leads=("leads", "sum"),
            spend=("spend", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
        if "age_gender" in set(dcur["breakdown"].astype(str))
        else pd.DataFrame(columns=["age_range", "gender", "leads", "spend", "clicks", "impressions"])
    )
    age_gender = (
        cross_roll.pivot_table(index="age_range", columns="gender", values="leads", aggfunc="sum", fill_value=0.0)
        if not cross_roll.empty
        else pd.DataFrame()
    )
    has_cross = not age_gender.empty

    viz_col_1, viz_col_2 = st_module.columns([1.9, 1.1], gap="large")
    with viz_col_1:
        if has_cross:
            ordered_age = [a for a in age_bucket_order if a in age_gender.index]
            if not ordered_age:
                ordered_age = sorted([str(v) for v in age_gender.index.tolist()])
            age_gender = age_gender.reindex(ordered_age, fill_value=0.0)

            bar = go.Figure()
            for gender, color in (
                ("Female", c_meta),
                ("Male", c_google),
                ("Unknown", c_mute),
                ("All", c_accent),
            ):
                if gender not in age_gender.columns:
                    continue
                bar.add_trace(
                    go.Bar(
                        x=age_gender.index.tolist(),
                        y=age_gender[gender].tolist(),
                        name=gender,
                        marker={"color": color},
                        hovertemplate="%{x}<br>" + gender + ": %{y:,.0f} leads<extra></extra>",
                    )
                )
            bar.update_layout(
                barmode="stack",
                height=290,
                margin={"l": 8, "r": 8, "t": 40, "b": 10},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend={"orientation": "h", "x": 0.0, "y": 1.15},
                xaxis={"title": "", "tickfont": {"size": 11, "color": c_mute}},
                yaxis={"title": "Leads", "gridcolor": c_grid, "tickfont": {"size": 11, "color": c_mute}},
                title={"text": "Distribuci\u00f3n de Leads por Edad", "font": {"size": 14, "color": c_text}},
            )
            st_module.plotly_chart(bar, width="stretch")
        elif not age_totals.empty:
            bar = go.Figure(
                go.Bar(
                    x=age_totals["age_range"],
                    y=age_totals["leads"],
                    marker={"color": c_google},
                    hovertemplate="%{x}<br>Leads: %{y:,.0f}<extra></extra>",
                )
            )
            bar.update_layout(
                height=290,
                margin={"l": 8, "r": 8, "t": 40, "b": 10},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis={"title": "", "tickfont": {"size": 11, "color": c_mute}},
                yaxis={"title": "Leads", "gridcolor": c_grid, "tickfont": {"size": 11, "color": c_mute}},
                title={"text": "Distribuci\u00f3n de Leads por Edad", "font": {"size": 14, "color": c_text}},
            )
            st_module.plotly_chart(bar, width="stretch")
        else:
            st_module.info("Sin datos por edad para el rango seleccionado.")
    with viz_col_2:
        if gender_totals.empty:
            st_module.info("Sin datos por g\u00e9nero para el rango seleccionado.")
        else:
            gender_color = {"Female": c_meta, "Male": c_google, "Unknown": c_mute, "All": c_accent}
            pie = go.Figure(
                go.Pie(
                    labels=gender_totals["gender"],
                    values=gender_totals["leads"],
                    hole=0.56,
                    marker={"colors": [gender_color.get(str(g), c_mute) for g in gender_totals["gender"]]},
                    texttemplate="%{label}<br>%{percent}",
                    hovertemplate="%{label}: %{value:,.0f} leads<extra></extra>",
                    sort=False,
                )
            )
            pie.update_layout(
                height=290,
                margin={"l": 4, "r": 4, "t": 40, "b": 8},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                title={"text": "Mix de G\u00e9nero", "font": {"size": 14, "color": c_text}},
            )
            st_module.plotly_chart(pie, width="stretch")

    if not cross_roll.empty:
        demo_table = cross_roll.copy()
    elif not age_totals.empty:
        demo_table = age_totals[["age_range", "leads", "spend", "impressions", "clicks"]].copy()
        demo_table["gender"] = "All"
    else:
        demo_table = gender_totals[["gender", "leads"]].copy()
        demo_table["age_range"] = "All"
        demo_table["spend"] = 0.0
        demo_table["impressions"] = 0.0
        demo_table["clicks"] = 0.0
    demo_table["share_leads"] = demo_table["leads"].apply(lambda v: sdiv_fn(float(v), total_leads) or 0.0)
    demo_table = demo_table.rename(
        columns={
            "age_range": "Edad",
            "gender": "G\u00e9nero",
            "leads": "Leads",
            "share_leads": "Share Leads",
            "spend": "Gasto",
            "impressions": "Impresiones",
            "clicks": "Clicks",
        }
    )[["Edad", "G\u00e9nero", "Leads", "Share Leads", "Gasto", "Impresiones", "Clicks"]]
    demo_table = demo_table.sort_values(["Leads", "Share Leads"], ascending=[False, False], na_position="last")
    st_module.dataframe(
        demo_table.style.format(
            {
                "Leads": "{:.0f}",
                "Share Leads": lambda v: fmt_pct_fn(float(v)),
                "Gasto": lambda v: fmt_money_fn(float(v)),
                "Impresiones": "{:.0f}",
                "Clicks": "{:.0f}",
            }
        ),
        width="stretch",
        hide_index=True,
    )


def render_lead_geo_map(
    *,
    st_module: Any,
    lead_geo_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    prev_start_date: Any,
    prev_end_date: Any,
    clean_text_value_fn: Callable[[Any], str],
    country_name_from_code_fn: Callable[[Any], str],
    theme_color_scale_fn: Callable[[str], list[str]],
    sdiv_fn: SafeDivideFn,
    pct_delta_fn: Callable[[float, float], float | None],
    fmt_delta_compact_fn: Callable[[float | None], str],
    fmt_pct_fn: Callable[[float | None], str],
    fmt_money_fn: FmtMoneyFn,
    c_google: str,
    c_mute: str,
    c_grid: str,
    c_text: str,
) -> None:
    st_module.markdown(
        "<div class='viz-title' style='margin-bottom:0.35rem;'>6) Mapa de Distribuci\u00f3n de Leads Paid</div>",
        unsafe_allow_html=True,
    )
    st_module.caption(
        "M\u00e9trica de an\u00e1lisis (breakdown de plataformas). No es comparable 1:1 con el KPI global de Conversiones."
    )
    if lead_geo_df.empty:
        st_module.info("No hay datos de geograf\u00eda de leads para el tenant.")
        return

    gcur = lead_geo_df[(lead_geo_df["date"] >= start_date) & (lead_geo_df["date"] <= end_date)].copy()
    gprev = lead_geo_df[(lead_geo_df["date"] >= prev_start_date) & (lead_geo_df["date"] <= prev_end_date)].copy()
    if platform in ("Google", "Meta"):
        gcur = gcur[gcur["platform"] == platform]
        gprev = gprev[gprev["platform"] == platform]
    if gcur.empty:
        st_module.info("Sin datos geogr\u00e1ficos de leads para el rango/plataforma seleccionados.")
        return

    def _first_text(values: pd.Series) -> str:
        for raw in values:
            txt = clean_text_value_fn(raw)
            if txt:
                return txt
        return ""

    geo_roll = (
        gcur.groupby(["country_code", "region"], as_index=False)
        .agg(
            country_name=("country_name", _first_text),
            leads=("leads", "sum"),
            spend=("spend", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
    )
    geo_roll["country_name"] = geo_roll.apply(
        lambda r: clean_text_value_fn(r.get("country_name")) or country_name_from_code_fn(r.get("country_code", "")),
        axis=1,
    )
    total_geo_leads = float(geo_roll["leads"].sum())
    if total_geo_leads <= 0:
        st_module.info("No se detectaron leads para construir el mapa geogr\u00e1fico.")
        return

    prev_geo_total = float(gprev["leads"].sum()) if not gprev.empty else 0.0
    country_roll = (
        geo_roll.groupby("country_code", as_index=False)
        .agg(
            country_name=("country_name", _first_text),
            leads=("leads", "sum"),
            spend=("spend", "sum"),
            clicks=("clicks", "sum"),
            impressions=("impressions", "sum"),
        )
    )
    country_roll["country_name"] = country_roll.apply(
        lambda r: clean_text_value_fn(r.get("country_name")) or country_name_from_code_fn(r.get("country_code", "")),
        axis=1,
    )
    country_roll["share_leads"] = country_roll["leads"].apply(lambda v: sdiv_fn(float(v), total_geo_leads) or 0.0)
    top_country = country_roll.sort_values("leads", ascending=False, na_position="last").head(1)
    top_country_name = (
        clean_text_value_fn(top_country.iloc[0]["country_name"])
        or clean_text_value_fn(top_country.iloc[0]["country_code"])
        or "N/A"
    ) if not top_country.empty else "N/A"
    top_country_share = float(top_country.iloc[0]["share_leads"]) if not top_country.empty else 0.0
    country_count = int((country_roll["leads"] > 0).sum())
    prev_country_count = (
        int((gprev.groupby("country_code", as_index=False)["leads"].sum()["leads"] > 0).sum())
        if not gprev.empty
        else 0
    )

    m1, m2, m3 = st_module.columns(3)
    m1.metric(
        "Leads (geo breakdown)",
        f"{total_geo_leads:,.0f}",
        fmt_delta_compact_fn(pct_delta_fn(total_geo_leads, prev_geo_total)),
    )
    m2.metric("Pa\u00eds Top (share)", top_country_name, fmt_pct_fn(top_country_share))
    m3.metric(
        "Cobertura Pa\u00edses",
        f"{country_count}",
        fmt_delta_compact_fn(pct_delta_fn(float(country_count), float(prev_country_count))),
    )

    map_df = country_roll.copy()
    map_df["country_code"] = map_df["country_code"].astype(str).str.strip().str.upper()
    map_df["country_name"] = map_df.apply(
        lambda r: clean_text_value_fn(r.get("country_name")) or country_name_from_code_fn(r.get("country_code", "")),
        axis=1,
    )
    map_df["country_label"] = map_df.apply(
        lambda r: clean_text_value_fn(r.get("country_name")) or clean_text_value_fn(r.get("country_code")),
        axis=1,
    )
    map_df["cpl"] = map_df.apply(
        lambda r: sdiv_fn(float(r.get("spend", 0.0)), float(r.get("leads", 0.0))),
        axis=1,
    )
    map_df = map_df[
        map_df["country_code"].str.fullmatch(r"[A-Z]{2}", na=False)
        & (pd.to_numeric(map_df["leads"], errors="coerce").fillna(0.0) > 0)
    ].copy()
    map_col, table_col = st_module.columns([1.7, 1.3], gap="large")
    with map_col:
        if map_df.empty:
            st_module.info("No hay pa\u00edses mapeables para mostrar en el mapa.")
        else:
            if int(map_df["country_name"].nunique()) <= 2:
                ch = px.choropleth(
                    map_df,
                    locations="country_name",
                    locationmode="country names",
                    color="share_leads",
                    hover_name="country_label",
                    hover_data={
                        "country_code": True,
                        "leads": ":.0f",
                        "spend": ":.2f",
                        "cpl": ":.2f",
                        "share_leads": ":.2%",
                    },
                    color_continuous_scale=theme_color_scale_fn(c_google),
                )
                ch.update_layout(
                    height=330,
                    margin={"l": 0, "r": 0, "t": 6, "b": 0},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    coloraxis_colorbar={"title": "Share Leads", "tickformat": ".0%"},
                )
                ch.update_geos(
                    fitbounds="locations",
                    visible=False,
                    showcountries=True,
                    countrycolor="rgba(255,255,255,0.9)",
                    bgcolor="rgba(0,0,0,0)",
                )
                st_module.plotly_chart(ch, width="stretch")
            else:
                bubble_df = map_df.copy()
                bubble_df["bubble_size"] = bubble_df["leads"].clip(lower=0.0)
                ch = px.scatter_geo(
                    bubble_df,
                    locations="country_name",
                    locationmode="country names",
                    size="bubble_size",
                    color="share_leads",
                    hover_name="country_label",
                    hover_data={
                        "country_code": True,
                        "leads": ":.0f",
                        "spend": ":.2f",
                        "cpl": ":.2f",
                        "clicks": ":.0f",
                        "impressions": ":.0f",
                        "share_leads": ":.2%",
                    },
                    color_continuous_scale=theme_color_scale_fn(c_google),
                    size_max=48,
                    projection="natural earth",
                )
                ch.update_traces(
                    marker={"line": {"width": 0.7, "color": "rgba(255,255,255,0.9)"}, "opacity": 0.82}
                )
                ch.update_layout(
                    height=330,
                    margin={"l": 0, "r": 0, "t": 6, "b": 0},
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    coloraxis_colorbar={"title": "Share Leads", "tickformat": ".0%"},
                )
                ch.update_geos(showframe=False, showcoastlines=False, bgcolor="rgba(0,0,0,0)")
                st_module.plotly_chart(ch, width="stretch")
    with table_col:
        top_source = map_df if not map_df.empty else country_roll.copy()
        top_source = top_source[pd.to_numeric(top_source["leads"], errors="coerce").fillna(0.0) > 0].copy()
        top_country_view = (
            top_source.sort_values(["leads", "share_leads"], ascending=[False, False], na_position="last")
            .head(10)
            .copy()
        )
        if not top_country_view.empty:
            top_country_view["country_label"] = top_country_view.apply(
                lambda r: clean_text_value_fn(r.get("country_name")) or clean_text_value_fn(r.get("country_code")),
                axis=1,
            )
            top_country_view = top_country_view.sort_values("leads", ascending=True, na_position="last")
            top_bar = go.Figure(
                go.Bar(
                    x=top_country_view["leads"],
                    y=top_country_view["country_label"],
                    orientation="h",
                    marker={"color": c_google},
                    hovertemplate="%{y}<br>Leads: %{x:,.0f}<extra></extra>",
                )
            )
            top_bar.update_layout(
                height=210,
                margin={"l": 6, "r": 6, "t": 18, "b": 6},
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis={"title": "Leads", "gridcolor": c_grid, "tickfont": {"size": 10, "color": c_mute}},
                yaxis={"title": "", "tickfont": {"size": 10, "color": c_mute}},
                title={"text": "Top Pa\u00edses por Leads", "font": {"size": 13, "color": c_text}},
            )
            st_module.plotly_chart(top_bar, width="stretch")

        region_view = geo_roll.copy()
        region_view["country_label"] = region_view.apply(
            lambda r: clean_text_value_fn(r.get("country_name")) or clean_text_value_fn(r.get("country_code")),
            axis=1,
        )
        region_view["share_leads"] = region_view["leads"].apply(lambda v: sdiv_fn(float(v), total_geo_leads) or 0.0)
        region_view = region_view.rename(
            columns={
                "country_label": "Pa\u00eds",
                "region": "Regi\u00f3n",
                "leads": "Leads",
                "share_leads": "Share Leads",
                "spend": "Gasto",
                "clicks": "Clicks",
                "impressions": "Impresiones",
            }
        )[["Pa\u00eds", "Regi\u00f3n", "Leads", "Share Leads", "Gasto", "Clicks", "Impresiones"]]
        region_view = region_view.sort_values(["Leads", "Share Leads"], ascending=[False, False], na_position="last").head(15)
        st_module.dataframe(
            region_view.style.format(
                {
                    "Leads": "{:.0f}",
                    "Share Leads": lambda v: fmt_pct_fn(float(v)),
                    "Gasto": lambda v: fmt_money_fn(float(v)),
                    "Clicks": "{:.0f}",
                    "Impresiones": "{:.0f}",
                }
            ),
            width="stretch",
            hide_index=True,
        )


def render_device_breakdown(
    *,
    st_module: Any,
    paid_dev_df: pd.DataFrame,
    platform: str,
    start_date: Any,
    end_date: Any,
    prev_start_date: Any,
    prev_end_date: Any,
    sdiv_fn: SafeDivideFn,
    pct_delta_fn: Callable[[float, float], float | None],
    fmt_delta_compact_fn: Callable[[float | None], str],
    fmt_money_fn: FmtMoneyFn,
    fmt_pct_fn: Callable[[float | None], str],
    c_google: str,
    c_meta: str,
    c_mute: str,
) -> None:
    st_module.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
    st_module.markdown(
        "<div class='viz-title' style='margin-bottom:0.35rem;'>7) Dispositivos de Pauta (Desktop / Mobile / Other)</div>",
        unsafe_allow_html=True,
    )
    if paid_dev_df.empty or "date" not in paid_dev_df.columns:
        st_module.info("No hay datos de dispositivo de pauta para el rango seleccionado.")
        return

    date_series = pd.to_datetime(paid_dev_df["date"], errors="coerce").dt.date
    valid_mask = date_series.notna()
    cur_mask = valid_mask & (date_series >= start_date) & (date_series <= end_date)
    prev_mask = valid_mask & (date_series >= prev_start_date) & (date_series <= prev_end_date)
    pcur = paid_dev_df.loc[cur_mask]
    pprev = paid_dev_df.loc[prev_mask]
    if platform in ("Google", "Meta"):
        pcur = pcur[pcur["platform"] == platform]
        pprev = pprev[pprev["platform"] == platform]

    if pcur.empty:
        st_module.info("No hay datos de dispositivo de pauta para el rango seleccionado.")
        return

    cur_roll = (
        pcur.groupby("device", as_index=False)
        .agg(
            spend=("spend", "sum"),
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            conversions=("conversions", "sum"),
        )
    )
    prev_roll = (
        pprev.groupby("device", as_index=False).agg(impressions_prev=("impressions", "sum"))
        if not pprev.empty
        else pd.DataFrame(columns=["device", "impressions_prev"])
    )
    cur_roll = cur_roll.merge(prev_roll, on="device", how="left").fillna({"impressions_prev": 0.0})
    cur_roll["ctr"] = pd.to_numeric(cur_roll["clicks"], errors="coerce").fillna(0.0).div(
        pd.to_numeric(cur_roll["impressions"], errors="coerce").replace(0.0, pd.NA)
    )
    cur_roll["cpl"] = pd.to_numeric(cur_roll["spend"], errors="coerce").fillna(0.0).div(
        pd.to_numeric(cur_roll["conversions"], errors="coerce").replace(0.0, pd.NA)
    )
    cur_roll["delta_impressions"] = cur_roll.apply(
        lambda r: pct_delta_fn(float(r["impressions"]), float(r["impressions_prev"])),
        axis=1,
    )
    order = ["Desktop", "Mobile", "Other"]
    roll = pd.DataFrame({"device": order}).merge(cur_roll, on="device", how="left").fillna(0.0)

    m1, m2, m3 = st_module.columns(3)
    for idx, dname in enumerate(order):
        row = roll[roll["device"] == dname].iloc[0]
        target_col = [m1, m2, m3][idx]
        target_col.metric(
            f"{dname} Impresiones",
            f"{float(row['impressions']):,.0f}",
            fmt_delta_compact_fn(row["delta_impressions"]),
        )
    total_impressions = float(pd.to_numeric(roll["impressions"], errors="coerce").fillna(0.0).sum())
    if total_impressions <= 0:
        st_module.info("Sin impresiones para graficar distribuci\u00f3n por dispositivo.")
    else:
        color_by_device = {"Desktop": c_google, "Mobile": c_meta, "Other": c_mute}
        rows_html: list[str] = []
        for dname in order:
            row = roll[roll["device"] == dname].iloc[0]
            impressions_value = float(row["impressions"])
            share_pct = max(min((impressions_value / total_impressions) * 100.0, 100.0), 0.0)
            rows_html.append(
                "<div class='device-dist-row'>"
                f"<div class='device-dist-head'><span class='device-dist-name'>{html.escape(dname)}</span>"
                f"<span class='device-dist-metric'>{impressions_value:,.0f} · {share_pct:.1f}%</span></div>"
                "<div class='device-dist-track'>"
                f"<div class='device-dist-fill' style='width:{share_pct:.2f}%; background:{color_by_device.get(dname, c_mute)};'></div>"
                "</div>"
                "</div>"
            )
        st_module.markdown(
            f"""
            <style>
              .device-dist-card {{
                border: 1px solid rgba(32,29,29,0.08);
                border-radius: 14px;
                padding: 0.7rem 0.9rem 0.78rem 0.9rem;
                background: rgba(255,255,255,0.62);
                margin-bottom: 0.5rem;
              }}
              .device-dist-row + .device-dist-row {{
                margin-top: 0.62rem;
              }}
              .device-dist-head {{
                display: flex;
                justify-content: space-between;
                align-items: baseline;
                gap: 0.5rem;
                margin-bottom: 0.22rem;
              }}
              .device-dist-name {{
                font-weight: 700;
                color: #201D1D;
              }}
              .device-dist-metric {{
                font-size: 0.84rem;
                color: #7A879D;
              }}
              .device-dist-track {{
                width: 100%;
                height: 8px;
                border-radius: 999px;
                background: rgba(32,29,29,0.10);
                overflow: hidden;
              }}
              .device-dist-fill {{
                height: 100%;
                border-radius: 999px;
              }}
            </style>
            <div class="device-dist-card">{''.join(rows_html)}</div>
            """,
            unsafe_allow_html=True,
        )

    table = roll.rename(
        columns={
            "device": "Device",
            "spend": "Spend",
            "impressions": "Impressions",
            "clicks": "Clicks",
            "conversions": "Conversions",
            "ctr": "CTR",
            "cpl": "CPL",
        }
    )[["Device", "Spend", "Impressions", "Clicks", "Conversions", "CTR", "CPL"]]

    table_display = table.copy()
    table_display["Spend"] = table_display["Spend"].map(lambda v: fmt_money_fn(float(v)))
    table_display["Impressions"] = table_display["Impressions"].map(lambda v: f"{float(v):,.0f}")
    table_display["Clicks"] = table_display["Clicks"].map(lambda v: f"{float(v):,.0f}")
    table_display["Conversions"] = table_display["Conversions"].map(lambda v: f"{float(v):,.2f}")
    table_display["CTR"] = table_display["CTR"].map(lambda v: fmt_pct_fn(v if pd.notna(v) else None))
    table_display["CPL"] = table_display["CPL"].map(lambda v: fmt_money_fn(v if pd.notna(v) else None))
    st_module.table(table_display.set_index("Device"))


def render_audit_table(
    *,
    st_module: Any,
    df_sel: pd.DataFrame,
    metric_cols: dict[str, str],
    sdiv_fn: SafeDivideFn,
    fmt_money_fn: FmtMoneyFn,
    fmt_pct_fn: Callable[[float | None], str],
) -> None:
    st_module.markdown("<div style='height:0.7rem;'></div>", unsafe_allow_html=True)
    st_module.markdown(
        "<div class='viz-title' style='margin-bottom:0.35rem;'>8) Tabla Maestra de Auditoria</div>",
        unsafe_allow_html=True,
    )
    t = df_sel[
        [
            "date",
            "meta_spend",
            "google_spend",
            metric_cols["spend"],
            metric_cols["clicks"],
            metric_cols["impr"],
            metric_cols["conv"],
            "ga4_sessions",
            "ga4_avg_sess",
            "ga4_bounce",
        ]
    ].copy()
    t.columns = [
        "Date",
        "Meta Spend",
        "Google Spend",
        "Spend",
        "Clicks",
        "Impressions",
        "Conversions",
        "Sessions",
        "Avg Session (s)",
        "Bounce Rate",
    ]
    t["CPL"] = t.apply(lambda r: sdiv_fn(float(r["Spend"]), float(r["Conversions"])), axis=1)
    t["CTR"] = t.apply(lambda r: sdiv_fn(float(r["Clicks"]), float(r["Impressions"])), axis=1)
    t = t.sort_values("Date", ascending=False)
    display = t.copy()
    display["Date"] = display["Date"].map(lambda v: v.isoformat() if hasattr(v, "isoformat") else str(v))
    display["Meta Spend"] = display["Meta Spend"].map(lambda v: fmt_money_fn(float(v)))
    display["Google Spend"] = display["Google Spend"].map(lambda v: fmt_money_fn(float(v)))
    display["Spend"] = display["Spend"].map(lambda v: fmt_money_fn(float(v)))
    display["Clicks"] = display["Clicks"].map(lambda v: f"{float(v):,.0f}")
    display["Impressions"] = display["Impressions"].map(lambda v: f"{float(v):,.0f}")
    display["Conversions"] = display["Conversions"].map(lambda v: f"{float(v):,.2f}")
    display["Sessions"] = display["Sessions"].map(lambda v: f"{float(v):,.0f}")
    display["Avg Session (s)"] = display["Avg Session (s)"].map(lambda v: f"{float(v):,.1f}")
    display["Bounce Rate"] = display["Bounce Rate"].map(lambda v: fmt_pct_fn(float(v)))
    display["CPL"] = display["CPL"].map(lambda v: fmt_money_fn(v if pd.notna(v) else None))
    display["CTR"] = display["CTR"].map(lambda v: fmt_pct_fn(v if pd.notna(v) else None))
    st_module.dataframe(display, width="stretch", hide_index=True)


def render_top_pieces_section(
    *,
    render_top_pieces_range_fn: Callable[..., None],
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    platform: str,
    start_ref: Any,
    end_ref: Any,
    tenant_meta_account_id: str,
    tenant_google_customer_id: str,
    campaign_filters: dict[str, str],
    report_cache_sig: tuple[str, int, int] | None,
) -> None:
    render_top_pieces_range_fn(
        camp_df,
        piece_df,
        platform,
        start_ref,
        end_ref,
        tenant_meta_account_id=tenant_meta_account_id,
        tenant_google_customer_id=tenant_google_customer_id,
        campaign_filters=campaign_filters,
        report_cache_sig=report_cache_sig,
    )


def render_top_pieces_range(
    *,
    st_module: Any,
    camp_df: pd.DataFrame,
    piece_df: pd.DataFrame,
    platform: str,
    start_ref: Any,
    end_ref: Any,
    tenant_meta_account_id: str = "",
    tenant_google_customer_id: str = "",
    campaign_filters: dict[str, str] | None = None,
    report_cache_sig: tuple[str, int, int] | None = None,
    campaign_filter_options: dict[str, str],
    apply_campaign_filters_fn: Callable[[pd.DataFrame, dict[str, str]], pd.DataFrame],
    campaign_filters_cache_key_fn: Callable[[dict[str, str]], str],
    cached_top_pieces_roll_from_report_fn: Callable[[str, int, int, str, str, str, str], pd.DataFrame],
    piece_platform_link_fn: Callable[..., str],
    campaign_platform_link_fn: Callable[..., str],
    sdiv_fn: SafeDivideFn,
) -> None:
    if report_cache_sig is not None:
        path_str, modified_ns, size_bytes = report_cache_sig
        filter_key = campaign_filters_cache_key_fn(campaign_filters or {})
        top = cached_top_pieces_roll_from_report_fn(
            path_str,
            modified_ns,
            size_bytes,
            str(start_ref.isoformat()),
            str(end_ref.isoformat()),
            str(platform or "All"),
            filter_key,
        )
        if top.empty:
            st_module.info("No hay piezas/campañas para el rango seleccionado.")
            return
        top = top.copy()
        if "campaign_id" not in top.columns:
            top["campaign_id"] = ""
        if "piece_id" not in top.columns:
            top["piece_id"] = ""
        if "piece_name" not in top.columns:
            top["piece_name"] = "Sin nombre"
        if "platform" not in top.columns:
            top["platform"] = ""
        if "inversion" not in top.columns:
            top["inversion"] = 0.0
        if "conversiones" not in top.columns:
            top["conversiones"] = 0.0
        if "cpl" not in top.columns:
            top["cpl"] = top.apply(
                lambda r: sdiv_fn(float(r.get("inversion", 0.0)), float(r.get("conversiones", 0.0))),
                axis=1,
            )
        if "vista_previa" not in top.columns:
            top["vista_previa"] = ""
        top["Ver"] = top.apply(
            lambda r: piece_platform_link_fn(
                r.get("platform"),
                piece_id=r.get("piece_id"),
                campaign_id=r.get("campaign_id"),
                meta_account_id=tenant_meta_account_id,
                google_customer_id=tenant_google_customer_id,
            ),
            axis=1,
        )
        top["Campaña / Pieza"] = top["piece_name"].astype(str).replace({"": "Sin nombre"})
        top["Vista"] = top["vista_previa"].astype(str).replace({"nan": "", "None": ""})
        top["Plataforma"] = top["platform"].astype(str).replace({"": "N/A"})
        top["Gasto"] = pd.to_numeric(top["inversion"], errors="coerce")
        top["Conversiones"] = pd.to_numeric(top["conversiones"], errors="coerce")
        top["CPL"] = pd.to_numeric(top["cpl"], errors="coerce")
        top_view = top[["Vista", "Ver", "Campaña / Pieza", "Plataforma", "Gasto", "Conversiones", "CPL"]].copy()
        top_view["Vista"] = top_view["Vista"].fillna("")
        top_view["Ver"] = top_view["Ver"].fillna("")

        st_module.markdown("<div class='top-pieces-card'>", unsafe_allow_html=True)
        st_module.markdown(
            """
            <div class='top-pieces-head'>
              <h3 class='top-pieces-title'>Top 10 Piezas</h3>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if campaign_filters:
            active_labels = [
                f"{campaign_filter_options.get(k, k)}: {v}"
                for k, v in campaign_filters.items()
                if str(v).strip()
            ]
            if active_labels:
                st_module.caption(" | ".join(active_labels))
        st_module.dataframe(
            top_view,
            width="stretch",
            hide_index=True,
            column_config={
                "Vista": st_module.column_config.ImageColumn(
                    "Vista",
                    help="Vista previa de la pieza (si está disponible).",
                ),
                "Ver": st_module.column_config.LinkColumn("Ver", help="Abrir campaña/pieza", display_text="Abrir"),
                "Campaña / Pieza": st_module.column_config.TextColumn("Campaña / Pieza"),
                "Plataforma": st_module.column_config.TextColumn("Plataforma"),
                "Gasto": st_module.column_config.NumberColumn("Gasto", format="$%.2f"),
                "Conversiones": st_module.column_config.NumberColumn("Conversiones", format="%.0f"),
                "CPL": st_module.column_config.NumberColumn("CPL", format="$%.2f"),
            },
        )
        st_module.markdown("</div>", unsafe_allow_html=True)
        return

    has_piece_data = bool(
        isinstance(piece_df, pd.DataFrame)
        and not piece_df.empty
        and "date" in piece_df.columns
    )
    source_df = piece_df if has_piece_data else camp_df
    if source_df.empty:
        st_module.info("No hay datos de piezas/campañas para construir el top 10.")
        return

    cp = source_df.copy()
    if "date" not in cp.columns:
        st_module.info("El dataset de piezas no contiene columna de fecha.")
        return

    cp["date"] = pd.to_datetime(cp["date"], errors="coerce").dt.date
    cp = cp.dropna(subset=["date"])
    cp = cp[(cp["date"] >= start_ref) & (cp["date"] <= end_ref)]

    if platform in ("Google", "Meta") and "platform" in cp.columns:
        cp = cp[cp["platform"] == platform]
    cp = apply_campaign_filters_fn(cp, campaign_filters or {})

    def _clean_text(value: Any) -> str:
        txt = str(value or "").strip()
        return "" if txt.lower() in {"nan", "none", "null"} else txt

    def _first_non_empty(series: pd.Series) -> str:
        for raw in series.tolist():
            txt = _clean_text(raw)
            if txt:
                return txt
        return ""

    if has_piece_data:
        required_defaults: dict[str, Any] = {
            "platform": "",
            "campaign_id": "",
            "campaign_name": "",
            "piece_id": "",
            "piece_name": "",
            "ad_id": "",
            "ad_name": "",
            "image_url": "",
            "thumbnail_url": "",
            "preview_url": "",
            "spend": 0.0,
            "impressions": 0.0,
            "clicks": 0.0,
            "conversions": 0.0,
        }
        for col, default in required_defaults.items():
            if col not in cp.columns:
                cp[col] = default
        camp_fallback_all = pd.DataFrame()
        if isinstance(camp_df, pd.DataFrame) and not camp_df.empty and "date" in camp_df.columns:
            camp_fallback_all = camp_df.copy()
            camp_fallback_all["date"] = pd.to_datetime(camp_fallback_all["date"], errors="coerce").dt.date
            camp_fallback_all = camp_fallback_all.dropna(subset=["date"])
            camp_fallback_all = camp_fallback_all[
                (camp_fallback_all["date"] >= start_ref) & (camp_fallback_all["date"] <= end_ref)
            ]
            if platform in ("Google", "Meta") and "platform" in camp_fallback_all.columns:
                camp_fallback_all = camp_fallback_all[camp_fallback_all["platform"] == platform]
            camp_fallback_all = apply_campaign_filters_fn(camp_fallback_all, campaign_filters or {})
        piece_platforms = (
            set(cp["platform"].astype(str).str.strip())
            if "platform" in cp.columns
            else set()
        )
        fallback_platforms = (
            set(camp_fallback_all["platform"].astype(str).str.strip())
            if not camp_fallback_all.empty and "platform" in camp_fallback_all.columns
            else set()
        )
        if platform in ("Google", "Meta"):
            missing_platforms = {platform} - piece_platforms
        else:
            missing_platforms = fallback_platforms - piece_platforms
        if missing_platforms and not camp_fallback_all.empty:
            camp_fallback = camp_fallback_all[
                camp_fallback_all["platform"].astype(str).str.strip().isin(missing_platforms)
            ].copy()
            if not camp_fallback.empty:
                for col, default in required_defaults.items():
                    if col not in camp_fallback.columns:
                        camp_fallback[col] = default
                camp_fallback["piece_id"] = camp_fallback["campaign_id"].astype(str)
                camp_fallback["piece_name"] = camp_fallback["campaign_name"].astype(str)
                camp_fallback["ad_id"] = ""
                camp_fallback["ad_name"] = ""
                camp_fallback["image_url"] = ""
                camp_fallback["thumbnail_url"] = ""
                camp_fallback["preview_url"] = ""
                for col in cp.columns:
                    if col not in camp_fallback.columns:
                        camp_fallback[col] = ""
                cp = pd.concat([cp, camp_fallback[cp.columns]], ignore_index=True)
        cp = cp.reset_index(drop=True)
        cp["piece_id"] = cp.apply(
            lambda r: (
                _clean_text(r.get("piece_id"))
                or _clean_text(r.get("ad_id"))
                or f"piece::{_clean_text(r.get('campaign_id')) or 'na'}::{r.name}"
            ),
            axis=1,
        )
        cp["piece_name"] = cp.apply(
            lambda r: (
                _clean_text(r.get("piece_name"))
                or _clean_text(r.get("ad_name"))
                or _clean_text(r.get("campaign_name"))
                or "Sin nombre"
            ),
            axis=1,
        )
        cp["preview_image"] = cp.apply(
            lambda r: (
                _clean_text(r.get("preview_url"))
                or _clean_text(r.get("image_url"))
                or _clean_text(r.get("thumbnail_url"))
            ),
            axis=1,
        )
        for num_col in ("spend", "impressions", "clicks", "conversions"):
            cp[num_col] = pd.to_numeric(cp[num_col], errors="coerce").fillna(0.0)

        if cp.empty:
            st_module.info("No hay piezas/campañas para el rango seleccionado.")
            return

        top = (
            cp.groupby(
                ["platform", "piece_id", "piece_name", "campaign_id", "campaign_name"],
                as_index=False,
            )
            .agg(
                inversion=("spend", "sum"),
                conversiones=("conversions", "sum"),
                clics=("clicks", "sum"),
                vista_previa=("preview_image", _first_non_empty),
            )
        )
        top["Ver"] = top.apply(
            lambda r: piece_platform_link_fn(
                r.get("platform"),
                piece_id=r.get("piece_id"),
                campaign_id=r.get("campaign_id"),
                meta_account_id=tenant_meta_account_id,
                google_customer_id=tenant_google_customer_id,
            ),
            axis=1,
        )
        top["Campaña / Pieza"] = top["piece_name"].astype(str).replace({"": "Sin nombre"})
        top["Vista"] = top["vista_previa"].astype(str).replace({"nan": "", "None": ""})
    else:
        required_defaults = {
            "platform": "",
            "campaign_id": "",
            "campaign_name": "",
            "spend": 0.0,
            "impressions": 0.0,
            "clicks": 0.0,
            "conversions": 0.0,
        }
        for col, default in required_defaults.items():
            if col not in cp.columns:
                cp[col] = default
        for num_col in ("spend", "impressions", "clicks", "conversions"):
            cp[num_col] = pd.to_numeric(cp[num_col], errors="coerce").fillna(0.0)

        if cp.empty:
            st_module.info("No hay piezas/campañas para el rango seleccionado.")
            return

        top = (
            cp.groupby(["platform", "campaign_id", "campaign_name"], as_index=False)
            .agg(
                inversion=("spend", "sum"),
                conversiones=("conversions", "sum"),
                clics=("clicks", "sum"),
            )
        )
        top["Ver"] = top.apply(
            lambda r: campaign_platform_link_fn(
                r.get("platform"),
                r.get("campaign_id"),
                meta_account_id=tenant_meta_account_id,
                google_customer_id=tenant_google_customer_id,
            ),
            axis=1,
        )
        top["Campaña / Pieza"] = top["campaign_name"].astype(str).replace({"": "Sin nombre"})
        top["Vista"] = ""

    top["cpl"] = top.apply(lambda r: sdiv_fn(float(r["inversion"]), float(r["conversiones"])), axis=1)
    top = top.sort_values(["conversiones", "clics"], ascending=[False, False], na_position="last").head(10)
    top["Plataforma"] = top["platform"].astype(str).replace({"": "N/A"})
    top["Gasto"] = pd.to_numeric(top["inversion"], errors="coerce")
    top["Conversiones"] = pd.to_numeric(top["conversiones"], errors="coerce")
    top["CPL"] = pd.to_numeric(top["cpl"], errors="coerce")
    top_view = top[["Vista", "Ver", "Campaña / Pieza", "Plataforma", "Gasto", "Conversiones", "CPL"]].copy()
    top_view["Vista"] = top_view["Vista"].fillna("")
    top_view["Ver"] = top_view["Ver"].fillna("")

    st_module.markdown("<div class='top-pieces-card'>", unsafe_allow_html=True)
    st_module.markdown(
        """
        <div class='top-pieces-head'>
          <h3 class='top-pieces-title'>Top 10 Piezas</h3>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if campaign_filters:
        active_labels = [
            f"{campaign_filter_options.get(k, k)}: {v}"
            for k, v in campaign_filters.items()
            if str(v).strip()
        ]
        if active_labels:
            st_module.caption(" | ".join(active_labels))
    st_module.dataframe(
        top_view,
        width="stretch",
        hide_index=True,
        column_config={
            "Vista": st_module.column_config.ImageColumn(
                "Vista",
                help="Vista previa de la pieza (si está disponible).",
            ),
            "Ver": st_module.column_config.LinkColumn("Ver", help="Abrir campaña/pieza", display_text="Abrir"),
            "Campaña / Pieza": st_module.column_config.TextColumn("Campaña / Pieza"),
            "Plataforma": st_module.column_config.TextColumn("Plataforma"),
            "Gasto": st_module.column_config.NumberColumn("Gasto", format="$%.2f"),
            "Conversiones": st_module.column_config.NumberColumn("Conversiones", format="%.0f"),
            "CPL": st_module.column_config.NumberColumn("CPL", format="$%.2f"),
        },
    )
    st_module.markdown("</div>", unsafe_allow_html=True)
