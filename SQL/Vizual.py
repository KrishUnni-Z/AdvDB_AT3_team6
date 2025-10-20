import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta
import json
import os

# ------------------ Page Configuration ------------------
st.set_page_config(
    page_title="Netflix Analytics",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------ Global Styles (Professional Dark) ------------------
st.markdown("""
<style>
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0f1419;
    color: #e5e7eb;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
}
[data-testid="stSidebar"] { background-color: #1a1f2e; }
.main-header { font-size: 2.5rem; font-weight: 800; color: #ffffff; margin-bottom: 0.5rem; letter-spacing: -1px; }
.sub-header { color: #9ca3af; font-size: 1rem; font-weight: 500; margin-top: 0; margin-bottom: 1.5rem; }
.metric-card { background: #1a1f2e; padding: 1.25rem; border-radius: 12px; border: 1px solid #2d3748; }
.stTabs [data-baseweb="tab-list"] { gap: 12px; }
.stTabs [data-baseweb="tab"]{ border-radius: 8px 8px 0 0; padding: 12px 20px; font-weight: 600;
    background-color: #1a1f2e; color: #9ca3af; border: 1px solid #2d3748; }
.stTabs [data-baseweb="tab"][aria-selected="true"] { background-color: #2d3748; border-bottom-color: #2d3748; color: #ffffff; }
div[data-testid="stMetricValue"] { font-size: 1.25rem; font-weight: 700; color: #ffffff; } /* smaller metrics globally */
div[data-testid="stMetricLabel"] { color: #9ca3af; font-size:.85rem; }
/* --- Mini metric widgets --- */
.mini-metric{background:#1a1f2e;border:1px solid #2d3748;border-radius:10px;padding:10px 12px}
.mini-metric .mm-label{font-size:.85rem;color:#9ca3af;white-space:nowrap}
.mini-metric .mm-value{font-size:1.25rem;font-weight:700;color:#fff;line-height:1.1}
.mini-metric .mm-delta{font-size:.85rem;color:#9ca3af}
.mini-metric-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:.75rem}
@media (max-width:1200px){.mini-metric-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media (max-width:768px){.mini-metric-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
.divider { border-top: 1px solid #2d3748; margin: 1.5rem 0; }
.info-box { background-color: #1a1f2e; border-left: 4px solid #3b82f6; color: #d1d5db; padding: 1rem; border-radius: 6px; }
</style>
""", unsafe_allow_html=True)

# Altair defaults (professional dark theme)
alt.data_transformers.disable_max_rows()
def configure_chart(chart):
    return (
        chart
        .configure_view(strokeWidth=0, fill='#0f1419')
        .configure_axis(labelColor='#9ca3af', titleColor='#d1d5db', gridColor='#2d3748', domainColor='#4b5563')
        .configure_legend(labelColor='#9ca3af', titleColor='#d1d5db', labelFontSize=12, titleFontSize=13)
        .configure_title(color='#ffffff', fontSize=14, anchor='start')
    )

session = get_active_session()

# ------------------ Helpers ------------------
@st.cache_data(ttl=600, show_spinner="Loading data…")
def run_df(sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query error: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_date_bounds() -> tuple[pd.Timestamp, pd.Timestamp]:
    df = run_df("SELECT MIN(occurred_date) AS min_d, MAX(occurred_date) AS max_d FROM DEMO_GOLD.DIM_TIME_T")
    if df.empty or pd.isna(df.loc[0, "MIN_D"]) or pd.isna(df.loc[0, "MAX_D"]):
        today = pd.Timestamp.today().normalize()
        return (today - pd.Timedelta(days=30), today)
    return (pd.to_datetime(df.loc[0, "MIN_D"]), pd.to_datetime(df.loc[0, "MAX_D"]))

def q_between_date(col, start, end):
    return f"{col} BETWEEN TO_DATE('{start:%Y-%m-%d}') AND TO_DATE('{end:%Y-%m-%d}')"

def prior_period(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    delta = end - start
    return (start - (delta + pd.Timedelta(days=1)), start - pd.Timedelta(days=1))

def fmt_int(n):
    try: return f"{int(n):,}"
    except Exception: return "—"

def fmt_pct(x, digits=2):
    if x is None or pd.isna(x): return "—"
    return f"{x*100:.{digits}f}%"

def fmt_delta(curr, prev):
    if prev in (0, None) or pd.isna(prev) or pd.isna(curr): return None
    return f"{((curr - prev) / prev) * 100:+.1f}%"

# Mini metric helper
def mini_metric(label: str, value: str, delta: str | None = None):
    delta_html = f"<div class='mm-delta'>{delta}</div>" if delta else ""
    st.markdown(
        f"<div class='mini-metric'><div class='mm-label'>{label}</div>"
        f"<div class='mm-value'>{value}</div>{delta_html}</div>",
        unsafe_allow_html=True
    )

# ------------------ Sidebar Filters ------------------
with st.sidebar:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
                padding: 1.25rem; border-radius: 12px; text-align: center; margin-bottom: 1.5rem;">
        <div style="color:#ffffff; font-size: 1.5rem; font-weight: 900; letter-spacing: -0.5px;">NETFLIX</div>
        <div style="color:#9ca3af; opacity: 0.9; margin-top: 6px; font-size: 0.9rem; font-weight: 500;">Analytics Dashboard</div>
    </div>
    """, unsafe_allow_html=True)
    st.header("Filters")

    min_d, max_d = get_date_bounds()

    preset = st.selectbox(
        "Quick date range",
        ["Custom", "Last 7 days", "Last 30 days", "Last 90 days", "This month", "Last month"],
        index=2
    )

    if preset == "Last 7 days":
        start_d, end_d = max_d - pd.Timedelta(days=7), max_d
    elif preset == "Last 30 days":
        start_d, end_d = max_d - pd.Timedelta(days=30), max_d
    elif preset == "Last 90 days":
        start_d, end_d = max_d - pd.Timedelta(days=90), max_d
    elif preset == "This month":
        start_d = pd.Timestamp(max_d.year, max_d.month, 1); end_d = max_d
    elif preset == "Last month":
        first_this = pd.Timestamp(max_d.year, max_d.month, 1)
        end_d = first_this - pd.Timedelta(days=1)
        start_d = pd.Timestamp(end_d.year, end_d.month, 1)
    else:
        default_start = max(min_d.date(), (max_d.date() - timedelta(days=30)))
        default_end = max_d.date()
        date_input = st.date_input(
            "Custom date range",
            (default_start, default_end),
            min_value=min_d.date(),
            max_value=max_d.date()
        )
        if isinstance(date_input, tuple) and len(date_input) == 2:
            start_d, end_d = [pd.to_datetime(d) for d in date_input]
        else:
            start_d = end_d = pd.to_datetime(date_input)

    st.caption(f"{start_d.date()} to {end_d.date()}")

    with st.expander("Country", expanded=False):
        countries = run_df("SELECT DISTINCT country FROM DEMO_GOLD.DIM_USER_T ORDER BY 1")
        country_opt = st.multiselect(
            "Select countries",
            [(c if pd.notna(c) else "unknown") for c in countries.get("COUNTRY", pd.Series([], dtype="object")).tolist()],
            default=[], label_visibility="collapsed"
        )

    scheme = st.selectbox("Chart colour scheme", ["darkblue", "category20b", "viridis"], index=0)

    if st.button("Reset filters", use_container_width=True):
        st.rerun()

# WHERE fragments for reuse
where_time = f" WHERE {q_between_date('TO_DATE(occurred_hour)', start_d, end_d)}"
if country_opt:
    in_c = ",".join([f"'{c}'" for c in country_opt])
    where_time += f" AND COALESCE(country,'unknown') IN ({in_c})"

country_list = ",".join([f"'{c}'" for c in country_opt]) if country_opt else ""
country_filter = f" AND COALESCE(country,'unknown') IN ({country_list})" if country_opt else ""

# ------------------ Header ------------------
st.markdown('<div class="main-header">Netflix Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Gold Layer Intelligence Dashboard</div>', unsafe_allow_html=True)

active_filters = []
if country_opt: active_filters.append(f"{len(country_opt)} country")
if active_filters:
    st.markdown(f'<div class="info-box">Active filters: {", ".join(active_filters)}</div>', unsafe_allow_html=True)

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

# ------------------ KPIs ------------------
pp_start, pp_end = prior_period(start_d, end_d)
where_time_bucket = where_time.replace('occurred_hour', 'hour_bucket')

kpi_curr_sql = f"""
SELECT
    (SELECT COUNT(*) FROM DEMO_GOLD.SL_FACT_SESSION {where_time_bucket}) AS sessions,
    (SELECT COUNT(*) FROM DEMO_GOLD.SL_FACT_ENGAGEMENT 
     WHERE (action='play' OR funnel_stage='conversion') AND {q_between_date('TO_DATE(occurred_at)', start_d, end_d)} {country_filter}) AS plays,
    (SELECT AVG(slot_quality_score) FROM DEMO_GOLD.SL_SLOT_QUALITY) AS avg_slot_quality
"""
pp_where_time = f" WHERE {q_between_date('TO_DATE(hour_bucket)', pp_start, pp_end)} {country_filter}"
kpi_prev_sql = f"""
SELECT
    (SELECT COUNT(*) FROM DEMO_GOLD.SL_FACT_SESSION {pp_where_time}) AS sessions,
    (SELECT COUNT(*) FROM DEMO_GOLD.SL_FACT_ENGAGEMENT 
     WHERE (action='play' OR funnel_stage='conversion') AND {q_between_date('TO_DATE(occurred_at)', pp_start, pp_end)} {country_filter}) AS plays
"""
kpi_curr = run_df(kpi_curr_sql)
kpi_prev = run_df(kpi_prev_sql)

if not kpi_curr.empty and not kpi_prev.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total sessions", fmt_int(kpi_curr.loc[0, "SESSIONS"]), fmt_delta(kpi_curr.loc[0, "SESSIONS"], kpi_prev.loc[0, "SESSIONS"]))
    c2.metric("Total plays", fmt_int(kpi_curr.loc[0, "PLAYS"]), fmt_delta(kpi_curr.loc[0, "PLAYS"], kpi_prev.loc[0, "PLAYS"]))
    c3.metric("Avg slot quality", fmt_pct(kpi_curr.loc[0, "AVG_SLOT_QUALITY"]), None)
    c4.metric("Period", f"{(end_d - start_d).days}d", None)

st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

# ==================== TABS ====================
tab1, tab2, tab3 = st.tabs(["Discovery Funnel", "Content Performance", "Experience"])

# ------------- TAB 1: DISCOVERY FUNNEL ------------- 
with tab1:
    st.markdown('<div class="section-header">Discovery funnel: tile → click → play</div>', unsafe_allow_html=True)

    df_funnel = run_df(f"""
    SELECT hr, surface, row_id, position, impressions, clicks, conversions, ctr, cvr_click_to_play, median_time_to_play_sec
    FROM DEMO_GOLD.SL_DISCOVERY_FUNNEL_BY_SURFACE
    WHERE {q_between_date('TO_DATE(hr)', start_d, end_d)}
    ORDER BY hr DESC, surface, row_id, position
    LIMIT 20000
    """)
    if not df_funnel.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("CTR trend by surface")
            avg_ctr = df_funnel["CTR"].mean()
            total_impressions = df_funnel["IMPRESSIONS"].sum()
            total_clicks = df_funnel["CLICKS"].sum()

            line_ctr = alt.Chart(df_funnel).mark_line(strokeWidth=2.5).encode(
                x=alt.X("HR:T", title="Date", axis=alt.Axis(format="%b %d")),
                y=alt.Y("CTR:Q", title="CTR", axis=alt.Axis(format="%")),
                color=alt.Color("SURFACE:N", scale=alt.Scale(scheme=scheme)),
                tooltip=["HR","SURFACE",alt.Tooltip("CTR:Q", format=".2%"),"IMPRESSIONS"]
            )
            avg_rule = alt.Chart(pd.DataFrame({'y': [avg_ctr]})).mark_rule(
                strokeDash=[5,5], color='#4b5563', size=1.2
            ).encode(y='y:Q')
            st.altair_chart(configure_chart((line_ctr + avg_rule).properties(height=300).interactive()), use_container_width=True)

            # mini metrics + insight
            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg CTR", fmt_pct(avg_ctr))
            mini_metric("Total impressions", fmt_int(total_impressions))
            mini_metric("Total clicks", fmt_int(total_clicks))
            st.markdown("</div>", unsafe_allow_html=True)

            top_ctr_surface = df_funnel.groupby("SURFACE")["CTR"].mean().sort_values(ascending=False).index[0]
            st.caption(f"**What this shows:** CTR averages {fmt_pct(avg_ctr)}; **{top_ctr_surface}** leads on CTR in the selected range.")

        with col2:
            st.markdown("CVR (Click→Play) trend by surface")
            avg_cvr = df_funnel["CVR_CLICK_TO_PLAY"].mean()
            total_conversions = df_funnel["CONVERSIONS"].sum()
            median_ttplay = df_funnel["MEDIAN_TIME_TO_PLAY_SEC"].median()

            line_cvr = alt.Chart(df_funnel).mark_line(strokeWidth=2.5).encode(
                x=alt.X("HR:T", title="Date", axis=alt.Axis(format="%b %d")),
                y=alt.Y("CVR_CLICK_TO_PLAY:Q", title="CVR", axis=alt.Axis(format="%")),
                color=alt.Color("SURFACE:N", scale=alt.Scale(scheme=scheme)),
                tooltip=["HR","SURFACE",alt.Tooltip("CVR_CLICK_TO_PLAY:Q", format=".2%")]
            )
            avg_rule_cvr = alt.Chart(pd.DataFrame({'y': [avg_cvr]})).mark_rule(
                strokeDash=[5,5], color='#4b5563', size=1.2
            ).encode(y='y:Q')
            st.altair_chart(configure_chart((line_cvr + avg_rule_cvr).properties(height=300).interactive()), use_container_width=True)

            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg CVR", fmt_pct(avg_cvr))
            mini_metric("Total conversions", fmt_int(total_conversions))
            mini_metric("Median time to play", f"{median_ttplay:.0f}s" if pd.notna(median_ttplay) else "—")
            st.markdown("</div>", unsafe_allow_html=True)

            top_cvr_surface = df_funnel.groupby("SURFACE")["CVR_CLICK_TO_PLAY"].mean().sort_values(ascending=False).index[0]
            med_str = f"{int(median_ttplay)}s" if pd.notna(median_ttplay) else "—"
            st.caption(f"**What this shows:** Avg CVR is {fmt_pct(avg_cvr)}; **{top_cvr_surface}** converts best. Median time to play ≈ {med_str}.")

        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Best performing slots</div>', unsafe_allow_html=True)

        df_slots = run_df(f"""
        SELECT surface, row_id, position,
               AVG(ctr) AS avg_ctr, AVG(cvr_click_to_play) AS avg_cvr,
               SUM(impressions) AS impressions
        FROM DEMO_GOLD.SL_DISCOVERY_FUNNEL_BY_SURFACE
        WHERE {q_between_date('TO_DATE(hr)', start_d, end_d)}
        GROUP BY surface, row_id, position
        HAVING SUM(impressions) > 50
        ORDER BY avg_cvr DESC, avg_ctr DESC
        LIMIT 30
        """)
        if not df_slots.empty:
            df_slots['SLOT'] = df_slots['SURFACE'] + ' - ' + df_slots['ROW_ID'].astype(str) + '/' + df_slots['POSITION'].astype(str)
            avg_ctr_scatter = df_slots['AVG_CTR'].mean()
            avg_cvr_scatter = df_slots['AVG_CVR'].mean()
            total_slot_imps = df_slots['IMPRESSIONS'].sum()
            num_slots = len(df_slots)

            scatter = alt.Chart(df_slots).mark_point(size=200, opacity=0.85).encode(
                x=alt.X("AVG_CTR:Q", title="CTR", scale=alt.Scale(zero=False)),
                y=alt.Y("AVG_CVR:Q", title="CVR", scale=alt.Scale(zero=False)),
                size=alt.Size("IMPRESSIONS:Q", scale=alt.Scale(range=[80, 600]), legend=alt.Legend(title="Impressions")),
                color=alt.Color("AVG_CVR:Q", scale=alt.Scale(scheme="greens"), legend=alt.Legend(title="Avg CVR")),
                tooltip=["SLOT", alt.Tooltip("AVG_CTR:Q", format=".2%"), alt.Tooltip("AVG_CVR:Q", format=".2%"), alt.Tooltip("IMPRESSIONS:Q", format=",")]
            )
            vline = alt.Chart(pd.DataFrame({'x': [avg_ctr_scatter]})).mark_rule(strokeDash=[5,5], color='#4b5563', size=1.2).encode(x='x:Q')
            hline = alt.Chart(pd.DataFrame({'y': [avg_cvr_scatter]})).mark_rule(strokeDash=[5,5], color='#4b5563', size=1.2).encode(y='y:Q')
            st.altair_chart(configure_chart((scatter + vline + hline).properties(height=400).interactive()), use_container_width=True)

            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg CTR", fmt_pct(avg_ctr_scatter))
            mini_metric("Avg CVR", fmt_pct(avg_cvr_scatter))
            mini_metric("Total impressions", fmt_int(total_slot_imps))
            mini_metric("Unique slots", fmt_int(num_slots))
            st.markdown("</div>", unsafe_allow_html=True)

            st.caption(
                f"**What this shows:** Among {fmt_int(num_slots)} slots, avg CTR is {fmt_pct(avg_ctr_scatter)} and CVR is {fmt_pct(avg_cvr_scatter)}; bigger bubbles = more impressions."
            )

        st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-header">Exposure decay</div>', unsafe_allow_html=True)

        df_decay = run_df("SELECT * FROM DEMO_GOLD.SL_EXPOSURE_DECAY ORDER BY exposure_seq")
        if not df_decay.empty:
            avg_conv = df_decay["CONV_RATE"].mean()
            max_exp = df_decay["EXPOSURE_SEQ"].max()
            total_decay_imps = df_decay["IMPRESSIONS"].sum()
            total_decay_cvs = df_decay["CONVERSIONS"].sum()

            line = alt.Chart(df_decay).mark_line(strokeWidth=2.5).encode(
                x=alt.X("EXPOSURE_SEQ:O", title="Exposure number"),
                y=alt.Y("CONV_RATE:Q", title="Conversion rate", axis=alt.Axis(format="%")),
                color=alt.value('#3b82f6'),
                tooltip=["EXPOSURE_SEQ","IMPRESSIONS","CLICKS","CONVERSIONS",alt.Tooltip("CONV_RATE:Q", format=".2%")]
            )
            points = alt.Chart(df_decay).mark_point(size=100, filled=True, color='#3b82f6').encode(
                x="EXPOSURE_SEQ:O", y="CONV_RATE:Q"
            )
            avg_line = alt.Chart(pd.DataFrame({'y': [avg_conv]})).mark_rule(
                strokeDash=[5,5], color='#4b5563', size=1.2
            ).encode(y='y:Q')
            st.altair_chart(configure_chart((line + points + avg_line).properties(height=300).interactive()), use_container_width=True)

            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg conversion rate", fmt_pct(avg_conv))
            mini_metric("Max exposures", fmt_int(max_exp))
            mini_metric("Total impressions", fmt_int(total_decay_imps))
            mini_metric("Total conversions", fmt_int(total_decay_cvs))
            st.markdown("</div>", unsafe_allow_html=True)

            st.caption(f"**What this shows:** Conversion rate averages {fmt_pct(avg_conv)} and typically declines with each exposure (diminishing returns).")

# ------------- TAB 2: CONTENT PERFORMANCE -----------
with tab2:
    st.markdown('<div class="section-header">Slot quality scorecard</div>', unsafe_allow_html=True)

    df_quality = run_df("""
    SELECT surface, row_id, position, ctr, cvr, avg_dwell_ms, avg_preview_ms, slot_quality_score, quality_rank
    FROM DEMO_GOLD.SL_SLOT_QUALITY
    ORDER BY quality_rank
    LIMIT 50
    """)
    if not df_quality.empty:
        col1, col2 = st.columns([2, 1])

        with col1:
            df_quality['SLOT'] = df_quality['SURFACE'] + ' - Row ' + df_quality['ROW_ID'].astype(str) + ' / Pos ' + df_quality['POSITION'].astype(str)
            chart_q = alt.Chart(df_quality.head(20)).mark_bar().encode(
                x=alt.X("SLOT_QUALITY_SCORE:Q", title="Quality score"),
                y=alt.Y("SLOT:N", sort="-x", title=None),
                color=alt.Color("SLOT_QUALITY_SCORE:Q", scale=alt.Scale(scheme="greens"), legend=None),
                tooltip=["SLOT", alt.Tooltip("SLOT_QUALITY_SCORE:Q", format=".3f"), alt.Tooltip("CTR:Q", format=".2%"), alt.Tooltip("CVR:Q", format=".2%")]
            )
            st.altair_chart(configure_chart(chart_q.properties(height=380)), use_container_width=True)

        with col2:
            st.markdown("Top 10 slots")
            top10 = df_quality.head(10)[['SLOT','QUALITY_RANK','SLOT_QUALITY_SCORE','CTR','CVR']].copy()
            top10.columns = ['Slot','Rank','Score','CTR','CVR']
            st.dataframe(
                top10.style.format({'Score':'{:.3f}','CTR':'{:.2%}','CVR':'{:.2%}'}),
                use_container_width=True, hide_index=True
            )

        avg_quality = df_quality['SLOT_QUALITY_SCORE'].mean()
        avg_ctr_quality = df_quality['CTR'].mean()
        avg_cvr_quality = df_quality['CVR'].mean()

        st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
        mini_metric("Avg slot quality", fmt_pct(avg_quality))
        mini_metric("Avg CTR across slots", fmt_pct(avg_ctr_quality))
        mini_metric("Avg CVR across slots", fmt_pct(avg_cvr_quality))
        st.markdown("</div>", unsafe_allow_html=True)

        st.caption("**What this shows:** Slots at the top combine high CTR/CVR and stronger micro-intent (dwell/preview), yielding higher quality scores.")

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Micro-intent: dwell & preview impact</div>', unsafe_allow_html=True)

    df_micro = run_df(f"""
    SELECT day_date, surface, row_id, position, impressions, avg_dwell_ms, avg_preview_watch_ms, ctr, cvr_click_to_play
    FROM DEMO_GOLD.SL_MICRO_INTENT
    WHERE {q_between_date('TO_DATE(day_date)', start_d, end_d)}
    ORDER BY day_date DESC, ctr DESC
    LIMIT 10000
    """)
    if not df_micro.empty:
        col1, col2 = st.columns(2)

        with col1:
            df_dwell = df_micro.groupby('SURFACE').agg({'AVG_DWELL_MS':'mean','CTR':'mean'}).reset_index()
            chart_dwell = alt.Chart(df_dwell).mark_bar().encode(
                x=alt.X("AVG_DWELL_MS:Q", title="Avg dwell (ms)"),
                y=alt.Y("SURFACE:N", title=None),
                color=alt.Color("CTR:Q", scale=alt.Scale(scheme="blues"), legend=alt.Legend(title="CTR")),
                tooltip=["SURFACE", alt.Tooltip("AVG_DWELL_MS:Q", format=".0f"), alt.Tooltip("CTR:Q", format=".2%")]
            )
            st.altair_chart(configure_chart(chart_dwell.properties(height=300)), use_container_width=True)

        with col2:
            df_preview = df_micro.groupby('SURFACE').agg({'AVG_PREVIEW_WATCH_MS':'mean','CVR_CLICK_TO_PLAY':'mean'}).reset_index()
            chart_preview = alt.Chart(df_preview).mark_bar().encode(
                x=alt.X("AVG_PREVIEW_WATCH_MS:Q", title="Avg preview watch (ms)"),
                y=alt.Y("SURFACE:N", title=None),
                color=alt.Color("CVR_CLICK_TO_PLAY:Q", scale=alt.Scale(scheme="greens"), legend=alt.Legend(title="CVR")),
                tooltip=["SURFACE", alt.Tooltip("AVG_PREVIEW_WATCH_MS:Q", format=".0f"), alt.Tooltip("CVR_CLICK_TO_PLAY:Q", format=".2%")]
            )
            st.altair_chart(configure_chart(chart_preview.properties(height=300)), use_container_width=True)

        st.caption("**What this shows:** Longer dwell/preview generally correlates with higher CTR/CVR; use this to tune tile designs and previews.")

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Time to value: first touch to play</div>', unsafe_allow_html=True)

    df_ttv = run_df(f"""
    SELECT day_date, touched_users, users_with_play, same_day_touch_to_play_rate, avg_minutes_to_value
    FROM DEMO_GOLD.SL_TIME_TO_VALUE
    WHERE {q_between_date('TO_DATE(day_date)', start_d, end_d)}
    ORDER BY day_date
    """)
    if not df_ttv.empty:
        df_ttv_agg = df_ttv.agg({
            'TOUCHED_USERS':'sum',
            'USERS_WITH_PLAY':'sum',
            'SAME_DAY_TOUCH_TO_PLAY_RATE':'mean',
            'AVG_MINUTES_TO_VALUE':'mean'
        })
        st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
        mini_metric("Total touched users", fmt_int(df_ttv_agg['TOUCHED_USERS']))
        mini_metric("Users with play", fmt_int(df_ttv_agg['USERS_WITH_PLAY']))
        mini_metric("Touch→Play rate", fmt_pct(df_ttv_agg['SAME_DAY_TOUCH_TO_PLAY_RATE']))
        mini_metric("Avg time to play", f"{df_ttv_agg['AVG_MINUTES_TO_VALUE']:.0f}m")
        st.markdown("</div>", unsafe_allow_html=True)

        line_ttv = alt.Chart(df_ttv).mark_line(strokeWidth=2.5, point=True).encode(
            x=alt.X("DAY_DATE:T", title="Date"),
            y=alt.Y("SAME_DAY_TOUCH_TO_PLAY_RATE:Q", title="Touch→Play rate", axis=alt.Axis(format="%")),
            color=alt.value('#10b981'),
            tooltip=["DAY_DATE", alt.Tooltip("SAME_DAY_TOUCH_TO_PLAY_RATE:Q", format=".2%"),
                     alt.Tooltip("TOUCHED_USERS:Q", format=","), alt.Tooltip("USERS_WITH_PLAY:Q", format=",")]
        )
        st.altair_chart(configure_chart(line_ttv.properties(height=300).interactive()), use_container_width=True)

        st.caption("**What this shows:** Same-day touch→play averages "
                   f"{fmt_pct(df_ttv_agg['SAME_DAY_TOUCH_TO_PLAY_RATE'])}; average time to play is "
                   f"{int(df_ttv_agg['AVG_MINUTES_TO_VALUE'])} minutes.")

# ------------- TAB 3: EXPERIENCE ---------------------
with tab3:
    st.markdown('<div class="section-header">Friction-adjusted CVR</div>', unsafe_allow_html=True)

    df_fadj = run_df(f"""
    SELECT day_date, surface, row_id, position, impressions, clicks, conversions, ctr, cvr, cvr_adj_friction, avg_friction_score
    FROM DEMO_GOLD.SL_FRICTION_ADJUSTED_CVR
    WHERE {q_between_date('TO_DATE(day_date)', start_d, end_d)}
    ORDER BY day_date DESC
    LIMIT 10000
    """)
    if not df_fadj.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("CVR vs friction-adjusted CVR")
            df_fadj_agg = df_fadj.groupby('DAY_DATE').agg({'CVR':'mean','CVR_ADJ_FRICTION':'mean'}).reset_index()
            df_fadj_melt = df_fadj_agg.melt('DAY_DATE', var_name='Metric', value_name='Rate')
            df_fadj_melt['Metric'] = df_fadj_melt['Metric'].map({
                'CVR':'CVR (unadjusted)', 'CVR_ADJ_FRICTION':'CVR (friction-adjusted)'
            })
            line_fadj = alt.Chart(df_fadj_melt).mark_line(strokeWidth=2.5).encode(
                x=alt.X("DAY_DATE:T", title="Date"),
                y=alt.Y("Rate:Q", title="CVR", axis=alt.Axis(format="%")),
                color=alt.Color("Metric:N", scale=alt.Scale(scheme=scheme)),
                tooltip=["DAY_DATE","Metric",alt.Tooltip("Rate:Q", format=".2%")]
            )
            st.altair_chart(configure_chart(line_fadj.properties(height=300).interactive()), use_container_width=True)

            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg CVR (unadj.)", fmt_pct(df_fadj_agg['CVR'].mean()))
            mini_metric("Avg CVR (adj.)", fmt_pct(df_fadj_agg['CVR_ADJ_FRICTION'].mean()))
            st.markdown("</div>", unsafe_allow_html=True)

            st.caption("**What this shows:** Adjusting for measured friction (join time, rebuffer) narrows gaps; reducing friction should lift realized CVR toward adjusted CVR.")

        with col2:
            st.markdown("Avg friction score trend")
            df_friction_trend = df_fadj.groupby('DAY_DATE')['AVG_FRICTION_SCORE'].mean().reset_index()
            area_friction = alt.Chart(df_friction_trend).mark_area(
                line={'color':'#ef4444','strokeWidth':2.5},
                color=alt.Gradient(gradient='linear',
                                   stops=[alt.GradientStop(color='#ef4444', offset=0),
                                          alt.GradientStop(color='#7f1d1d', offset=1)],
                                   x1=1, x2=1, y1=1, y2=0)
            ).encode(
                x=alt.X("DAY_DATE:T", title="Date"),
                y=alt.Y("AVG_FRICTION_SCORE:Q", title="Friction score"),
                tooltip=["DAY_DATE", alt.Tooltip("AVG_FRICTION_SCORE:Q", format=".3f")]
            )
            st.altair_chart(configure_chart(area_friction.properties(height=300).interactive()), use_container_width=True)

            avg_friction = df_friction_trend['AVG_FRICTION_SCORE'].mean()
            min_friction = df_friction_trend['AVG_FRICTION_SCORE'].min()
            max_friction = df_friction_trend['AVG_FRICTION_SCORE'].max()

            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg friction", f"{avg_friction:.3f}")
            mini_metric("Min friction", f"{min_friction:.3f}")
            mini_metric("Max friction", f"{max_friction:.3f}")
            st.markdown("</div>", unsafe_allow_html=True)

            st.caption("**What this shows:** Track this line; sustained upticks in friction often precede CVR drops by hours.")

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">Quality breakdown by surface</div>', unsafe_allow_html=True)

    if not df_fadj.empty:
        df_surf = df_fadj.groupby('SURFACE').agg({
            'AVG_FRICTION_SCORE':'mean',
            'CVR':'mean',
            'CVR_ADJ_FRICTION':'mean',
            'IMPRESSIONS':'sum'
        }).reset_index().sort_values('CVR_ADJ_FRICTION', ascending=False)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("Unadjusted CVR by surface")
            bar_cvr = alt.Chart(df_surf).mark_bar().encode(
                x=alt.X("CVR:Q", title="CVR", axis=alt.Axis(format="%")),
                y=alt.Y("SURFACE:N", sort="-x", title=None),
                color=alt.value('#3b82f6'),
                tooltip=["SURFACE", alt.Tooltip("CVR:Q", format=".2%"), alt.Tooltip("IMPRESSIONS:Q", format=",")]
            )
            st.altair_chart(configure_chart(bar_cvr.properties(height=280)), use_container_width=True)
            st.caption(f"**What this shows:** Baseline conversion by surface; use alongside impressions to weigh impact.")

        with col2:
            st.markdown("Friction-adjusted CVR by surface")
            bar_adj = alt.Chart(df_surf).mark_bar().encode(
                x=alt.X("CVR_ADJ_FRICTION:Q", title="CVR (friction-adj)", axis=alt.Axis(format="%")),
                y=alt.Y("SURFACE:N", sort="-x", title=None),
                color=alt.value('#10b981'),
                tooltip=["SURFACE", alt.Tooltip("CVR_ADJ_FRICTION:Q", format=".2%"), alt.Tooltip("AVG_FRICTION_SCORE:Q", format=".3f")]
            )
            st.altair_chart(configure_chart(bar_adj.properties(height=280)), use_container_width=True)

            st.markdown("<div class='mini-metric-grid'>", unsafe_allow_html=True)
            mini_metric("Avg CVR (adj)", fmt_pct(df_surf['CVR_ADJ_FRICTION'].mean()))
            mini_metric("Avg friction", f"{df_surf['AVG_FRICTION_SCORE'].mean():.3f}")
            st.markdown("</div>", unsafe_allow_html=True)

            st.caption("**What this shows:** Surfaces with lower average friction tend to show higher adjusted CVR; prioritize mitigations there first.")

# ------------------ Footer ------------------
st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)
with col1:
    st.caption("Data layer: DEMO_GOLD (fact & KPI views)")
with col2:
    st.caption("Stack: Snowflake, Snowpark, Streamlit")
with col3:
    st.caption("Updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
