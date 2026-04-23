"""
Admin Panel Page
================
Operational visibility for the DES_ADMIN_FR role.

Layout:
    Row 1 — Pipeline health KPIs (null counts)
    Row 2 — Enrichment status breakdown by STARS (stacked bar)
             + Ingestion timeline (line chart)
    Row 3 — Event log table (from DB_ADMIN_DES.LOGS.TB_LOGS)
"""

import pandas as pd
import plotly.express as px
import streamlit as st
from snowflake.snowpark import Session

import importlib.util
from pathlib import Path

_SRVCS = Path(__file__).resolve().parents[1] / "2__services"

def _load(path):
    spec = importlib.util.spec_from_file_location(path.stem.split("__")[-1], path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

_q = _load(_SRVCS / "R__4.2.1__queries.py")
get_enrichment_status_breakdown = _q.get_enrichment_status_breakdown
get_event_logs = _q.get_event_logs
get_ingestion_timeline = _q.get_ingestion_timeline
get_null_stats = _q.get_null_stats


STATUS_COLORS = {
    "FULLY_ENRICHED": "#276749",
    "FAST_ONLY":      "#2b6cb0",
}


def _health_kpis(stats: dict) -> None:
    """Render pipeline health metrics — rows missing key AI fields.

    Args:
        stats: Dict returned by ``get_null_stats()``.
    """
    c1, c2, c3 = st.columns(3)
    c1.metric("Missing Sentiment", int(stats.get("no_sentiment") or 0),
              delta_color="inverse")
    c2.metric("Missing Keywords",  int(stats.get("no_keywords") or 0),
              delta_color="inverse")
    c3.metric("Missing Body",      int(stats.get("no_body") or 0),
              delta_color="inverse")


def _enrichment_breakdown(df: pd.DataFrame) -> None:
    """Render enrichment status breakdown as a stacked bar per STARS.

    Args:
        df: DataFrame from ``get_enrichment_status_breakdown()``.
    """
    st.subheader("Enrichment Status by Rating")
    if df.empty:
        st.info("No data available.")
        return

    fig = px.bar(
        df,
        x="STARS",
        y="COUNT",
        color="ENRICHMENT_STATUS",
        color_discrete_map=STATUS_COLORS,
        barmode="stack",
        labels={"COUNT": "Reviews", "STARS": "Stars", "ENRICHMENT_STATUS": "Status"},
        text_auto=True,
    )
    fig.update_layout(
        margin=dict(t=10, b=10, l=0, r=0),
        height=340,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickmode="linear", gridcolor="#e2e8f0"),
        yaxis=dict(gridcolor="#e2e8f0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)


def _timeline_chart(df: pd.DataFrame) -> None:
    """Render the reviews-enriched-per-hour ingestion timeline.

    Args:
        df: DataFrame from ``get_ingestion_timeline()``.
    """
    st.subheader("Ingestion Timeline")
    if df.empty:
        st.info("No timeline data available.")
        return

    fig = px.line(
        df,
        x="HOUR",
        y="COUNT",
        markers=True,
        labels={"HOUR": "Hour", "COUNT": "Reviews Enriched"},
        color_discrete_sequence=["#2b6cb0"],
    )
    fig.update_layout(
        margin=dict(t=10, b=10, l=0, r=0),
        height=340,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="#e2e8f0"),
        yaxis=dict(gridcolor="#e2e8f0"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _event_log_table(df: pd.DataFrame) -> None:
    """Render the event log table from TB_LOGS.

    Args:
        df: DataFrame from ``get_event_logs()``.
    """
    st.subheader("Event Log (last 50 entries)")
    if df.empty:
        st.info("No log entries found. TB_LOGS may be empty or not accessible with this role.")
        return

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=380,
        column_config={
            "TIMESTAMP": st.column_config.DatetimeColumn("Timestamp", width="medium"),
            "SEVERITY":  st.column_config.TextColumn("Severity",  width="small"),
            "MESSAGE":   st.column_config.TextColumn("Message",   width="large"),
        },
    )


def render(session: Session) -> None:
    st.title("Admin Panel")
    st.caption("Pipeline health, enrichment coverage, and operational logs.")

    with st.spinner("Loading admin data..."):
        try:
            null_stats   = get_null_stats(session)
            breakdown_df = get_enrichment_status_breakdown(session)
            timeline_df  = get_ingestion_timeline(session)
            logs_df      = get_event_logs(session)
        except Exception as e:
            st.error(f"Failed to load admin data: {e}")
            return

    st.subheader("Pipeline Health")
    _health_kpis(null_stats)
    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        _enrichment_breakdown(breakdown_df)
    with col_right:
        _timeline_chart(timeline_df)

    st.divider()
    _event_log_table(logs_df)
