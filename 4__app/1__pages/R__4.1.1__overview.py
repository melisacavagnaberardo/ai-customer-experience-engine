"""
Overview Page
=============
Landing page after login. Shows headline KPIs and sentiment distribution charts.

Layout:
    Row 1 — 4 KPI metric cards
    Row 2 — Donut (STARS distribution) + Bar (avg sentiment per STARS)
    Row 3 — Table of most polarised reviews
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
get_kpis = _q.get_kpis
get_sentiment_by_stars = _q.get_sentiment_by_stars
get_stars_distribution = _q.get_stars_distribution
get_top_reviews = _q.get_top_reviews


# ---------------------------------------------------------------------------
# Palette — consistent with the project design token
# ---------------------------------------------------------------------------
STAR_COLORS = {1: "#c53030", 2: "#dd6b20", 3: "#d69e2e", 4: "#2b6cb0", 5: "#276749"}
NAVY = "#1a3a5c"


def _kpi_row(kpis: dict) -> None:
    """Render the four headline metric cards.

    Args:
        kpis: Dict returned by ``get_kpis()``.
    """
    c1, c2, c3, c4 = st.columns(4)

    total   = int(kpis.get("total") or 0)
    avg_s   = float(kpis.get("avg_sentiment") or 0)
    pct_fe  = float(kpis.get("pct_fully_enriched") or 0)
    pct_neg = float(kpis.get("pct_negative") or 0)

    sentiment_delta = f"{avg_s:+.3f}"

    c1.metric("Total Reviews",       f"{total:,}")
    c2.metric("Avg Sentiment",        f"{avg_s:.3f}",  sentiment_delta)
    c3.metric("Fully Enriched",       f"{pct_fe:.1f}%")
    c4.metric("Negative Sentiment",   f"{pct_neg:.1f}%")


def _charts_row(dist_df: pd.DataFrame, sentiment_df: pd.DataFrame) -> None:
    """Render the STARS donut and sentiment bar chart side by side.

    Args:
        dist_df: DataFrame from ``get_stars_distribution()``.
        sentiment_df: DataFrame from ``get_sentiment_by_stars()``.
    """
    col_left, col_right = st.columns(2)

    # -- Donut: STARS distribution --
    with col_left:
        st.subheader("Rating Distribution")
        if dist_df.empty:
            st.info("No data available.")
        else:
            colors = [STAR_COLORS.get(s, NAVY) for s in dist_df["STARS"]]
            fig = go.Figure(go.Pie(
                labels=[f"{s} star" for s in dist_df["STARS"]],
                values=dist_df["COUNT"],
                hole=0.55,
                marker_colors=colors,
                textinfo="percent+label",
                hovertemplate="%{label}: %{value:,} reviews<extra></extra>",
            ))
            fig.update_layout(
                showlegend=False,
                margin=dict(t=20, b=20, l=0, r=0),
                height=320,
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)

    # -- Bar: avg sentiment per STARS --
    with col_right:
        st.subheader("Avg Sentiment by Rating")
        if sentiment_df.empty:
            st.info("No data available.")
        else:
            colors = [STAR_COLORS.get(s, NAVY) for s in sentiment_df["STARS"]]
            colors = [STAR_COLORS.get(int(s), NAVY) for s in sentiment_df["STARS"]]
            fig = px.bar(
                sentiment_df,
                x="STARS",
                y="AVG_SENTIMENT",
                labels={"STARS": "Stars", "AVG_SENTIMENT": "Avg Sentiment"},
                text_auto=".3f",
            )
            fig.update_traces(marker_color=colors)
            fig.add_hline(y=0, line_dash="dot", line_color="#718096", line_width=1)
            fig.update_layout(
                showlegend=False,
                margin=dict(t=20, b=20, l=0, r=0),
                height=320,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis=dict(range=[-1, 1], gridcolor="#e2e8f0"),
                xaxis=dict(tickmode="linear"),
            )
            st.plotly_chart(fig, use_container_width=True)


def _top_reviews_table(df: pd.DataFrame) -> None:
    """Render the most polarised reviews table.

    Args:
        df: DataFrame from ``get_top_reviews()``.
    """
    st.subheader("Most Polarised Reviews")
    if df.empty:
        st.info("No data available.")
        return

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "SENTIMENT":   st.column_config.NumberColumn("Sentiment", format="%.3f"),
            "STARS":       st.column_config.NumberColumn("Stars"),
            "BODY_PREVIEW": st.column_config.TextColumn("Review Preview", width="large"),
        },
    )


def render(session: Session) -> None:
    st.title("Overview")
    st.caption("Headline metrics across all enriched reviews.")

    with st.spinner("Loading..."):
        try:
            kpis         = get_kpis(session)
            dist_df      = get_stars_distribution(session)
            sentiment_df = get_sentiment_by_stars(session)
            top_df       = get_top_reviews(session)
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

    _kpi_row(kpis)
    st.divider()
    _charts_row(dist_df, sentiment_df)
    st.divider()
    _top_reviews_table(top_df)
