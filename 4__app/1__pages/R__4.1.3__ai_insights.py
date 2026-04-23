"""
AI Insights Page
================
Visual analysis of the Cortex AI enrichment results.

Layout:
    Row 1 — Enrichment coverage KPIs (3 cards)
    Row 2 — Top 20 keywords by frequency (horizontal bar)
    Row 3 — Sentiment vs Stars scatter, coloured by enrichment status
"""

from collections import Counter

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
get_enrichment_coverage = _q.get_enrichment_coverage
get_keywords_raw = _q.get_keywords_raw
get_sentiment_scatter = _q.get_sentiment_scatter


NAVY      = "#1a3a5c"
BLUE      = "#2b6cb0"
LIGHT     = "#bee3f8"
STATUS_COLORS = {
    "FULLY_ENRICHED": "#276749",
    "FAST_ONLY":      "#2b6cb0",
}


def _coverage_kpis(cov: dict) -> None:
    """Render the three enrichment coverage metric cards.

    Args:
        cov: Dict returned by ``get_enrichment_coverage()``.
    """
    c1, c2, c3 = st.columns(3)

    total = int(cov.get("total") or 1)
    fast  = int(cov.get("fast_only") or 0)
    full  = int(cov.get("fully_enriched") or 0)
    nobod = int(cov.get("no_body") or 0)
    nokw  = int(cov.get("no_keywords") or 0)

    c1.metric("Sentiment Coverage",  f"{(total - nobod) / max(total, 1) * 100:.0f}%",
              f"{total - nobod:,} reviews with BODY")
    c2.metric("Keyword Coverage",    f"{full:,} reviews",
              f"{full/total*100:.1f}% FULLY_ENRICHED")
    c3.metric("Missing Keywords",    f"{nokw:,}",
              f"{nokw/total*100:.1f}% of total", delta_color="inverse")


def _parse_keywords(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Parse the comma-separated KEYWORDS column into a frequency table.

    Args:
        df: DataFrame with a single ``KEYWORDS`` column.
        top_n: Number of top keywords to return (default 20).

    Returns:
        DataFrame with columns ``KEYWORD`` and ``COUNT``, sorted descending.
    """
    all_kws: list[str] = []
    for raw in df["KEYWORDS"].dropna():
        for kw in raw.split(","):
            cleaned = kw.strip().lower()
            if cleaned:
                all_kws.append(cleaned)

    counts = Counter(all_kws).most_common(top_n)
    return pd.DataFrame(counts, columns=["KEYWORD", "COUNT"])


def _keywords_chart(kw_df: pd.DataFrame) -> None:
    """Render the top keywords horizontal bar chart.

    Args:
        kw_df: DataFrame from ``_parse_keywords()``.
    """
    st.subheader("Top Keywords (FULLY_ENRICHED reviews)")
    if kw_df.empty:
        st.info("No keyword data available yet. Run V3.3.4 to generate keywords.")
        return

    fig = px.bar(
        kw_df.sort_values("COUNT"),
        x="COUNT",
        y="KEYWORD",
        orientation="h",
        color="COUNT",
        color_continuous_scale=[[0, LIGHT], [1, NAVY]],
        labels={"COUNT": "Frequency", "KEYWORD": ""},
        text_auto=True,
    )
    fig.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        margin=dict(t=10, b=10, l=0, r=0),
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="#e2e8f0"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _scatter_chart(df: pd.DataFrame) -> None:
    """Render the sentiment vs. stars scatter plot.

    Args:
        df: DataFrame from ``get_sentiment_scatter()``.
    """
    st.subheader("Sentiment vs Rating")
    if df.empty:
        st.info("No data available.")
        return

    fig = px.scatter(
        df,
        x="STARS",
        y="SENTIMENT",
        color="ENRICHMENT_STATUS",
        color_discrete_map=STATUS_COLORS,
        opacity=0.55,
        labels={"STARS": "Stars", "SENTIMENT": "Sentiment Score",
                "ENRICHMENT_STATUS": "Status"},
    )
    fig.add_hline(y=0, line_dash="dot", line_color="#718096", line_width=1)
    fig.update_layout(
        margin=dict(t=10, b=10, l=0, r=0),
        height=380,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickmode="linear", gridcolor="#e2e8f0"),
        yaxis=dict(gridcolor="#e2e8f0"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)


def render(session: Session) -> None:
    st.title("AI Insights")
    st.caption("Analysis of Cortex enrichment results — sentiment, keywords, and coverage.")

    with st.spinner("Loading enrichment data..."):
        try:
            coverage   = get_enrichment_coverage(session)
            kw_raw_df  = get_keywords_raw(session)
            scatter_df = get_sentiment_scatter(session)
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

    _coverage_kpis(coverage)
    st.divider()

    col_left, col_right = st.columns([1, 1])
    with col_left:
        kw_df = _parse_keywords(kw_raw_df)
        _keywords_chart(kw_df)
    with col_right:
        _scatter_chart(scatter_df)
