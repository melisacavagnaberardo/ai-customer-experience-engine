"""
Explorer Page
=============
Filterable, paginated table of enriched reviews.

Filters (rendered at top of page):
    - Star ratings    — multiselect (1-5)
    - Sentiment range — range slider (-1 to +1)
    - ASIN            — text input substring match
    - Status          — radio (All / FAST_ONLY / FULLY_ENRICHED)

Results are fetched from Snowflake with the filters pushed to SQL (max 500 rows).
A CSV export button is available below the table.
"""

import pandas as pd
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
get_reviews_filtered = _q.get_reviews_filtered


def _filters() -> tuple:
    """Render the filter row and return the selected values.

    Returns:
        Tuple of ``(stars, min_sent, max_sent, asin_filter, status_filter)``.
    """
    c1, c2, c3, c4 = st.columns([2, 2, 2, 2])

    stars = c1.multiselect(
        "Stars",
        options=[1, 2, 3, 4, 5],
        default=[1, 2, 3, 4, 5],
    )

    sent_range = c2.slider(
        "Sentiment range",
        min_value=-1.0,
        max_value=1.0,
        value=(-1.0, 1.0),
        step=0.05,
    )

    asin_filter = c3.text_input("ASIN contains", placeholder="e.g. B0C4Y34Q9L")

    status = c4.radio(
        "Enrichment status",
        options=["All", "FAST_ONLY", "FULLY_ENRICHED"],
        horizontal=True,
    )

    return stars, sent_range[0], sent_range[1], asin_filter, status


def _sentiment_color(val: float):
    """Return a CSS color string based on sentiment polarity.

    Args:
        val: Sentiment score between -1 and 1.

    Returns:
        CSS color string.
    """
    if val is None:
        return "color: #718096"
    if val > 0.2:
        return "color: #276749; font-weight: 600"
    if val < -0.2:
        return "color: #c53030; font-weight: 600"
    return "color: #d69e2e"


def render(session: Session) -> None:
    st.title("Explorer")
    st.caption("Filter and browse individual reviews with their AI-generated attributes.")

    stars, min_s, max_s, asin, status = _filters()

    if not stars:
        st.warning("Select at least one star rating.")
        return

    st.divider()

    with st.spinner("Querying Snowflake..."):
        try:
            df = get_reviews_filtered(session, stars, min_s, max_s, asin, status)
        except Exception as e:
            st.error(f"Query failed: {e}")
            return

    # -- Result count + export --
    hdr, btn = st.columns([4, 1])
    hdr.markdown(f"**{len(df):,} reviews** found (max 500 returned)")
    if not df.empty:
        csv = df.to_csv(index=False).encode("utf-8")
        btn.download_button(
            label="Export CSV",
            data=csv,
            file_name="reviews_export.csv",
            mime="text/csv",
        )

    if df.empty:
        st.info("No reviews match the selected filters.")
        return

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=520,
        column_config={
            "ASIN":             st.column_config.TextColumn("ASIN", width="small"),
            "STARS":            st.column_config.NumberColumn("Stars", width="small"),
            "SENTIMENT":        st.column_config.NumberColumn("Sentiment", format="%.3f", width="small"),
            "KEYWORDS":         st.column_config.TextColumn("Keywords", width="medium"),
            "ENRICHMENT_STATUS":st.column_config.TextColumn("Status", width="small"),
            "BODY_PREVIEW":     st.column_config.TextColumn("Review", width="large"),
        },
    )
