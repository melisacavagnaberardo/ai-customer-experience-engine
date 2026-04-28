"""
AI Insights Page
================
Product ranking and keyword analysis from Cortex AI enrichment.

Layout:
    Row 1 — Best / Worst reviewed products (5 each, by avg sentiment)
    
    Row 2 — Top keywords (HTML gradient bars)
"""

from collections import Counter

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

import importlib.util
from pathlib import Path

_SRVCS = Path(__file__).resolve().parents[1] / "2__services"


def _load(path):
    """Load a Python module from *path*, handling dotted filenames via spec_from_file_location."""
    spec = importlib.util.spec_from_file_location(path.stem.split("__")[-1], path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_q = _load(_SRVCS / "R__4.2.1__queries.py")
get_best_products  = _q.get_best_products
get_worst_products = _q.get_worst_products
get_keywords_raw   = _q.get_keywords_raw

_NAVY      = "#1a3a5c"
_TEAL      = "#2b6cb0"
_PROMOTER  = "#276749"
_DETRACTOR = "#c53030"


def _product_panel_html(df: pd.DataFrame, title: str, accent: str) -> str:
    """Return HTML for a product ranking panel (best or worst).

    Renders each product as a row with rank badge, star rating, review count,
    and a horizontal sentiment bar. *accent* drives the colour theme: green for
    best products, red for worst.
    """
    if df.empty:
        return (
            f"<div style='background:#fff;border-radius:10px;padding:14px 16px;"
            f"box-shadow:0 1px 6px rgba(26,58,92,0.06);'>"
            f"<div style='font-size:13px;font-weight:700;color:{accent};margin-bottom:10px;'>{title}</div>"
            f"<div style='font-size:12px;color:#718096;'>Not enough data yet (min. 3 reviews per product).</div>"
            f"</div>"
        )

    rank_shades_best = ["#276749", "#38a169", "#48bb78", "#68d391", "#9ae6b4"]
    rank_shades_worst = ["#c53030", "#e53e3e", "#fc8181", "#feb2b2", "#fed7d7"]
    shades = rank_shades_best if accent == _PROMOTER else rank_shades_worst

    rows = ""
    for i, row in enumerate(df.itertuples()):
        full = int(round(float(row.AVG_STARS)))
        stars = "★" * full + "☆" * (5 - full)
        val = float(row.AVG_SENTIMENT)
        bar_pct = max(4, int(abs(val) * 100))
        sign = "+" if val >= 0 else ""
        shade = shades[i] if i < len(shades) else accent
        rows += (
            f"<div style='display:flex;align-items:center;gap:8px;padding:5px 8px;"
            f"border-radius:7px;margin-bottom:3px;background:#f8fbff;border:1px solid #e2e8f0;'>"
            f"<div style='width:20px;height:20px;border-radius:50%;display:flex;align-items:center;"
            f"justify-content:center;font-size:10px;font-weight:800;color:#fff;"
            f"flex-shrink:0;background:{shade};'>{i+1}</div>"
            f"<div style='flex:1;min-width:0;'>"
            f"<div style='font-size:11px;font-weight:700;color:{_NAVY};white-space:nowrap;"
            f"overflow:hidden;text-overflow:ellipsis;'>{row.ASIN}</div>"
            f"<div style='font-size:10px;color:#d69e2e;'>{stars}&nbsp;"
            f"<span style='color:#718096;'>{float(row.AVG_STARS):.1f} avg</span></div>"
            f"<div style='font-size:10px;color:#718096;'>{int(row.REVIEW_COUNT)} reviews</div>"
            f"</div>"
            f"<div style='width:70px;flex-shrink:0;'>"
            f"<div style='background:#e2e8f0;border-radius:4px;height:6px;overflow:hidden;'>"
            f"<div style='width:{bar_pct}%;height:100%;border-radius:4px;background:{accent};'></div>"
            f"</div>"
            f"<div style='font-size:10px;font-weight:700;color:{accent};text-align:right;margin-top:2px;'>"
            f"{sign}{val:.3f}</div>"
            f"</div>"
            f"</div>"
        )

    return (
        f"<div style='background:#fff;border-radius:10px;padding:10px 12px;"
        f"box-shadow:0 1px 6px rgba(26,58,92,0.06);'>"
        f"<div style='font-size:13px;font-weight:700;color:{accent};margin-bottom:6px;"
        f"display:flex;align-items:center;gap:6px;'>{title}</div>"
        f"{rows}"
        f"</div>"
    )


def _keywords_html(kw_df: pd.DataFrame) -> str:
    """Return HTML for the top-keywords gradient bar chart.

    Each keyword is rendered as a labelled bar whose width is proportional to
    its frequency. Width is clamped to a minimum of 6% so labels remain visible.
    """
    if kw_df.empty:
        return "<div style='color:#718096;font-size:12px;'>No keyword data available yet.</div>"
    max_count = kw_df["COUNT"].max()
    rows = ""
    for _, row in kw_df.iterrows():
        kw  = str(row["KEYWORD"])
        cnt = int(row["COUNT"])
        pct = max(6, int(cnt / max_count * 90))
        rows += (
            f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:4px;'>"
            f"<div style='width:100px;font-size:11px;color:{_NAVY};font-weight:600;"
            f"text-align:right;flex-shrink:0;'>{kw}</div>"
            f"<div style='flex:1;background:#e2e8f0;border-radius:4px;height:18px;overflow:hidden;'>"
            f"<div style='width:{pct}%;height:100%;border-radius:4px;"
            f"background:linear-gradient(90deg,{_TEAL},#4299e1);display:flex;"
            f"align-items:center;padding-left:6px;'>"
            f"<span style='font-size:10px;color:#fff;font-weight:700;'>{cnt}</span>"
            f"</div></div></div>"
        )
    return (
        f"<div style='background:#fff;border-radius:10px;padding:10px 12px;"
        f"box-shadow:0 1px 6px rgba(26,58,92,0.06);'>"
        f"<div style='font-size:13px;font-weight:700;color:{_NAVY};margin-bottom:6px;'>"
        f"🔑 Top Keywords (FULLY_ENRICHED reviews)</div>"
        f"{rows}"
        f"</div>"
    )


def _parse_keywords(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """Parse comma-separated KEYWORDS strings into a ranked frequency DataFrame.

    Splits each value in the KEYWORDS column on commas, lowercases and strips
    whitespace, then returns the top *top_n* keywords with their counts.

    Columns: KEYWORD, COUNT
    """
    all_kws: list = []
    for raw in df["KEYWORDS"].dropna():
        for kw in raw.split(","):
            cleaned = kw.strip().lower()
            if cleaned:
                all_kws.append(cleaned)
    counts = Counter(all_kws).most_common(top_n)
    return pd.DataFrame(counts, columns=["KEYWORD", "COUNT"])


def render(session: Session) -> None:
    """Render the AI Insights page: best/worst product panels and top-keywords chart."""
    st.markdown(
        f"<div style='font-size:20px;font-weight:800;color:{_NAVY};margin-bottom:3px;'>AI Insights</div>"
        f"<div style='font-size:12px;color:#718096;margin-bottom:8px;'>"
        "Product reputation ranking and top extracted keywords from Cortex AI enrichment</div>",
        unsafe_allow_html=True,
    )

    with st.spinner("Loading..."):
        try:
            best_df  = get_best_products(session, limit=5)
            worst_df = get_worst_products(session, limit=5)
            kw_raw   = get_keywords_raw(session)
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

    col_best, col_worst = st.columns(2)
    with col_best:
        st.markdown(
            _product_panel_html(best_df, "🏆 Best Reviewed Products", _PROMOTER),
            unsafe_allow_html=True,
        )
    with col_worst:
        st.markdown(
            _product_panel_html(worst_df, "⚠️ Worst Reviewed Products", _DETRACTOR),
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)
    kw_df = _parse_keywords(kw_raw, top_n=8)
    st.markdown(_keywords_html(kw_df), unsafe_allow_html=True)
