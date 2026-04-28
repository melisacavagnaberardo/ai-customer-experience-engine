"""
Explorer Page — NPS Sentiment Dashboard
========================================
Net Promoter Score analytics derived from product review star ratings.

Mapping:
    1–2 stars  → Detractors
    3–4 stars  → Passives
    5 stars    → Promoters

NPS = (% Promoters − % Detractors)  ·  range −100 to +100
"""


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
get_nps_summary               = _q.get_nps_summary
get_stars_sentiment_breakdown = _q.get_stars_sentiment_breakdown

_NAVY      = "#1a3a5c"
_TEAL      = "#2b6cb0"
_DETRACTOR = "#c53030"
_PASSIVE   = "#d69e2e"
_PROMOTER  = "#276749"


def _pct(n: int, total: int) -> float:
    """Return *n* as a percentage of *total*, rounded to 1 decimal; 0.0 when total is zero."""
    return round(n / total * 100, 1) if total else 0.0


def _nps(total: int, promoters: int, detractors: int) -> int:
    """Return NPS score = round(% promoters − % detractors); 0 when total is zero."""
    return round((_pct(promoters, total) - _pct(detractors, total))) if total else 0


def _nps_color(score: int) -> str:
    """Return a hex colour for an NPS score: green ≥ 30, yellow ≥ 0, red otherwise."""
    if score >= 30: return _PROMOTER
    if score >= 0:  return _PASSIVE
    return _DETRACTOR


def _kpi_card(label: str, value: str, sub: str, border: str, val_color: str) -> str:
    """Return HTML for a styled KPI metric card with label, value, subtitle, and accent colour."""
    return (
        f"<div style='background:#fff;border:1px solid #e2e8f0;"
        f"border-left:4px solid {border};border-radius:8px;"
        f"padding:14px 16px;box-shadow:0 1px 4px rgba(26,58,92,0.05);height:100%;'>"
        f"<div style='font-size:11px;color:#718096;margin-bottom:4px;"
        f"text-transform:uppercase;letter-spacing:0.5px;'>{label}</div>"
        f"<div style='font-size:26px;font-weight:800;color:{val_color};'>{value}</div>"
        f"<div style='font-size:11px;color:#a0aec0;margin-top:2px;'>{sub}</div>"
        f"</div>"
    )


def _bars_html(df, threshold: float = 0.1) -> str:
    """Return HTML for the stacked sentiment-by-star-rating bar chart.

    Each row shows the % positive (green), neutral (yellow), and negative (red)
    sentiment for that star level. The subtitle reflects the active threshold.
    """
    if df.empty:
        return "<div style='color:#718096;font-size:12px;'>No data.</div>"

    rows = ""
    for _, row in df.sort_values("STARS", ascending=False).iterrows():
        stars    = int(row["STARS"])
        star_str = "★" * stars + "☆" * (5 - stars)
        pos = max(0.0, float(row.get("PCT_POSITIVE", 0) or 0))
        neu = max(0.0, float(row.get("PCT_NEUTRAL",  0) or 0))
        neg = max(0.0, float(row.get("PCT_NEGATIVE", 0) or 0))

        pos_txt = f"{pos:.0f}%" if pos >= 8 else ""
        neu_txt = f"{neu:.0f}%" if neu >= 8 else ""
        neg_txt = f"{neg:.0f}%" if neg >= 8 else ""

        rows += (
            f"<div style='margin-bottom:10px;'>"
            f"<div style='font-size:12px;font-weight:600;color:{_NAVY};margin-bottom:4px;'>"
            f"{stars} {star_str}</div>"
            f"<div style='display:flex;border-radius:5px;overflow:hidden;height:26px;'>"
            f"<div style='width:{pos}%;background:{_PROMOTER};display:flex;align-items:center;"
            f"justify-content:center;font-size:11px;font-weight:700;color:#fff;'>{pos_txt}</div>"
            f"<div style='width:{neu}%;background:{_PASSIVE};display:flex;align-items:center;"
            f"justify-content:center;font-size:11px;font-weight:700;color:#fff;'>{neu_txt}</div>"
            f"<div style='width:{neg}%;background:{_DETRACTOR};display:flex;align-items:center;"
            f"justify-content:center;font-size:11px;font-weight:700;color:#fff;'>{neg_txt}</div>"
            f"</div></div>"
        )

    legend = (
        f"<div style='display:flex;gap:14px;margin-top:10px;'>"
        f"<div style='display:flex;align-items:center;gap:5px;font-size:11px;color:#4a5568;'>"
        f"<div style='width:10px;height:10px;border-radius:50%;background:{_PROMOTER};flex-shrink:0;'></div>"
        f" Positive sentiment</div>"
        f"<div style='display:flex;align-items:center;gap:5px;font-size:11px;color:#4a5568;'>"
        f"<div style='width:10px;height:10px;border-radius:50%;background:{_PASSIVE};flex-shrink:0;'></div>"
        f" Neutral</div>"
        f"<div style='display:flex;align-items:center;gap:5px;font-size:11px;color:#4a5568;'>"
        f"<div style='width:10px;height:10px;border-radius:50%;background:{_DETRACTOR};flex-shrink:0;'></div>"
        f" Negative</div>"
        f"</div>"
    )

    return (
        f"<div style='background:#fff;border-radius:10px;padding:16px 18px;"
        f"box-shadow:0 1px 6px rgba(26,58,92,0.06);height:100%;box-sizing:border-box;'>"
        f"<div style='font-size:14px;font-weight:700;color:{_NAVY};margin-bottom:3px;'>"
        f"Sentiment by Star Rating</div>"
        f"<div style='font-size:11px;color:#718096;margin-bottom:12px;'>"
        f"Positive &gt; {threshold:.2f} · "
        f"Neutral ±{threshold:.2f} · "
        f"Negative &lt; -{threshold:.2f}</div>"
        f"{rows}{legend}"
        f"</div>"
    )


def _nps_panel_html(promoters: int, passives: int, detractors: int, nps: int) -> str:
    """Return HTML for the NPS breakdown panel.

    Renders an emoji satisfaction scale, the NPS formula, a CSS conic-gradient
    donut chart, and a 4-column grid of detractor/passive/promoter/total cards.
    """
    total_d  = max(promoters + passives + detractors, 1)
    prom_end = promoters / total_d * 100
    pass_end = prom_end + passives / total_d * 100

    nps_col = _nps_color(nps)
    sign    = "+" if nps >= 0 else ""

    emoji_items = [
        ("😁", "5★"), ("😊", "5★"), ("🙂", "5★"),
        ("😐", "4★"), ("😑", "4★"),
        ("😕", "3★"), ("😟", "3★"),
        ("😠", "2★"), ("😡", "2★"), ("🤬", "1★"),
    ]
    emoji_html = "".join(
        f"<div style='text-align:center;'>"
        f"<div style='font-size:18px;line-height:1;'>{face}</div>"
        f"<div style='font-size:9px;color:#718096;margin-top:1px;'>{num}</div>"
        f"</div>"
        for face, num in emoji_items
    )

    total_s = promoters + passives + detractors
    prom_d  = _pct(promoters,  total_s)
    pass_d  = _pct(passives,   total_s)
    det_d   = _pct(detractors, total_s)

    return (
        f"<div style='background:#fff;border-radius:10px;padding:16px 18px;"
        f"box-shadow:0 1px 6px rgba(26,58,92,0.06);height:100%;box-sizing:border-box;'>"

        f"<div style='font-size:14px;font-weight:700;color:{_NAVY};margin-bottom:8px;'>"
        f"NPS Breakdown</div>"

        # Emoji scale
        f"<div style='display:flex;justify-content:center;gap:3px;margin-bottom:8px;'>"
        f"{emoji_html}</div>"

        # NPS formula
        f"<div style='font-size:14px;font-weight:700;color:{_NAVY};"
        f"margin-bottom:10px;text-align:center;'>"
        f"NPS = <span style='color:{_PROMOTER};'>%Promoters</span>"
        f" &minus; <span style='color:{_DETRACTOR};'>%Detractors</span></div>"

        # CSS Donut (conic-gradient)
        f"<div style='display:flex;justify-content:center;margin:10px 0;'>"
        f"<div style='width:140px;height:140px;border-radius:50%;"
        f"background:conic-gradient("
        f"{_PROMOTER} 0% {prom_end:.1f}%,"
        f"{_PASSIVE} {prom_end:.1f}% {pass_end:.1f}%,"
        f"{_DETRACTOR} {pass_end:.1f}% 100%);"
        f"display:flex;align-items:center;justify-content:center;'>"
        f"<div style='width:92px;height:92px;border-radius:50%;background:#fff;"
        f"display:flex;flex-direction:column;align-items:center;justify-content:center;'>"
        f"<div style='font-size:30px;font-weight:800;color:{nps_col};line-height:1;'>{sign}{nps}</div>"
        f"<div style='font-size:10px;color:#718096;'>NPS Score</div>"
        f"</div></div></div>"

        # Breakdown cards (4-column grid)
        f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:12px;'>"

        f"<div style='background:{_DETRACTOR}18;border:1px solid {_DETRACTOR}40;"
        f"border-radius:7px;padding:10px 6px;text-align:center;'>"
        f"<div style='font-size:9px;font-weight:700;color:{_DETRACTOR};"
        f"text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;'>Detractors</div>"
        f"<div style='font-size:20px;font-weight:800;color:{_DETRACTOR};line-height:1;'>{det_d:.1f}%</div>"
        f"<div style='font-size:9px;color:#718096;margin-top:2px;'>{detractors:,} reviews</div></div>"

        f"<div style='background:{_PASSIVE}18;border:1px solid {_PASSIVE}40;"
        f"border-radius:7px;padding:10px 6px;text-align:center;'>"
        f"<div style='font-size:9px;font-weight:700;color:{_PASSIVE};"
        f"text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;'>Passives</div>"
        f"<div style='font-size:20px;font-weight:800;color:{_PASSIVE};line-height:1;'>{pass_d:.1f}%</div>"
        f"<div style='font-size:9px;color:#718096;margin-top:2px;'>{passives:,} reviews</div></div>"

        f"<div style='background:{_PROMOTER}18;border:1px solid {_PROMOTER}40;"
        f"border-radius:7px;padding:10px 6px;text-align:center;'>"
        f"<div style='font-size:9px;font-weight:700;color:{_PROMOTER};"
        f"text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;'>Promoters</div>"
        f"<div style='font-size:20px;font-weight:800;color:{_PROMOTER};line-height:1;'>{prom_d:.1f}%</div>"
        f"<div style='font-size:9px;color:#718096;margin-top:2px;'>{promoters:,} reviews</div></div>"

        f"<div style='background:#f5f9ff;border:1px solid #bee3f8;"
        f"border-radius:7px;padding:10px 6px;text-align:center;'>"
        f"<div style='font-size:9px;font-weight:700;color:{_NAVY};"
        f"text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;'>Total</div>"
        f"<div style='font-size:20px;font-weight:800;color:{_NAVY};line-height:1;'>{total_s:,}</div>"
        f"<div style='font-size:9px;color:#718096;margin-top:2px;'>respondents</div></div>"

        f"</div></div>"
    )


def render(session: Session) -> None:
    """Render the Explorer page: KPI row, NPS badge, threshold slider, and two-panel layout."""
    st.markdown(
        f"<div style='font-size:20px;font-weight:800;color:{_NAVY};margin-bottom:3px;'>Explorer</div>"
        f"<div style='font-size:12px;color:#718096;margin-bottom:14px;'>"
        "Net Promoter Score derived from product reviews · "
        "Promoters = 5★ · Passives = 3–4★ · Detractors = 1–2★</div>",
        unsafe_allow_html=True,
    )

    # NPS data is threshold-independent — fetch once
    with st.spinner("Loading..."):
        try:
            nps_data = get_nps_summary(session)
        except Exception as e:
            st.error(f"Failed to load data: {e}")
            return

    total      = int(nps_data.get("total")      or 0)
    detractors = int(nps_data.get("detractors") or 0)
    passives   = int(nps_data.get("passives")   or 0)
    promoters  = int(nps_data.get("promoters")  or 0)
    nps        = _nps(total, promoters, detractors)
    nps_col    = _nps_color(nps)
    nps_label  = "Excellent" if nps >= 50 else "Good" if nps >= 30 else "Needs improvement" if nps >= 0 else "Critical"

    # ── KPI row ──────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(_kpi_card("Total Reviews",  f"{total:,}",                       "enriched records",                _TEAL,      _NAVY),      unsafe_allow_html=True)
    k2.markdown(_kpi_card("NPS Score",      f"{nps:+d}",                        nps_label,                         nps_col,    nps_col),    unsafe_allow_html=True)
    k3.markdown(_kpi_card("Promoters",      f"{_pct(promoters, total):.1f}%",   f"{promoters:,} reviews (5★)",     _PROMOTER,  _PROMOTER),  unsafe_allow_html=True)
    k4.markdown(_kpi_card("Detractors",     f"{_pct(detractors, total):.1f}%",  f"{detractors:,} reviews (1–2★)",  _DETRACTOR, _DETRACTOR), unsafe_allow_html=True)

    # NPS badge
    st.markdown(
        f"<div style='margin:6px 0 6px;'>"
        f"<span style='background:{nps_col}18;color:{nps_col};"
        f"border:1px solid {nps_col}50;border-radius:20px;"
        f"padding:4px 16px;font-size:12px;font-weight:600;'>"
        f"NPS {nps:+d} — {nps_label}"
        f"</span></div>",
        unsafe_allow_html=True,
    )

    # ── Confidence threshold slider ───────────────────────────────────────────
    # Persist the threshold across page navigations so the user's last value
    # is restored when returning to Explorer from another page.
    if "sentiment_threshold" not in st.session_state:
        st.session_state.sentiment_threshold = 0.1

    ctrl_col, _ = st.columns([2, 5])
    with ctrl_col:
        threshold = st.slider(
            "Neutral band  ±threshold",
            min_value=0.0,
            max_value=0.5,
            value=st.session_state.sentiment_threshold,
            step=0.05,
            key="sentiment_threshold",
            help=(
                "Cortex SENTIMENT scores within ±threshold are classified as Neutral. "
                "Increase to widen the neutral band; decrease for stricter polarity."
            ),
        )

    # Bars are threshold-dependent — re-query on every slider change
    try:
        bars_df = get_stars_sentiment_breakdown(session, threshold=threshold)
    except Exception as e:
        st.error(f"Failed to load sentiment breakdown: {e}")
        bars_df = pd.DataFrame()

    # ── Main section — single flex container so both panels match height ──────
    st.markdown(
        f"<div style='display:flex;gap:16px;align-items:stretch;'>"
        f"<div style='flex:5;min-width:0;'>{_bars_html(bars_df, threshold)}</div>"
        f"<div style='flex:6;min-width:0;'>{_nps_panel_html(promoters, passives, detractors, nps)}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )
