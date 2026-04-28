"""
Overview Page — Application Landing
=====================================
Static introduction to the AI Customer Experience Engine.
No Snowflake queries — fully static for fast load and demo readiness.
"""

import streamlit as st
from snowflake.snowpark import Session

_NAVY = "#1a3a5c"
_TEAL = "#2b6cb0"
_GH   = "https://github.com/melisacavagnaberardo/ai-customer-experience-engine-Private"

_MODULES = [
    ("📊", "Explorer",
     "NPS-style sentiment dashboard. Visualise promoter / passive / detractor "
     "distribution derived from product reviews, broken down by star rating."),
    ("🤖", "AI Insights",
     "Cortex enrichment analytics — top extracted keywords and product ranking "
     "across the full catalogue."),
    ("⚙️", "Admin Panel",
     "Pipeline health and processing costs at a glance. Monitor enrichment "
     "throughput and inspect the operational event log in real time."),
]

_STEPS = [
    ("1", "Ingest",  "Raw product reviews land in the SOURCE layer via the schemachange ETL pipeline."),
    ("2", "Enrich",  "Snowflake Cortex runs sentiment scoring and keyword extraction on every review."),
    ("3", "Analyse", "Interactive dashboards surface the enriched data for product and CX teams."),
]

_TECH = ["Snowflake", "Cortex AI", "Streamlit in Snowflake", "Snowpark Python", "Plotly", "schemachange"]

_GH_ICON = (
    '<svg width="13" height="13" viewBox="0 0 16 16" fill="currentColor">'
    '<path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 '
    '0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-'
    '.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07'
    '-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-'
    '.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 '
    '1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73'
    '.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>'
    '</svg>'
)


def render(session: Session) -> None:
    """Render the static Overview landing page.

    Displays: hero banner with GitHub link, application module cards,
    pipeline steps (Ingest → Enrich → Analyse), and technology stack badges.
    No Snowflake queries are executed — this page loads instantly.
    """
    # ── Hero ────────────────────────────────────────────────────────────────
    st.markdown(
        f"""
        <div style="background:linear-gradient(135deg,{_NAVY} 0%,{_TEAL} 100%);
            border-radius:10px;padding:22px 28px;margin-bottom:16px;
            box-shadow:0 4px 16px rgba(26,58,92,0.18);">
            <div style="font-size:10px;font-weight:700;color:#90cdf4;
                letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px;">
                Powered by Snowflake Cortex AI
            </div>
            <h1 style="color:#ffffff;margin:0 0 6px;font-size:20px;line-height:1.2;
                border:none;padding:0;">
                AI Customer Experience Engine
            </h1>
            <p style="color:#bee3f8;font-size:12.5px;margin:0 0 14px;
                max-width:560px;line-height:1.6;">
                An end-to-end AI platform that automatically enriches e-commerce
                product reviews with sentiment scores and keyword insights — all
                running natively inside Snowflake.
            </p>
            <a href="{_GH}" target="_blank"
               style="display:inline-flex;align-items:center;gap:6px;
                background:rgba(255,255,255,0.15);color:#fff;
                border:1px solid rgba(255,255,255,0.30);border-radius:6px;
                padding:5px 12px;font-size:11.5px;font-weight:600;
                text-decoration:none;">
                {_GH_ICON} View on GitHub
            </a>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Modules ──────────────────────────────────────────────────────────────
    modules_html = "".join([
        f"""
        <div style="display:flex;align-items:flex-start;gap:14px;
            background:#f8fbff;border:1px solid #e2e8f0;
            border-left:4px solid {_TEAL};border-radius:7px;
            padding:12px 14px;margin-bottom:8px;">
            <span style="font-size:20px;flex-shrink:0;margin-top:1px;">{icon}</span>
            <div>
                <div style="font-size:13px;font-weight:700;color:{_NAVY};
                    margin-bottom:3px;">{title}</div>
                <div style="font-size:11.5px;color:#4a5568;line-height:1.5;">{desc}</div>
            </div>
        </div>
        """
        for icon, title, desc in _MODULES
    ])
    st.markdown(
        f"""
        <div style="background:#fff;border-radius:10px;padding:16px 18px;
            box-shadow:0 1px 6px rgba(26,58,92,0.07);margin-bottom:14px;">
            <div style="font-size:15px;font-weight:700;color:{_NAVY};margin-bottom:3px;">
                🧩 Application Modules
            </div>
            <div style="font-size:11.5px;color:#718096;margin-bottom:10px;">
                Navigate using the sidebar. Available modules depend on your role.
            </div>
            {modules_html}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Pipeline ─────────────────────────────────────────────────────────────
    steps_html = "".join([
        f"""
        <div style="flex:1;display:flex;align-items:flex-start;gap:8px;padding-right:10px;">
            <div style="width:28px;height:28px;border-radius:50%;background:{_TEAL};
                color:#fff;font-size:13px;font-weight:800;flex-shrink:0;
                display:flex;align-items:center;justify-content:center;">{num}</div>
            <div>
                <div style="font-size:12.5px;font-weight:700;color:{_NAVY};
                    margin-bottom:2px;">{title}</div>
                <div style="font-size:11.5px;color:#718096;line-height:1.5;">{desc}</div>
            </div>
        </div>
        """
        for num, title, desc in _STEPS
    ])
    st.markdown(
        f"""
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:12px 0;">
        <div style="font-size:15px;font-weight:700;color:{_NAVY};margin-bottom:8px;">
            How It Works
        </div>
        <div style="display:flex;gap:0;margin-bottom:12px;">{steps_html}</div>
        """,
        unsafe_allow_html=True,
    )

    # ── Tech stack ───────────────────────────────────────────────────────────
    badges = "".join([
        f"<span style='display:inline-block;background:#e8f4fd;color:{_NAVY};"
        f"border:1px solid #bee3f8;border-radius:20px;padding:2px 10px;"
        f"font-size:10.5px;font-weight:600;margin:0 4px 4px 0;'>{t}</span>"
        for t in _TECH
    ])
    st.markdown(
        f"""
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:12px 0;">
        <div style="font-size:15px;font-weight:700;color:{_NAVY};margin-bottom:8px;">
            Technology Stack
        </div>
        <div style="line-height:2;">{badges}</div>
        """,
        unsafe_allow_html=True,
    )
