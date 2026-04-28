"""
Admin Panel Page
================
Two sections:

    1. Costs  — Snowflake credit consumption for the last 30 days.
                Sources: ACCOUNT_USAGE warehouse metering, AI/Cortex metering,
                and database storage. Falls back gracefully (shows —) when
                the current role lacks ACCOUNT_USAGE access.

    2. Logs   — Pipeline stage durations derived from TB_LOGS keywords,
                followed by the last 50 event log entries.
"""

from typing import Optional
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
get_snowflake_costs          = _q.get_snowflake_costs
get_pipeline_stage_durations = _q.get_pipeline_stage_durations
get_event_logs               = _q.get_event_logs

_NAVY   = "#1a3a5c"
_TEAL   = "#2b6cb0"
_GREEN  = "#276749"
_YELLOW = "#d69e2e"
_RED    = "#c53030"


def _cost_card(col, icon: str, label: str, value: Optional[float], sub: str, color: str) -> None:
    """Render a styled cost metric card into a Streamlit column.

    When *value* is ``None`` (ACCOUNT_USAGE access denied), shows "—" with a
    grey message. Otherwise renders the formatted value, subtitle, and a
    proportional fill bar.
    """
    if value is None:
        display   = "—"
        sub_text  = "Requires ACCOUNT_USAGE privilege"
        val_color = "#a0aec0"
        bar_pct   = 0
    else:
        display   = f"{value:,.2f}".rstrip("0").rstrip(".")
        sub_text  = sub
        val_color = _NAVY
        bar_pct   = min(int(value / max(value, 1) * 60) + 10, 95)

    col.markdown(
        f"""
        <div style="background:#ffffff;border:1px solid #e2e8f0;
            border-left:5px solid {color};border-radius:10px;
            padding:12px 16px 10px;box-shadow:0 2px 8px rgba(26,58,92,0.07);">
            <div style="font-size:10px;font-weight:700;color:{color};
                text-transform:uppercase;letter-spacing:0.8px;margin-bottom:8px;">
                {icon}&nbsp; {label}
            </div>
            <div style="font-size:28px;font-weight:800;color:{val_color};line-height:1;">
                {display}
            </div>
            <div style="font-size:11px;color:#718096;margin-top:4px;">{sub_text}</div>
            <div style="margin-top:10px;background:#f5f9ff;border-radius:4px;height:4px;">
                <div style="width:{bar_pct}%;background:{color};height:100%;
                    border-radius:4px;min-width:4px;"></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _pipeline_html(df) -> str:
    """Return HTML for the pipeline stage duration bar chart.

    Each stage row shows a labelled horizontal bar proportional to its duration.
    AI Enrichment bars are rendered in green; all other stages use teal.
    Shows a placeholder message when *df* is empty.
    """
    if df.empty:
        return (
            f"<div style='background:#fff;border-radius:10px;padding:14px 18px;"
            f"box-shadow:0 1px 6px rgba(26,58,92,0.06);margin-bottom:12px;'>"
            f"<div style='font-size:12px;font-weight:700;color:{_NAVY};margin-bottom:8px;'>"
            f"⏱ Pipeline Stage Duration</div>"
            f"<div style='font-size:11px;color:#a0aec0;font-style:italic;'>"
            f"No timing data yet — run the full deploy pipeline (migrations → seed → schemachange → app deploy) "
            f"to populate this chart.</div>"
            f"</div>"
        )
    max_sec = max(int(df["DURATION_SEC"].max()), 1)
    rows = ""
    for _, row in df.iterrows():
        stage   = str(row["STAGE"])
        dur_sec = int(row["DURATION_SEC"])
        dur_min = round(dur_sec / 60, 1)
        label   = f"{dur_min} min" if dur_min >= 1 else f"{dur_sec}s"
        pct     = max(3, int(dur_sec / max_sec * 80))
        color   = _GREEN if stage == "AI Enrichment" else _TEAL
        rows += (
            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:7px;'>"
            f"<div style='width:130px;font-size:11px;font-weight:600;color:#4a5568;"
            f"flex-shrink:0;text-align:right;'>{stage}</div>"
            f"<div style='flex:1;background:#f0f4f8;border-radius:4px;height:18px;overflow:hidden;'>"
            f"<div style='width:{pct}%;height:100%;border-radius:4px;background:{color};"
            f"min-width:28px;display:flex;align-items:center;padding-left:8px;'>"
            f"<span style='font-size:10px;font-weight:700;color:#fff;'>{label}</span>"
            f"</div></div>"
            f"<div style='width:40px;font-size:10px;color:#718096;text-align:right;"
            f"flex-shrink:0;'>{label}</div>"
            f"</div>"
        )
    return (
        f"<div style='background:#fff;border-radius:10px;padding:14px 18px;"
        f"box-shadow:0 1px 6px rgba(26,58,92,0.06);margin-bottom:12px;'>"
        f"<div style='font-size:12px;font-weight:700;color:{_NAVY};margin-bottom:10px;'>"
        f"⏱ Pipeline Stage Duration</div>"
        f"{rows}"
        f"</div>"
    )


def _severity_badge(sev: str) -> str:
    """Return a coloured HTML inline badge for a log severity level (ERROR/WARN/INFO/DEBUG)."""
    s = (sev or "").upper()
    cfg = {
        "ERROR":   ("#c53030", "#fff5f5"),
        "WARN":    ("#d69e2e", "#fffaf0"),
        "WARNING": ("#d69e2e", "#fffaf0"),
        "INFO":    ("#276749", "#f0fff4"),
        "DEBUG":   ("#718096", "#f7fafc"),
    }
    fg, bg = cfg.get(s, ("#718096", "#f7fafc"))
    return (
        f"<span style='background:{bg};color:{fg};border:1px solid {fg};"
        f"border-radius:4px;padding:1px 7px;font-size:11px;font-weight:700;"
        f"white-space:nowrap;'>{sev}</span>"
    )


def _log_table_html(df, env: str = "") -> str:
    """Return HTML for the scrollable pipeline log table.

    Renders TIMESTAMP, SEVERITY badge, and MESSAGE columns with alternating row
    backgrounds. Limited to ``max-height: 190px`` with overflow scroll.
    """
    rows = ""
    for i, (_, row) in enumerate(df.iterrows()):
        ts  = str(row.get("TIMESTAMP", ""))
        sev = str(row.get("SEVERITY",  ""))
        msg = str(row.get("MESSAGE",   ""))
        bg  = "#fafbfc" if i % 2 == 1 else "#ffffff"
        rows += (
            f"<tr style='border-bottom:1px solid #f0f4f8;background:{bg};'>"
            f"<td style='padding:6px 10px;color:#718096;white-space:nowrap;font-size:11px;'>{ts}</td>"
            f"<td style='padding:6px 10px;'>{_severity_badge(sev)}</td>"
            f"<td style='padding:6px 10px;color:#4a5568;font-size:11px;'>{msg}</td>"
            f"</tr>"
        )
    table_ref = f"DB_ADMIN_{env}.LOGS.TB_PIPELINE_LOGS · " if env else ""
    return (
        f"<div style='background:#fff;border-radius:10px;padding:14px 18px;"
        f"box-shadow:0 1px 6px rgba(26,58,92,0.06);'>"
        f"<div style='font-size:12px;font-weight:700;color:{_NAVY};margin-bottom:10px;'>"
        f"Last 50 entries · {table_ref}ordered by timestamp ↓</div>"
        f"<div style='overflow-x:auto;max-height:190px;overflow-y:auto;'>"
        f"<table style='width:100%;border-collapse:collapse;font-size:11px;'>"
        f"<thead><tr style='background:#f5f9ff;border-bottom:2px solid #e2e8f0;'>"
        f"<th style='padding:7px 10px;text-align:left;color:#4a5568;font-weight:700;'>TIMESTAMP</th>"
        f"<th style='padding:7px 10px;text-align:left;color:#4a5568;font-weight:700;'>SEVERITY</th>"
        f"<th style='padding:7px 10px;text-align:left;color:#4a5568;font-weight:700;'>MESSAGE</th>"
        f"</tr></thead><tbody>{rows}</tbody>"
        f"</table></div></div>"
    )


# ── Page ───────────────────────────────────────────────────────────────────

def render(session: Session) -> None:
    """Render the Admin Panel page: costs section (3 credit cards) and logs section."""
    st.markdown(
        f"<div style='font-size:20px;font-weight:800;color:{_NAVY};margin-bottom:3px;'>Admin Panel</div>"
        f"<div style='font-size:12px;color:#718096;margin-bottom:8px;'>"
        "Snowflake cost monitoring and pipeline operational logs</div>",
        unsafe_allow_html=True,
    )

    with st.spinner("Loading..."):
        try:
            costs    = get_snowflake_costs(session)
            stage_df = get_pipeline_stage_durations(session)
            logs_df  = get_event_logs(session, limit=50)
        except Exception as e:
            st.error(f"Failed to load admin data: {e}")
            return

    env = costs.get("env", "")

    # ── 💰 COSTS ─────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:8px;'>"
        f"<div style='font-size:15px;font-weight:700;color:{_NAVY};'>💰 Costs</div>"
        f"<div style='font-size:11px;color:#718096;background:#e8f4fd;"
        f"border:1px solid #bee3f8;border-radius:12px;padding:2px 10px;'>Last 30 days</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    _cost_card(c1, "🏭", "Warehouse Credits",
               costs.get("wh_credits"),
               f"credits · WH_ADMIN_{env}",
               _TEAL)
    _cost_card(c2, "🤖", "AI Function Credits",
               costs.get("ai_credits"),
               "credits · Cortex AI (sentiment + keywords)",
               _GREEN)
    _cost_card(c3, "🗄️", "Storage",
               costs.get("storage_gb"),
               f"DB_GOLD_{env} + DB_ADMIN_{env} + DB_SOURCE_{env}",
               _YELLOW)

    st.markdown("<div style='margin:10px 0 6px;'></div>", unsafe_allow_html=True)

    # ── 📋 LOGS ───────────────────────────────────────────────────────────────
    st.markdown(
        f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:8px;'>"
        f"<div style='font-size:15px;font-weight:700;color:{_NAVY};'>📋 Logs</div>"
        f"</div>",
        unsafe_allow_html=True,
    )

    st.markdown(_pipeline_html(stage_df), unsafe_allow_html=True)

    if logs_df.empty:
        st.info("No log entries found. TB_LOGS may be empty or not accessible with this role.")
    else:
        st.markdown(_log_table_html(logs_df, env), unsafe_allow_html=True)
