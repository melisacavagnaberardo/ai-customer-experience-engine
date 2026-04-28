"""
AI Customer Experience Engine
==============================
Streamlit in Snowflake (SiS) entry point.

Authentication is handled by Snowflake — the active session is injected at
runtime via get_active_session(). No login form needed.

Page routing is role-based:

- \\*_ADMIN_FR  → Overview, Explorer, AI Insights, Admin Panel
- \\*_REPORT_FR → Overview, Explorer, AI Insights
"""

import importlib.util
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_APP   = Path(__file__).resolve().parent
_PAGES = _APP / "1__pages"
_SRVCS = _APP / "2__services"


# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------
def _load(path: Path):
    """Load a Python module from *path*, handling dotted filenames via spec_from_file_location."""
    name = path.stem.split("__")[-1]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# ---------------------------------------------------------------------------
# Load services and pages
# ---------------------------------------------------------------------------
_sf            = _load(_SRVCS / "R__4.2.2__snowflake_client.py")
get_session      = _sf.get_session
get_current_role = _sf.get_current_role
get_current_user = _sf.get_current_user

overview    = _load(_PAGES / "R__4.1.1__overview.py")
explorer    = _load(_PAGES / "R__4.1.2__explorer.py")
ai_insights = _load(_PAGES / "R__4.1.3__ai_insights.py")
admin       = _load(_PAGES / "R__4.1.4__admin.py")

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="AI Customer Experience Engine",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Global CSS
# ---------------------------------------------------------------------------
def _inject_css() -> None:
    """Inject global CSS: app background, sidebar colours, nav button styles, and container padding."""
    st.markdown("""
    <style>
    .stApp {
        background-color: #f5f9ff;
    }
    [data-testid="stSidebar"] {
        background-color: #1a3a5c;
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {
        color: #e2eef9 !important;
    }
    [data-testid="stSidebar"] hr {
        border-color: rgba(255,255,255,0.10);
    }
    /* ── Sidebar nav buttons ─────────────────────────────── */
    [data-testid="stSidebar"] .stButton > button {
        background: transparent;
        border: none;
        border-left: 4px solid transparent;
        color: #c8dff5 !important;
        text-align: left !important;
        justify-content: flex-start !important;
        display: flex !important;
        align-items: center !important;
        width: 100%;
        padding: 10px 14px !important;
        font-size: 14px !important;
        border-radius: 7px;
        line-height: 1.4;
        min-height: 42px;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: rgba(255,255,255,0.10) !important;
        color: #ffffff !important;
        border-left: 4px solid rgba(66,153,225,0.50) !important;
    }
    /* Remove only the excess element-container padding, keep internal gaps */
    [data-testid="stSidebar"] .stButton [data-testid="element-container"],
    [data-testid="stSidebar"] .stButton [data-testid="stButtonGroup"] {
        margin: 0 !important;
        padding: 0 !important;
    }
    /* Each nav button wrapper: compact but breathable */
    [data-testid="stSidebar"] div[data-testid="stButton"] {
        margin-bottom: 2px !important;
        margin-top: 0 !important;
    }
    .main .block-container {
        padding-top: 1.2rem !important;
        padding-bottom: 1.5rem !important;
        padding-left: 1.8rem !important;
        padding-right: 1.8rem !important;
        max-width: 1200px;
    }
    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-left: 4px solid #2b6cb0;
        border-radius: 6px;
        padding: 14px 18px;
        box-shadow: 0 1px 4px rgba(26,58,92,0.06);
    }
    h1 { color: #1a3a5c; border-bottom: 2px solid #2b6cb0;
         padding-bottom: 6px; margin-bottom: 1rem; }
    h2, h3 { color: #1a3a5c; }
    </style>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------
CONSUMER_PAGES = ["Overview", "Explorer", "AI Insights"]
ADMIN_PAGES    = CONSUMER_PAGES + ["Admin Panel"]

PAGE_MODULES = {
    "Overview":    overview,
    "Explorer":    explorer,
    "AI Insights": ai_insights,
    "Admin Panel": admin,
}


def _pages_for_role(role: str) -> list:
    """Return the list of pages accessible to *role*: all pages for ADMIN, consumer pages otherwise."""
    return ADMIN_PAGES if "ADMIN" in role else CONSUMER_PAGES


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
_GH_ICON = (
    '<svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor">'
    '<path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 '
    '0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-'
    '.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07'
    '-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-'
    '.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 '
    '1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73'
    '.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>'
    '</svg>'
)
_GH_URL = "https://github.com/melisacavagnaberardo/ai-customer-experience-engine-Private"


def _render_sidebar() -> str:
    """Render the sidebar navigation and return the currently selected page name."""
    role  = st.session_state.get("role", "")
    user  = st.session_state.get("user", "")
    pages = _pages_for_role(role)

    with st.sidebar:
        st.markdown(
            f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:6px;'>"
            f"<div style='width:44px;height:44px;background:#2b6cb0;border-radius:10px;"
            f"display:flex;align-items:center;justify-content:center;font-size:22px;"
            f"flex-shrink:0;box-shadow:0 2px 8px rgba(0,0,0,0.25);'>🤖</div>"
            f"<div>"
            f"<div style='font-size:17px;font-weight:800;color:#ffffff;line-height:1.2;'>AI CX Engine</div>"
            f"<div style='font-size:11.5px;color:#ffffff;margin-top:2px;opacity:0.85;'>{user} · {role}</div>"
            f"</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        st.divider()

        st.markdown(
            "<p style='font-size:10px;font-weight:700;color:#4a7ab5;"
            "text-transform:uppercase;letter-spacing:1.2px;margin:0 0 6px;'>"
            "Navigation</p>",
            unsafe_allow_html=True,
        )
        current = st.session_state.get("page", pages[0])
        _ICONS = {"Overview": "🏠", "Explorer": "📊", "AI Insights": "🤖", "Admin Panel": "⚙️"}
        for page in pages:
            icon = _ICONS.get(page, "•")
            if page == current:
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:10px;"
                    f"padding:10px 14px;border-radius:7px;margin-bottom:3px;"
                    f"background:rgba(66,153,225,0.18);color:#ffffff;"
                    f"border-left:4px solid #4299e1;font-weight:700;font-size:14px;"
                    f"box-shadow:0 2px 10px rgba(66,153,225,0.30);'>"
                    f"<span style='font-size:15px;'>{icon}</span> {page}</div>",
                    unsafe_allow_html=True,
                )
            else:
                if st.button(f"{icon}  {page}", key=f"nav_{page}", use_container_width=True):
                    st.session_state.page = page
                    st.experimental_rerun()

        st.markdown(
            "<div style='margin-top:16px;border-top:1px solid rgba(255,255,255,0.10);"
            "padding-top:12px;'></div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<a href='{_GH_URL}' target='_blank' "
            f"style='display:inline-flex;align-items:center;gap:6px;"
            f"background:rgba(255,255,255,0.15);color:#fff;"
            f"border:1px solid rgba(255,255,255,0.30);border-radius:6px;"
            f"padding:5px 12px;font-size:11.5px;font-weight:600;text-decoration:none;"
            f"margin-top:4px;'>"
            f"{_GH_ICON} View on GitHub</a>",
            unsafe_allow_html=True,
        )

    return st.session_state.get("page", pages[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """App entry point: initialise session state, render sidebar, and dispatch to the active page module."""
    _inject_css()

    if "session" not in st.session_state:
        session = get_session()
        role    = get_current_role(session)

        # st.experimental_user is the reliable SiS source for the human user.
        # Fall back to CURRENT_USER() if not available.
        try:
            exp = st.experimental_user
            raw = (
                getattr(exp, "email",        None)
                or getattr(exp, "display_name", None)
                or getattr(exp, "user_name",    None)
                or getattr(exp, "login_name",   None)
            )
            if raw and "@" in str(raw):
                user = str(raw).split("@")[0].lower()
            elif raw:
                user = str(raw).lower()
            else:
                user = None
        except Exception:
            user = None
        if not user:
            raw = get_current_user(session)
            user = str(raw).lower() if raw else None

        st.session_state.session = session
        st.session_state.role    = role
        st.session_state.user    = user or "—"
        st.session_state.page    = "Overview"

    page   = _render_sidebar()
    module = PAGE_MODULES.get(page)

    if module is None:
        st.error(f"Page not found: {page}")
        return

    if page == "Admin Panel" and "ADMIN" not in st.session_state.get("role", ""):
        st.error("Access denied. Admin role required.")
        return

    module.render(st.session_state.session)


if __name__ == "__main__":
    main()
