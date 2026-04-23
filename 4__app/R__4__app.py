"""
AI Customer Experience Engine
==============================
Streamlit in Snowflake (SiS) entry point.

Authentication is handled by Snowflake — the active session is injected at
runtime via get_active_session(). No login form needed.

Page routing is role-based:
- *_ADMIN_FR  → Overview, Explorer, AI Insights, Admin Panel
- *_REPORT_FR → Overview, Explorer, AI Insights
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
    st.markdown("""
    <style>
    [data-testid="stSidebar"] {
        background-color: #1a3a5c;
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label {
        color: #c8dff5 !important;
    }
    [data-testid="stSidebar"] hr {
        border-color: #2b6cb0;
    }
    [data-testid="stSidebar"] .stButton > button {
        background: transparent;
        border: none;
        color: #c8dff5 !important;
        text-align: left;
        width: 100%;
        padding: 6px 12px;
        font-size: 15px;
        border-radius: 4px;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: #0d2240;
        color: #ffffff !important;
    }
    .main .block-container {
        padding-top: 2rem;
        max-width: 1200px;
    }
    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-left: 4px solid #2b6cb0;
        border-radius: 6px;
        padding: 16px 20px;
        box-shadow: 0 1px 4px rgba(26,58,92,0.06);
    }
    h1 { color: #1a3a5c; border-bottom: 2px solid #2b6cb0;
         padding-bottom: 6px; margin-bottom: 1.2rem; }
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
    return ADMIN_PAGES if "ADMIN" in role else CONSUMER_PAGES


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def _render_sidebar() -> str:
    role  = st.session_state.get("role", "")
    user  = st.session_state.get("user", "")
    pages = _pages_for_role(role)

    with st.sidebar:
        st.markdown(
            f"<p style='font-size:18px; font-weight:700; color:#ffffff; "
            f"margin-bottom:4px;'>AI CX Engine</p>"
            f"<p style='font-size:12px; color:#8eb8e0;'>{user} · {role}</p>",
            unsafe_allow_html=True,
        )
        st.divider()

        for page in pages:
            if st.button(page, key=f"nav_{page}", use_container_width=True):
                st.session_state.page = page

    return st.session_state.get("page", pages[0])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    _inject_css()

    if "session" not in st.session_state:
        session = get_session()
        role    = get_current_role(session)
        user    = get_current_user(session)
        env     = role.split("_")[0]

        st.session_state.session = session
        st.session_state.role    = role
        st.session_state.user    = user
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
