"""
Snowflake Client — Streamlit in Snowflake
==========================================
Session helpers for the SiS runtime.

In SiS, authentication is handled by Snowflake — no credentials needed.
The active session is injected by the runtime via get_active_session().
"""

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session


def get_session() -> Session:
    """Return the active Snowpark session provided by the SiS runtime."""
    return get_active_session()


def get_current_role(session: Session) -> str:
    """Return the active role name for this session."""
    return session.sql("SELECT CURRENT_ROLE()").collect()[0][0]


def get_current_user(session: Session) -> str:
    """Return the authenticated username for this session."""
    return session.sql("SELECT CURRENT_USER()").collect()[0][0]
