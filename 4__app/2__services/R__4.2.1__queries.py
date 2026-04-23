"""
Queries
=======
All Snowflake queries for the Streamlit app, centralised in one module.

Every function accepts a Snowpark Session and returns either a
pandas DataFrame or a plain dict of scalar KPIs.

Table names are fully qualified (DB_GOLD_{env}.AI.*) so the session
does not need a specific database/schema context — USE statements are
not supported in Streamlit in Snowflake.
"""

import pandas as pd
from snowflake.snowpark import Session

# ---------------------------------------------------------------------------
# Env resolution (cached per module load = per SiS session)
# ---------------------------------------------------------------------------
_env_cache: str = ""


def _env(session: Session) -> str:
    global _env_cache
    if not _env_cache:
        role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
        _env_cache = role.split("_")[0]
    return _env_cache


def _ai(session: Session) -> str:
    return f"DB_GOLD_{_env(session)}.AI"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _df(session: Session, sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas()


def _one(session: Session, sql: str) -> tuple:
    rows = session.sql(sql).collect()
    return tuple(rows[0]) if rows else ()


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

def get_kpis(session: Session) -> dict:
    t = _ai(session)
    sql = f"""
        SELECT
            COUNT(*)                                                              AS total,
            ROUND(AVG(SENTIMENT), 3)                                              AS avg_sentiment,
            ROUND(
                SUM(CASE WHEN ENRICHMENT_STATUS = 'FULLY_ENRICHED' THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0), 1
            )                                                                     AS pct_fully_enriched,
            ROUND(
                SUM(CASE WHEN SENTIMENT < 0 THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0), 1
            )                                                                     AS pct_negative
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE SENTIMENT IS NOT NULL
    """
    row = _one(session, sql)
    keys = ["total", "avg_sentiment", "pct_fully_enriched", "pct_negative"]
    return dict(zip(keys, row)) if row else {k: None for k in keys}


def get_stars_distribution(session: Session) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT STARS, COUNT(*) AS COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IS NOT NULL
        GROUP BY STARS ORDER BY STARS
    """)


def get_sentiment_by_stars(session: Session) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT STARS, ROUND(AVG(SENTIMENT), 3) AS AVG_SENTIMENT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IS NOT NULL AND SENTIMENT IS NOT NULL
        GROUP BY STARS ORDER BY STARS
    """)


def get_top_reviews(session: Session, limit: int = 10) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT
            ASIN,
            STARS,
            ROUND(SENTIMENT, 3)     AS SENTIMENT,
            ENRICHMENT_STATUS,
            LEFT(BODY, 250)         AS BODY_PREVIEW
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE SENTIMENT IS NOT NULL AND BODY IS NOT NULL
        ORDER BY ABS(SENTIMENT) DESC
        LIMIT {limit}
    """)


# ---------------------------------------------------------------------------
# Explorer
# ---------------------------------------------------------------------------

def get_reviews_filtered(
    session: Session,
    stars: list,
    min_sentiment: float,
    max_sentiment: float,
    asin_filter: str,
    status_filter: str,
    limit: int = 500,
) -> pd.DataFrame:
    t = _ai(session)
    stars_list    = ", ".join(str(s) for s in stars) if stars else "1,2,3,4,5"
    asin_clause   = f"AND ASIN ILIKE '%{asin_filter}%'" if asin_filter.strip() else ""
    status_clause = f"AND ENRICHMENT_STATUS = '{status_filter}'" if status_filter != "All" else ""

    return _df(session, f"""
        SELECT
            ASIN,
            STARS,
            ROUND(SENTIMENT, 3)  AS SENTIMENT,
            KEYWORDS,
            ENRICHMENT_STATUS,
            LEFT(BODY, 300)      AS BODY_PREVIEW
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IN ({stars_list})
          AND SENTIMENT BETWEEN {min_sentiment} AND {max_sentiment}
          {asin_clause}
          {status_clause}
        ORDER BY ABS(SENTIMENT) DESC
        LIMIT {limit}
    """)


# ---------------------------------------------------------------------------
# AI Insights
# ---------------------------------------------------------------------------

def get_keywords_raw(session: Session) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT KEYWORDS
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE KEYWORDS IS NOT NULL
          AND ENRICHMENT_STATUS = 'FULLY_ENRICHED'
    """)


def get_sentiment_scatter(session: Session) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT
            STARS,
            ROUND(SENTIMENT, 3) AS SENTIMENT,
            ENRICHMENT_STATUS
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE SENTIMENT IS NOT NULL AND STARS IS NOT NULL
    """)


def get_enrichment_coverage(session: Session) -> dict:
    t = _ai(session)
    sql = f"""
        SELECT
            COUNT(*)                                                                  AS total,
            SUM(CASE WHEN ENRICHMENT_STATUS = 'FAST_ONLY'      THEN 1 ELSE 0 END)   AS fast_only,
            SUM(CASE WHEN ENRICHMENT_STATUS = 'FULLY_ENRICHED' THEN 1 ELSE 0 END)   AS fully_enriched,
            SUM(CASE WHEN BODY IS NULL OR TRIM(BODY) = ''      THEN 1 ELSE 0 END)   AS no_body,
            SUM(CASE WHEN KEYWORDS IS NULL                     THEN 1 ELSE 0 END)   AS no_keywords
        FROM {t}.TB_REVIEWS_ENRICHED
    """
    row = _one(session, sql)
    keys = ["total", "fast_only", "fully_enriched", "no_body", "no_keywords"]
    return dict(zip(keys, row)) if row else {k: 0 for k in keys}


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

def get_enrichment_status_breakdown(session: Session) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT STARS, ENRICHMENT_STATUS, COUNT(*) AS COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        GROUP BY STARS, ENRICHMENT_STATUS
        ORDER BY STARS, ENRICHMENT_STATUS
    """)


def get_ingestion_timeline(session: Session) -> pd.DataFrame:
    t = _ai(session)
    return _df(session, f"""
        SELECT
            DATE_TRUNC('hour', CREATED_AT) AS HOUR,
            COUNT(*)                        AS COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE CREATED_AT IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """)


def get_event_logs(session: Session, limit: int = 50) -> pd.DataFrame:
    try:
        env = _env(session)
        sql = f"""
            SELECT
                TIMESTAMP,
                RECORD['severity_text']::STRING  AS SEVERITY,
                VALUE::STRING                    AS MESSAGE
            FROM DB_ADMIN_{env}.LOGS.TB_LOGS
            ORDER BY TIMESTAMP DESC
            LIMIT {limit}
        """
        return session.sql(sql).to_pandas()
    except Exception:
        return pd.DataFrame(columns=["TIMESTAMP", "SEVERITY", "MESSAGE"])


def get_null_stats(session: Session) -> dict:
    t = _ai(session)
    sql = f"""
        SELECT
            SUM(CASE WHEN SENTIMENT IS NULL               THEN 1 ELSE 0 END) AS no_sentiment,
            SUM(CASE WHEN KEYWORDS  IS NULL               THEN 1 ELSE 0 END) AS no_keywords,
            SUM(CASE WHEN BODY IS NULL OR TRIM(BODY) = '' THEN 1 ELSE 0 END) AS no_body
        FROM {t}.TB_REVIEWS_ENRICHED
    """
    row = _one(session, sql)
    keys = ["no_sentiment", "no_keywords", "no_body"]
    return dict(zip(keys, row)) if row else {k: 0 for k in keys}
