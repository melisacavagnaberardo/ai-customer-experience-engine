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
    """Resolve the environment prefix (DES/PRE/PRO) from CURRENT_ROLE(), cached per module load."""
    global _env_cache
    if not _env_cache:
        role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
        _env_cache = role.split("_")[0]
    return _env_cache


def _ai(session: Session) -> str:
    """Return the fully-qualified AI schema prefix: ``DB_GOLD_{env}.AI``."""
    return f"DB_GOLD_{_env(session)}.AI"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _df(session: Session, sql: str) -> pd.DataFrame:
    """Execute *sql* and return results as a pandas DataFrame."""
    return session.sql(sql).to_pandas()


def _one(session: Session, sql: str) -> tuple:
    """Execute *sql* and return the first row as a tuple; empty tuple if no rows."""
    rows = session.sql(sql).collect()
    return tuple(rows[0]) if rows else ()


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------

def get_kpis(session: Session) -> dict:
    """Return top-level KPIs for the Overview page.

    Keys:
        total (int): Total enriched review count.
        avg_sentiment (float): Average Cortex sentiment score across all reviews.
        pct_fully_enriched (float): Percentage of reviews with both sentiment and keywords.
        pct_negative (float): Percentage of reviews with negative sentiment (< 0).
    """
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
    """Return review count per star rating (1–5) from TB_REVIEWS_ENRICHED.

    Columns: STARS, COUNT
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT STARS, COUNT(*) AS COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IS NOT NULL
        GROUP BY STARS ORDER BY STARS
    """)


def get_sentiment_by_stars(session: Session) -> pd.DataFrame:
    """Return average Cortex sentiment score grouped by star rating.

    Columns: STARS, AVG_SENTIMENT
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT STARS, ROUND(AVG(SENTIMENT), 3) AS AVG_SENTIMENT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IS NOT NULL AND SENTIMENT IS NOT NULL
        GROUP BY STARS ORDER BY STARS
    """)


def get_top_reviews(session: Session, limit: int = 10) -> pd.DataFrame:
    """Return the *limit* reviews with the highest absolute sentiment score.

    Ordered by ``ABS(SENTIMENT) DESC`` so the most polarised reviews (very
    positive or very negative) appear first. Body is truncated to 250 chars.

    Columns: ASIN, STARS, SENTIMENT, ENRICHMENT_STATUS, BODY_PREVIEW
    """
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
    """Return reviews matching the Explorer filter panel criteria.

    Args:
        session: Active Snowpark session.
        stars: List of star ratings to include (1–5).
        min_sentiment: Lower bound for SENTIMENT (inclusive).
        max_sentiment: Upper bound for SENTIMENT (inclusive).
        asin_filter: Partial ASIN string; ignored when blank.
        status_filter: Enrichment status value or ``"All"`` to skip filtering.
        limit: Maximum rows returned (default 500).

    Columns: ASIN, STARS, SENTIMENT, KEYWORDS, ENRICHMENT_STATUS, BODY_PREVIEW
    """
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
    """Return the raw KEYWORDS column for FULLY_ENRICHED reviews.

    Used by the AI Insights page, which parses comma-separated keyword strings
    and aggregates them into a frequency table.

    Columns: KEYWORDS
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT KEYWORDS
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE KEYWORDS IS NOT NULL
          AND ENRICHMENT_STATUS = 'FULLY_ENRICHED'
    """)


def get_sentiment_scatter(session: Session) -> pd.DataFrame:
    """Return star rating, sentiment, and enrichment status for all enriched reviews.

    Intended for scatter plot visualisations that compare star rating vs.
    Cortex sentiment score.

    Columns: STARS, SENTIMENT, ENRICHMENT_STATUS
    """
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
    """Return enrichment coverage counts for data quality monitoring.

    Keys:
        total (int): Total row count in TB_REVIEWS_ENRICHED.
        fast_only (int): Rows with ENRICHMENT_STATUS = 'FAST_ONLY'.
        fully_enriched (int): Rows with ENRICHMENT_STATUS = 'FULLY_ENRICHED'.
        no_body (int): Rows with a NULL or empty BODY.
        no_keywords (int): Rows with a NULL KEYWORDS value.
    """
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
    """Return review count grouped by (STARS, ENRICHMENT_STATUS).

    Used by the Admin Panel for heatmap-style enrichment coverage analysis.

    Columns: STARS, ENRICHMENT_STATUS, COUNT
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT STARS, ENRICHMENT_STATUS, COUNT(*) AS COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        GROUP BY STARS, ENRICHMENT_STATUS
        ORDER BY STARS, ENRICHMENT_STATUS
    """)


def get_ingestion_timeline(session: Session) -> pd.DataFrame:
    """Return review ingestion volume bucketed by hour.

    Useful for identifying batch load spikes in the Admin timeline chart.

    Columns: HOUR, COUNT
    """
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
    """Return the most recent *limit* pipeline log entries.

    Query strategy:
        1. Tries ``DB_ADMIN_{env}.LOGS.TB_PIPELINE_LOGS`` (pipeline deploy logs).
        2. Falls back to ``DB_ADMIN_{env}.LOGS.TB_LOGS`` (Snowflake event table).
        3. Returns an empty DataFrame if both sources are inaccessible.

    Columns: TIMESTAMP, SEVERITY, MESSAGE
    """
    env = _env(session)
    try:
        return _df(session, f"""
            SELECT
                LOG_TIMESTAMP  AS TIMESTAMP,
                LEVEL          AS SEVERITY,
                MESSAGE
            FROM DB_ADMIN_{env}.LOGS.TB_PIPELINE_LOGS
            ORDER BY LOG_TIMESTAMP DESC
            LIMIT {limit}
        """)
    except Exception:
        try:
            return _df(session, f"""
                SELECT
                    TIMESTAMP,
                    RECORD['severity_text']::STRING  AS SEVERITY,
                    VALUE::STRING                    AS MESSAGE
                FROM DB_ADMIN_{env}.LOGS.TB_LOGS
                ORDER BY TIMESTAMP DESC
                LIMIT {limit}
            """)
        except Exception:
            return pd.DataFrame(columns=["TIMESTAMP", "SEVERITY", "MESSAGE"])


def get_null_stats(session: Session) -> dict:
    """Return counts of records missing sentiment, keywords, or body text.

    Used for data quality monitoring in the Admin Panel.

    Keys:
        no_sentiment (int): Rows where SENTIMENT IS NULL.
        no_keywords (int): Rows where KEYWORDS IS NULL.
        no_body (int): Rows where BODY IS NULL or blank.
    """
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


# ---------------------------------------------------------------------------
# Explorer — NPS
# ---------------------------------------------------------------------------

def get_nps_summary(session: Session) -> dict:
    """Return NPS component counts derived from star ratings.

    Mapping applied:
        1–2 stars → Detractors
        3–4 stars → Passives
        5 stars   → Promoters

    Keys:
        total (int): Total reviews with a non-null STARS value.
        detractors (int): Count of 1–2 star reviews.
        passives (int): Count of 3–4 star reviews.
        promoters (int): Count of 5-star reviews.
    """
    t = _ai(session)
    sql = f"""
        SELECT
            COUNT(*)                                              AS total,
            SUM(CASE WHEN STARS IN (1, 2) THEN 1 ELSE 0 END)    AS detractors,
            SUM(CASE WHEN STARS IN (3, 4) THEN 1 ELSE 0 END)    AS passives,
            SUM(CASE WHEN STARS = 5      THEN 1 ELSE 0 END)     AS promoters
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IS NOT NULL
    """
    row = _one(session, sql)
    keys = ["total", "detractors", "passives", "promoters"]
    return dict(zip(keys, row)) if row else {k: 0 for k in keys}


# ---------------------------------------------------------------------------
# Admin — Costs (ACCOUNT_USAGE, falls back gracefully if access denied)
# ---------------------------------------------------------------------------

def get_snowflake_costs(session: Session) -> dict:
    """Return credit and storage costs for the last 30 days from ACCOUNT_USAGE.

    Each metric is fetched independently and set to ``None`` when the current
    role lacks ``SNOWFLAKE.ACCOUNT_USAGE`` access; the Admin Panel shows "—"
    in that case.

    AI credits query tries ``METERING_HISTORY`` first, then falls back to
    ``CORTEX_FUNCTIONS_USAGE_HISTORY`` for accounts where the AI_SERVICES
    service type is not yet available.

    Keys:
        wh_credits (float | None): Warehouse credits used by ``WH_ADMIN_{env}``.
        ai_credits (float | None): AI/Cortex credits consumed.
        storage_gb (float | None): Average storage in GB across the three project databases.
        env (str): Environment prefix (DES/PRE/PRO).
    """
    env = _env(session)
    result: dict = {"wh_credits": None, "ai_credits": None, "storage_gb": None, "env": env}

    try:
        row = _one(session, f"""
            SELECT ROUND(SUM(CREDITS_USED), 2)
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
              AND WAREHOUSE_NAME = 'WH_ADMIN_{env}'
        """)
        result["wh_credits"] = float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        pass

    try:
        row = _one(session, """
            SELECT ROUND(SUM(CREDITS_USED), 2)
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
              AND SERVICE_TYPE = 'AI_SERVICES'
        """)
        if row and row[0] is not None:
            result["ai_credits"] = float(row[0])
        else:
            result["ai_credits"] = 0.0
    except Exception:
        try:
            row = _one(session, """
                SELECT ROUND(SUM(CREDITS_USED), 2)
                FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
                WHERE START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            """)
            result["ai_credits"] = float(row[0]) if row and row[0] is not None else 0.0
        except Exception:
            pass

    try:
        row = _one(session, f"""
            SELECT ROUND(SUM(AVERAGE_DATABASE_BYTES) / POWER(1024, 3), 2)
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
            WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
              AND DATABASE_NAME ILIKE '%{env}%'
        """)
        result["storage_gb"] = float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        pass

    return result


def get_pipeline_stage_durations(session: Session) -> pd.DataFrame:
    """Return per-stage pipeline duration in seconds, derived from TB_PIPELINE_LOGS.

    Classifies log messages into pipeline stages by keyword matching:
        - Migrations, Seed Load, Schemachange, AI Enrichment, App Deploy

    Duration is ``MAX(ts) - MIN(ts)`` per stage, clamped to a minimum of 1 second.
    Returns an empty DataFrame when TB_PIPELINE_LOGS is inaccessible.

    Columns: STAGE, FIRST_EVENT, LAST_EVENT, DURATION_SEC
    """
    env = _env(session)
    try:
        return _df(session, f"""
            WITH raw AS (
                SELECT
                    LOG_TIMESTAMP AS ts,
                    LOWER(MESSAGE) AS msg
                FROM DB_ADMIN_{env}.LOGS.TB_PIPELINE_LOGS
            ),
            staged AS (
                SELECT ts,
                    CASE
                        WHEN msg LIKE '%migration%'
                            THEN 'Migrations'
                        WHEN msg LIKE '%seed%' OR msg LIKE '%tb_reviews_src%'
                            THEN 'Seed Load'
                        WHEN msg LIKE '%schemachange%'
                            THEN 'Schemachange'
                        WHEN msg LIKE '%enrichment%' OR msg LIKE '%cortex%'
                          OR msg LIKE '%sentiment%' OR msg LIKE '%keyword%'
                            THEN 'AI Enrichment'
                        WHEN msg LIKE '%deploy%' OR msg LIKE '%streamlit%'
                            THEN 'App Deploy'
                        ELSE NULL
                    END AS STAGE
                FROM raw
            )
            SELECT
                STAGE,
                MIN(ts)                                            AS FIRST_EVENT,
                MAX(ts)                                            AS LAST_EVENT,
                GREATEST(DATEDIFF('second', MIN(ts), MAX(ts)), 1) AS DURATION_SEC
            FROM staged
            WHERE STAGE IS NOT NULL
            GROUP BY STAGE
            ORDER BY FIRST_EVENT
        """)
    except Exception:
        return pd.DataFrame(columns=["STAGE", "FIRST_EVENT", "LAST_EVENT", "DURATION_SEC"])


# ---------------------------------------------------------------------------
# AI Insights — product ranking
# ---------------------------------------------------------------------------

def get_best_products(session: Session, limit: int = 5) -> pd.DataFrame:
    """Return the top *limit* products ranked by average Cortex sentiment score.

    Only products with at least 3 reviews are considered to avoid noise from
    single-review outliers.

    Columns: ASIN, AVG_STARS, AVG_SENTIMENT, REVIEW_COUNT
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT
            ASIN,
            ROUND(AVG(STARS), 1)        AS AVG_STARS,
            ROUND(AVG(SENTIMENT), 3)    AS AVG_SENTIMENT,
            COUNT(*)                    AS REVIEW_COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE SENTIMENT IS NOT NULL AND ASIN IS NOT NULL
        GROUP BY ASIN
        HAVING COUNT(*) >= 3
        ORDER BY AVG_SENTIMENT DESC
        LIMIT {limit}
    """)


def get_worst_products(session: Session, limit: int = 5) -> pd.DataFrame:
    """Return the bottom *limit* products ranked by average Cortex sentiment score.

    Only products with at least 3 reviews are considered to avoid noise from
    single-review outliers.

    Columns: ASIN, AVG_STARS, AVG_SENTIMENT, REVIEW_COUNT
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT
            ASIN,
            ROUND(AVG(STARS), 1)        AS AVG_STARS,
            ROUND(AVG(SENTIMENT), 3)    AS AVG_SENTIMENT,
            COUNT(*)                    AS REVIEW_COUNT
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE SENTIMENT IS NOT NULL AND ASIN IS NOT NULL
        GROUP BY ASIN
        HAVING COUNT(*) >= 3
        ORDER BY AVG_SENTIMENT ASC
        LIMIT {limit}
    """)


def get_stars_sentiment_breakdown(session: Session) -> pd.DataFrame:
    """Return percentage breakdown of positive/neutral/negative sentiment per star rating.

    Sentiment thresholds:
        Positive  → SENTIMENT >  0.1
        Neutral   → SENTIMENT between −0.1 and 0.1
        Negative  → SENTIMENT < −0.1

    Ordered by STARS DESC (5 → 1) for top-down display in the Explorer bars chart.

    Columns: STARS, PCT_POSITIVE, PCT_NEUTRAL, PCT_NEGATIVE
    """
    t = _ai(session)
    return _df(session, f"""
        SELECT
            STARS,
            ROUND(100.0 * SUM(CASE WHEN SENTIMENT >  0.1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1) AS PCT_POSITIVE,
            ROUND(100.0 * SUM(CASE WHEN SENTIMENT BETWEEN -0.1 AND 0.1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1) AS PCT_NEUTRAL,
            ROUND(100.0 * SUM(CASE WHEN SENTIMENT < -0.1 THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1) AS PCT_NEGATIVE
        FROM {t}.TB_REVIEWS_ENRICHED
        WHERE STARS IS NOT NULL AND SENTIMENT IS NOT NULL
        GROUP BY STARS ORDER BY STARS DESC
    """)
