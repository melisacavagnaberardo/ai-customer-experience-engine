-- =====================================================
-- 1.1.10__sp_ai_keywords_retry.sql
-- Layer   : ADMIN / PLATFORM
-- Purpose : Row-level keyword extraction with retry + backoff.
--
-- Replaces the raw CORTEX.COMPLETE UPDATE in V3.3.4 and TSK_AI_KEYWORDS.
-- Each row is attempted up to MAX_RETRIES times (exponential backoff).
-- On exhausted retries the row is marked ENRICHMENT_STATUS = 'FAILED'
-- and the error is stored in ERROR_MESSAGE for later inspection.
-- A WARN entry is written to TB_PIPELINE_LOGS when any row fails.
-- =====================================================

USE ROLE {{ environment }}_ADMIN_FR;
USE DATABASE DB_ADMIN_{{ environment }};
USE SCHEMA PLATFORM;

CREATE OR REPLACE PROCEDURE SP_AI_KEYWORDS_RETRY(ENV VARCHAR)
    RETURNS VARIANT
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS $$
import time
import json

MAX_RETRIES = 3
BATCH_SIZE  = 500


def run(session, env: str) -> dict:
    db_gold  = f"DB_GOLD_{env}"
    tbl      = f"{db_gold}.AI.TB_REVIEWS_ENRICHED"
    view_rev = f"{db_gold}.GOLD.VW_TB_REVIEWS"
    log_tbl  = f"DB_ADMIN_{env}.LOGS.TB_PIPELINE_LOGS"

    def _log(msg: str, level: str = "INFO") -> None:
        safe = msg.replace("'", "''")
        try:
            session.sql(
                f"INSERT INTO {log_tbl} (LEVEL, MESSAGE) VALUES ('{level}', '{safe}')"
            ).collect()
        except Exception:
            pass  # logging must never crash the SP

    # ── Fetch strategic rows pending keyword extraction ────────────────────────
    rows = session.sql(f"""
        SELECT t.ID, t.BODY, COALESCE(t.RETRY_COUNT, 0) AS PRIOR_RETRIES
        FROM {tbl} t
        JOIN {view_rev} r ON t.ID = r.ID
        WHERE t.KEYWORDS IS NULL
          AND t.ENRICHMENT_STATUS NOT IN ('FULLY_ENRICHED', 'FAILED')
          AND t.BODY IS NOT NULL
          AND TRIM(t.BODY) <> ''
          AND (
              r.VERIFIED_PURCHASE = 'TRUE'
           OR r.FOUND_HELPFUL > 5
           OR t.STARS IN (1, 5)
          )
        ORDER BY r.FOUND_HELPFUL DESC NULLS LAST, t.STARS ASC
        LIMIT {BATCH_SIZE}
    """).collect()

    processed = 0
    failed    = 0
    retried   = 0

    for row in rows:
        row_id   = row["ID"]
        body     = row["BODY"]
        prompt   = (
            "Extract 3 to 5 key drivers or topics from this customer review. "
            "Return ONLY a comma-separated list.\n\nReview:\n" + body
        )

        kw_array = None   # will hold a Python list → stored as Snowflake ARRAY
        last_err = None
        attempts = 0

        for attempt in range(MAX_RETRIES):
            attempts = attempt + 1
            try:
                result = session.sql(
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', ?)",
                    params=[prompt]
                ).collect()
                raw_csv  = result[0][0] or ""
                kw_array = [k.strip() for k in raw_csv.split(',') if k.strip()]
                if attempt > 0:
                    retried += 1
                break
            except Exception as exc:
                last_err = str(exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)   # 1 s → 2 s between retries

        safe_id = row_id.replace("'", "''")

        if kw_array is not None:
            # Store as native ARRAY via PARSE_JSON — enables ARRAY_CONTAINS / FLATTEN
            safe_kw_json = json.dumps(kw_array).replace("'", "''")
            session.sql(f"""
                UPDATE {tbl}
                SET KEYWORDS          = PARSE_JSON('{safe_kw_json}')::ARRAY,
                    ENRICHMENT_STATUS = 'FULLY_ENRICHED',
                    RETRY_COUNT       = {attempts - 1}
                WHERE ID = '{safe_id}'
            """).collect()
            processed += 1
        else:
            safe_err = (last_err or "unknown error")[:1990].replace("'", "''")
            session.sql(f"""
                UPDATE {tbl}
                SET ENRICHMENT_STATUS = 'FAILED',
                    ERROR_MESSAGE     = '{safe_err}',
                    FAILED_AT         = CURRENT_TIMESTAMP(),
                    RETRY_COUNT       = {MAX_RETRIES}
                WHERE ID = '{safe_id}'
            """).collect()
            failed += 1

    summary = {
        "processed": processed,
        "failed":    failed,
        "retried":   retried,
        "total":     len(rows),
    }

    _log(
        f"SP_AI_KEYWORDS_RETRY completed — "
        f"processed={processed} failed={failed} retried={retried} total={len(rows)}"
    )

    if failed > 0:
        _log(
            f"ALERT: {failed} review(s) failed keyword extraction after {MAX_RETRIES} retries. "
            f"Rows marked ENRICHMENT_STATUS=FAILED. Inspect ERROR_MESSAGE in {tbl}.",
            "WARN"
        )

    return summary
$$;
