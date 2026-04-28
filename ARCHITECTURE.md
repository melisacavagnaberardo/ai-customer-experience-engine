# Architecture Decision Records

Key design decisions for the AI Customer Experience Engine. Each entry documents the context, the decision taken, and the reasoning behind it.

---

## ADR-1 — Five databases instead of five schemas

**Decision:** Each architectural layer (SOURCE, RAW, SILVER, GOLD, ADMIN) lives in a separate Snowflake database, not in schemas within a single database.

**Why:** Snowflake RBAC is cleanest at the database level. Separating layers into databases lets `REPORT_FR` have SELECT access on `DB_GOLD` with zero visibility into `DB_RAW` or `DB_ADMIN` — enforced by the platform, not by convention. Schema-level separation within one database achieves the same logical isolation but requires more complex grant management and is easier to misconfigure under ACCOUNTADMIN.

**Trade-off:** Higher object count, fully-qualified names throughout. Justified because role isolation is a first-class requirement.

---

## ADR-2 — Two-tier Cortex AI enrichment

**Decision:** `CORTEX.SENTIMENT()` runs on all ~5 000 reviews. `CORTEX.COMPLETE(llama3.1-8b)` runs only on a strategic subset of ~500 rows.

**Why:** `CORTEX.COMPLETE()` is orders of magnitude more expensive than `SENTIMENT()`. Blanket LLM calls on all rows produce diminishing returns: the keyword signal from a low-quality, unverified 3-star review has negligible business value. The strategic filter — `VERIFIED_PURCHASE = TRUE OR FOUND_HELPFUL > 5 OR STARS IN (1, 5)` — targets the highest-signal reviews and keeps credit consumption bounded and predictable.

**Trade-off:** ~90% of reviews carry `FAST_ONLY` status. This is intentional. Sentiment covers the full corpus; LLM enrichment covers the actionable subset. To scale up: increase `LIMIT 500` in `V3.3.4` or swap `llama3.1-8b` for `mistral-large`.

---

## ADR-3 — APPEND_ONLY stream for incremental processing

**Decision:** `STR_REVIEWS_NEW` is created with `APPEND_ONLY = TRUE` on `DB_RAW.RAW.TB_REVIEWS`.

**Why:** The ingestion SP (`SP_UNIVERSAL_BATCH_RAW_INGEST`) uses a MERGE keyed on `ROW_HASH`. All new records arrive as INSERTs — the SP never updates or deletes existing rows. `APPEND_ONLY` streams are more efficient in this scenario: they track only INSERT metadata and skip the overhead of capturing UPDATE/DELETE change vectors that will never occur.

**Trade-off:** If the ingestion pattern ever changes to include updates (e.g., moderated reviews), the stream must be recreated without `APPEND_ONLY`.

---

## ADR-4 — Single XSMALL warehouse

**Decision:** One warehouse (`WH_ADMIN_{env}`, XSMALL, auto-suspend 60s) handles all workloads: migrations, seed loads, schemachange models, the incremental task DAG, and the Streamlit app queries.

**Why:** The workload is sequential and batch-oriented — no concurrent queries compete for the same warehouse. XSMALL is right-sized for these workloads. Auto-suspend at 60 seconds minimises idle credit consumption between pipeline stages.

**When to change:** Add a dedicated `WH_QUERY_{env}` (XSMALL) for the Streamlit app if multiple users query concurrently, to avoid the app blocking on warehouse resume after enrichment tasks.

---

## ADR-5 — Environment prefix pattern (DES / PRE / PRO)

**Decision:** All Snowflake objects are prefixed with the environment name (e.g., `DB_GOLD_DES`, `DES_ADMIN_FR`). All environments coexist in a single Snowflake account.

**Why:** Snowflake trial accounts are single-account. The prefix pattern provides logical isolation at near-zero cost and enables the full DES → PRE → PRO promotion workflow to be demonstrated within one account. Role-based access ensures `DES_ADMIN_FR` cannot access `PRO_` objects.

**Enterprise path:** In production, each environment maps to a separate Snowflake account, eliminating cross-environment blast radius risk. The object naming convention remains identical, making the migration straightforward — update connection strings and re-run `R__deploy.py`.

---

## ADR-6 — CLUSTER BY (ASIN, STARS) on TB_REVIEWS_ENRICHED

**Decision:** `TB_REVIEWS_ENRICHED` is clustered on `(ASIN, STARS)`.

**Why:** Analytical queries on this table filter and aggregate by product (`ASIN`) and rating (`STARS`) — the Explorer and AI Insights pages are built entirely on these two dimensions. Clustering collocates micro-partitions along these axes, reducing the number of partitions scanned per query. At 5 000 rows the effect is immeasurable; it becomes material above ~1 M rows.

---

## ADR-7 — Daily cost snapshot (TB_COST_SNAPSHOT)

**Decision:** `TSK_COST_SNAPSHOT` materialises ACCOUNT_USAGE cost metrics into `DB_ADMIN.LOGS.TB_COST_SNAPSHOT` daily. The Admin Panel reads from this table instead of querying ACCOUNT_USAGE in real time.

**Why:** ACCOUNT_USAGE views have a 45-minute ingestion latency and are expensive to aggregate on each page load. Pre-aggregating once per day into a single-row snapshot makes the Admin Panel load in milliseconds instead of seconds, and decouples the UI from ACCOUNT_USAGE availability. The live fallback remains for accounts where the snapshot task hasn't run yet.
