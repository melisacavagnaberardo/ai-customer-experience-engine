# AI Customer Experience Engine

End-to-end data platform for AI-powered customer review analysis, built on Snowflake. Transforms raw product reviews into actionable intelligence using a medallion architecture, Cortex AI enrichment, and a role-gated Streamlit application.

---

## Architecture overview

```
                       +-----------------------------------------------------+
                       |                   SNOWFLAKE                          |
                       |                                                       |
  CSV seeds ---------> |  SOURCE --> RAW --> SILVER --> GOLD --> AI           | --> Streamlit
  (PRODUCTS, REVIEWS)  |                                                       |
                       |  DB_SOURCE  DB_RAW   DB_SILVER  DB_GOLD   DB_GOLD   |
                       +-----------------------------------------------------+
                                             |
                              DB_ADMIN (platform, logs, schemachange)
```

| Layer | Database | Contents |
|---|---|---|
| SOURCE | `DB_SOURCE_<env>` | Staging tables loaded from CSV via Python connector |
| RAW | `DB_RAW_<env>` | Hashed, immutable copy — one row per source record |
| SILVER | `DB_SILVER_<env>` | Cleansed, typed, deduplicated views |
| GOLD | `DB_GOLD_<env>.GOLD` | Business views joining reviews + products |
| AI | `DB_GOLD_<env>.AI` | `TB_REVIEWS_ENRICHED` — Cortex-enriched fact table |
| Admin | `DB_ADMIN_<env>` | Schemachange history, stored procedures, event logs |

---

## Tech stack

| Component | Technology |
|---|---|
| Data warehouse | Snowflake |
| AI enrichment | Snowflake Cortex — `SENTIMENT`, `COMPLETE` (llama3.1-8b) |
| Schema versioning | schemachange (V__ / R__ SQL scripts) |
| Orchestration | Python (`R__deploy.py`) |
| Application | Streamlit + Plotly |
| Testing | pytest |
| Documentation | Sphinx + RTD theme |

---

## Repository structure

```
.
├── 1__config/                  YAML configuration (docs pipeline)
├── 2__infra/
│   └── migrations/             DDL scripts — roles, databases, warehouses, SPs
│       ├── 1.1.x__*.sql        Infrastructure objects (run once, ordered)
│       ├── 1.1.9__sp_universal_batch_raw_ingest.sql
│       ├── 1.10__sp_create_silver_views.sql
│       └── 1.11__create_users.sql
├── 3__models/                  schemachange-managed SQL models
│   ├── 1__raw/                 V1.x — source ingest calls
│   ├── 2__silver/              V2.x — Silver layer view creation
│   └── 3__gold/                V3.x — Gold + AI enrichment
│       ├── 3.1__setup/
│       │   ├── V3.3.1__call_sp_create_gold_views.sql
│       │   └── V3.3.2__create_tb_reviews_enriched.sql
│       └── 3.2__enrichment/
│           ├── V3.3.3__ai_enrich_reviews.sql   (SENTIMENT, all rows)
│           └── V3.3.4__ai_keywords_strategic.sql  (COMPLETE, 500 strategic rows)
├── 4__app/                     Streamlit application
│   ├── R__4__app.py            Entry point — login, routing, CSS
│   ├── 1__pages/
│   │   ├── R__4.1.1__overview.py
│   │   ├── R__4.1.2__explorer.py
│   │   ├── R__4.1.3__ai_insights.py
│   │   └── R__4.1.4__admin.py
│   └── 2__services/
│       ├── R__4.2.1__queries.py
│       └── R__4.2.2__snowflake_client.py
├── 5__tests/                   pytest test suite
│   ├── R__5.1.1__data_quality.py  CSV seed validation (20 tests)
│   └── R__5.1.2__sql_utils.py     Python utility unit tests (26 tests)
├── 6__scripts/
│   └── R__6.1.1__build_docs.py    Automated Sphinx pipeline
├── 7__data/
│   └── seeds/                  PRODUCTS.csv, REVIEWS.csv (~4 994 rows)
├── 8__docs/                    Sphinx HTML documentation
│   └── source/
├── context.md                  Dataset sampling decisions and cleaning log
├── pytest.ini
├── requirements.txt
└── R__deploy.py                One-shot deployment orchestrator
```

---

## AI enrichment pipeline

Two-tier design to optimise Cortex credit consumption:

```
All reviews (~4 994)
        |
        v
  V3.3.3  FAST ENRICHMENT
  CORTEX.SENTIMENT(BODY)          -> SENTIMENT float, ENRICHMENT_STATUS = 'FAST_ONLY'
        |
        v
  V3.3.4  DEEP ENRICHMENT  --- strategic filter --->  ~500 rows
  CORTEX.COMPLETE(llama3.1-8b)    -> KEYWORDS csv,    ENRICHMENT_STATUS = 'FULLY_ENRICHED'
```

**Strategic filter** (V3.3.4): `VERIFIED_PURCHASE = TRUE` OR `FOUND_HELPFUL > 5` OR `STARS IN (1, 5)`.
Focuses expensive COMPLETE calls on reviews with the highest business signal.

---

## Data model — `TB_REVIEWS_ENRICHED`

| Column | Type | Description |
|---|---|---|
| `ID` | STRING PK | Original review identifier |
| `ASIN` | STRING | Product identifier (join key) |
| `BODY` | STRING | Review text |
| `STARS` | NUMBER | Rating 1–5 |
| `ROW_HASH` | STRING | MD5 of source row — used for MERGE deduplication |
| `SENTIMENT` | FLOAT | Cortex score -1 (negative) to +1 (positive) |
| `SUMMARY` | STRING | Reserved (SUMMARIZE removed — cost optimisation) |
| `KEYWORDS` | STRING | Comma-separated key drivers (FULLY_ENRICHED only) |
| `ENRICHMENT_STATUS` | VARCHAR(20) | `FAST_ONLY` or `FULLY_ENRICHED` |
| `CREATED_AT` | TIMESTAMP_TZ | Enrichment timestamp |

---

## Access control (RBAC)

| Role | Access |
|---|---|
| `<env>_ADMIN_FR` | Full platform admin — all databases, SPs, event logs |
| `<env>_REPORT_FR` | Read-only on `DB_GOLD_<env>.AI` — Streamlit consumer |

The Streamlit app gates pages based on `CURRENT_ROLE()` after login:
- `<env>_REPORT_FR` → Overview, Explorer, AI Insights
- `<env>_ADMIN_FR` → + Admin Panel (enrichment health, ingestion timeline, event logs)

---

## Snowflake requirements

| Feature | Required for |
|---|---|
| Snowflake Cortex (`SENTIMENT`, `COMPLETE`) | AI enrichment pipeline (steps V3.3.3, V3.3.4) |
| Streamlit in Snowflake (SiS) | Production app deployment |
| `ACCOUNT_USAGE` schema access | Cost metrics on the Admin Panel |

Cortex and SiS are available on **Enterprise edition and above** (including trial accounts). `llama3.1-8b` is free during trial.

---

## Prerequisites

```
Python 3.10+
snowflake-connector-python
snowflake-snowpark-python
schemachange
streamlit
plotly
pandas
sphinx
sphinx-rtd-theme
pytest
pyyaml
```

Install:
```bash
pip install -r requirements.txt
```

---

## Deploy

The full pipeline runs in a single command:

```bash
python R__deploy.py
```

On startup, the script prompts for:

```
Snowflake account   : <your-account-identifier>
Snowflake user      : <your-user>
Snowflake password  : (hidden)
Environment         : <env-prefix, e.g. DES / PRE / PRO>
```

Stages executed in order:

| Step | Function | What it does |
|---|---|---|
| 1 | `run_migrations()` | Applies all `2__infra/migrations/*.sql` — creates roles, DBs, schemas, warehouses, SPs |
| 2 | `run_seed()` | Truncates and reloads `TB_PRODUCTS_SRC` and `TB_REVIEWS_SRC` from CSV |
| 3 | `run_schemachange()` | Runs versioned SQL models in `3__models/` via schemachange |
| 4 | `run_app_deploy()` | PUTs Streamlit app files to stage and creates the SiS object |
| 5 | `run_docs()` | Generates Sphinx HTML documentation |

To run stages individually, comment/uncomment in the `__main__` block of `R__deploy.py`.

---

## Streamlit app

The application is deployed as a **Streamlit in Snowflake (SiS)** object — step 4 of the deploy pipeline handles this automatically via `run_app_deploy()`.

For **local development**, the app can also be run against a live Snowflake connection:

```bash
streamlit run 4__app/R__4__app.py
```

Navigation is role-gated automatically:
- `<env>_REPORT_FR` → Overview, Explorer, AI Insights
- `<env>_ADMIN_FR` → + Admin Panel

---

## Tests

```bash
pytest
```

46 tests, no Snowflake connection required:

| File | Scope | Tests |
|---|---|---|
| `5__tests/R__5.1.1__data_quality.py` | CSV seed validation | 20 |
| `5__tests/R__5.1.2__sql_utils.py` | Python utility functions | 26 |

---

## Documentation

```bash
python 6__scripts/R__6.1.1__build_docs.py
```

Output: `8__docs/build/html/index.html`

---

## Design decisions — AI enrichment

The AI pipeline applies a **two-tier enrichment strategy** designed for efficiency at any scale:

- **Selective LLM usage** — `CORTEX.COMPLETE` runs only on the 500 reviews with the highest business signal (verified purchases, helpful votes, extreme ratings). Blanket LLM calls on all rows produce diminishing returns; targeted enrichment yields better signal-to-noise.
- **Right-sized model** — `llama3.1-8b` is sufficient for structured keyword extraction. Larger models add latency without meaningful quality gain for this task.
- **Stratified dataset** — the ~5 000-row seed is a representative sample across all star ratings, not a random slice. See `context.md` for sampling methodology.

To scale: increase `LIMIT 500` in V3.3.4, adjust the strategic filter, and swap in `mistral-large` or `llama3.1-70b` if richer keyword quality is required.
