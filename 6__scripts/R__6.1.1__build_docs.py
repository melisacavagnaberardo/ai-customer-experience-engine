"""
Build Documentation Script
===========================
Automated Sphinx documentation pipeline (production-grade).

Key improvements:
- Safe module discovery (only real Python packages)
- Controlled apidoc scope
- Guaranteed importability via sys.path injection
- Clean reproducible builds
"""

import subprocess
from pathlib import Path
import yaml
import shutil
import stat
import os

# =====================================================
# PATHS
# =====================================================
ROOT = Path(__file__).resolve().parents[1]

CONFIG_FILE = ROOT / "1__config" / "docs_config.yaml"

if not CONFIG_FILE.exists():
    raise FileNotFoundError(f"Missing config: {CONFIG_FILE}")

config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))

SOURCE_DIR = ROOT / config["paths"]["source_dir"]
BUILD_DIR  = ROOT / config["paths"]["build_dir"]

HTML_DIR = BUILD_DIR / "html"

SOURCE_DIR.mkdir(parents=True, exist_ok=True)
HTML_DIR.mkdir(parents=True, exist_ok=True)

CONF_FILE  = SOURCE_DIR / "conf.py"
INDEX_FILE = SOURCE_DIR / "index.rst"
STATIC_DIR = SOURCE_DIR / "_static"


# =====================================================
# CONF.PY GENERATION
# =====================================================
def generate_conf():
    """Generate Sphinx configuration file."""

    project = config["project"]["name"]
    author  = config["project"]["author"]

    # IMPORTANT: only real Python code roots
    sys_paths = [
        str(ROOT),
        str(ROOT / "4__app"),
        str(ROOT / "4__app" / "1__pages"),
        str(ROOT / "4__app" / "2__services"),
        str(ROOT / "5__tests"),
        str(ROOT / "3__models"),
        str(ROOT / "2__infra"),
        str(ROOT / "6__scripts"),
    ]

    path_lines = "\n".join([f'sys.path.insert(0, r"{p}")' for p in sys_paths])

    # Pre-register page/service modules with simplified names so automodule can find them.
    # Needed because filenames like R__4.1.1__overview.py contain dots — invalid Python identifiers.
    preload = f"""
from pathlib import Path as _Path
import importlib.util as _ilu
from unittest.mock import MagicMock as _MM

for _m in ["snowflake", "snowflake.connector", "snowflake.connector.pandas_tools",
           "snowflake.snowpark", "snowflake.snowpark.functions",
           "snowflake.snowpark.context", "pandas", "streamlit", "pytest"]:
    sys.modules.setdefault(_m, _MM())

_ROOT = _Path(r"{str(ROOT)}")
# deploy must load before test files (sql_utils imports from R__deploy)
_py_dirs = (
    [_ROOT / "4__app" / _s for _s in ("2__services", "1__pages")]
    + [_ROOT / "6__scripts", _ROOT / "5__tests"]
)
_all_py = [_ROOT / "R__deploy.py"]
_all_py += [_f for _d in _py_dirs for _f in sorted(_d.glob("R__*.py"))]

for _f in _all_py:
    _name = _f.stem.split("__")[-1]
    try:
        _spec = _ilu.spec_from_file_location(_name, _f)
        _mod = _ilu.module_from_spec(_spec)
        sys.modules[_name] = _mod          # pre-register before exec so reloads work
        _spec.loader.exec_module(_mod)
    except Exception:
        sys.modules.pop(_name, None)       # clean up on failure
"""

    conf = f"""
import sys
import os

{path_lines}
{preload}
project = "{project}"
author = "{author}"
release = "1.0"
html_title = "{project} — Documentation"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
]

templates_path = []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_static_path = ["_static"]
html_css_files = ["custom.css"]

html_theme = "sphinx_rtd_theme"
html_theme_options = {{
    "navigation_depth": 4,
    "collapse_navigation": False,
    "sticky_navigation": True,
    "titles_only": False,
}}

# Mock external dependencies unavailable in the Sphinx build environment
autodoc_mock_imports = [
    "snowflake",
    "snowflake.connector",
    "snowflake.connector.pandas_tools",
    "snowflake.snowpark",
    "snowflake.snowpark.functions",
    "snowflake.snowpark.context",
    "pandas",
    "streamlit",
    "pytest",
]

autodoc_default_options = {{
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "private-members": False,
}}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
todo_include_todos = False

# Suppress Pygments warnings for SQL files containing Jinja {{ environment }} tokens
suppress_warnings = ["misc.highlighting_failure"]
"""

    CONF_FILE.write_text(conf.strip(), encoding="utf-8")
    print("[DOCS] conf.py generated")


# =====================================================
# INDEX
# =====================================================
def generate_index():
    """Generate ``index.rst`` with all toctrees grouped by project layer for full sidebar visibility."""
    name = config["project"]["name"]

    content = f"""\
{name}
{'=' * len(name)}

.. toctree::
   :maxdepth: 2
   :caption: Configuration (1__config)

   config_docs_config

.. toctree::
   :maxdepth: 2
   :caption: Infrastructure DDL (2__infra)

   infra_roles
   infra_databases
   infra_schemas
   infra_event_table
   infra_warehouses
   infra_resource_monitors
   infra_grants
   infra_ingest_objects
   infra_sp_universal_batch_raw_ingest
   infra_sp_create_silver_views
   infra_create_users

.. toctree::
   :maxdepth: 2
   :caption: Data Models (3__models)

   model_raw_products
   model_raw_reviews
   model_silver_views
   model_gold_views
   model_reviews_enriched
   model_ai_enrich
   model_ai_keywords

.. toctree::
   :maxdepth: 4
   :caption: Application (4__app)

   4__app
   overview
   explorer
   ai_insights
   admin
   queries
   snowflake_client

.. toctree::
   :maxdepth: 3
   :caption: Tests (5__tests)

   test_data_quality
   test_sql_utils

.. toctree::
   :maxdepth: 3
   :caption: Scripts (6__scripts)

   script_build_docs

.. toctree::
   :maxdepth: 2
   :caption: Seed Data (7__data)

   data_products
   data_reviews

.. toctree::
   :maxdepth: 3
   :caption: Deploy Script

   script_deploy
"""

    INDEX_FILE.write_text(content, encoding="utf-8")
    print("[DOCS] index.rst generated")


# =====================================================
# CSS
# =====================================================
def generate_css():
    """Write ``_static/custom.css`` with the blue minimalist RTD theme overrides."""
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    css = """
/* ═══════════════════════════════════════════════════════
   RTD THEME — Blue Minimalist Professional
   ═══════════════════════════════════════════════════════ */

/* ── Sidebar nav ───────────────────────────────────────── */
.wy-nav-side                          { background: #1a3a5c; }
.wy-side-nav-search                   { background: #12294a; padding: 16px 12px; }
.wy-side-nav-search input[type=text]  { border-color: #2b6cb0; border-radius: 4px; }
.wy-side-nav-search > a               { color: #ffffff; font-size: 18px; font-weight: 700; letter-spacing: 0.5px; }
.wy-side-nav-search > a:hover         { color: #bee3f8; }

.wy-menu-vertical a                   { color: #c8dff5; }
.wy-menu-vertical a:hover             { background: #0d2240; color: #ffffff; }
.wy-menu-vertical li.current > a,
.wy-menu-vertical li.current > a:hover{ background: #0d2240; color: #ffffff; font-weight: 600; }
.wy-menu-vertical li.toctree-l1.current > a { border-left: 3px solid #4299e1; }
.wy-menu-vertical li.toctree-l2.current > a { border-left: 3px solid #63b3ed; }

/* ── Top mobile bar ────────────────────────────────────── */
.wy-nav-top { background: #1a3a5c; }

/* ── Content area ──────────────────────────────────────── */
.wy-nav-content                       { background: #ffffff; max-width: 1000px; }
.wy-nav-content-wrap                  { background: #f5f9ff; }

.rst-content                          { font-family: 'Segoe UI', Arial, sans-serif; color: #1a202c; font-size: 15px; line-height: 1.75; }

/* ── Headings ───────────────────────────────────────────── */
.rst-content h1 { color: #1a3a5c; border-bottom: 2px solid #2b6cb0; padding-bottom: 8px; margin-top: 0; }
.rst-content h2 { color: #1a3a5c; border-bottom: 1px solid #bee3f8; padding-bottom: 4px; }
.rst-content h3 { color: #2b6cb0; }
.rst-content h4 { color: #2c5282; font-size: 1em; text-transform: uppercase; letter-spacing: 0.5px; }

/* ── Links ──────────────────────────────────────────────── */
.rst-content a                        { color: #2b6cb0; }
.rst-content a:hover                  { color: #1a3a5c; text-decoration: underline; }

/* ── Code inline ────────────────────────────────────────── */
.rst-content code, .rst-content tt   { background: #e8f4fd; color: #1a3a5c; border: none; padding: 1px 5px; border-radius: 3px; font-size: 0.87em; }

/* ── Code blocks ────────────────────────────────────────── */
.rst-content .highlight               { background: #f0f7ff; border-left: 4px solid #2b6cb0; border-radius: 0 4px 4px 0; }
.rst-content pre                      { background: #f0f7ff; border: none; }

/* ── API signatures ─────────────────────────────────────── */
.rst-content dl.py dt,
.rst-content dl.function dt,
.rst-content dl.method dt,
.rst-content dl.class dt             { background: #e8f4fd; border-left: 4px solid #2b6cb0; padding: 7px 14px; border-radius: 0 4px 4px 0; color: #1a3a5c; font-family: monospace; margin-bottom: 6px; }

.rst-content .viewcode-link           { color: #4a7ab5; font-size: 0.8em; }

/* ── Field lists (Args, Returns…) ───────────────────────── */
.rst-content .field-list th           { color: #2b6cb0; font-weight: 700; min-width: 80px; }
.rst-content .field-list td           { padding-left: 12px; }

/* ── Notes / warnings / tips ────────────────────────────── */
.rst-content .note                    { background: #e8f4fd; border-left-color: #2b6cb0; }
.rst-content .warning                 { background: #fff5f5; border-left-color: #c53030; }
.rst-content .tip                     { background: #f0fff4; border-left-color: #276749; }

/* ── Footer ─────────────────────────────────────────────── */
footer                                { color: #718096; font-size: 12px; border-top: 1px solid #e2e8f0; padding-top: 10px; }
"""

    (STATIC_DIR / "custom.css").write_text(css.strip(), encoding="utf-8")
    print("[DOCS] css generated")


# =====================================================
# CLEAN
# =====================================================
def _force_remove(func, path, _exc_info):
    """Error handler for ``shutil.rmtree`` that forces deletion of read-only files."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def clean():
    """Remove and recreate the HTML build directory for a clean build."""
    if HTML_DIR.exists():
        shutil.rmtree(HTML_DIR, onexc=_force_remove)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    print("[DOCS] clean")


# =====================================================
# SAFE APIDOC (CRITICAL FIX)
# =====================================================
def run_apidoc():
    """Generate RSTs for all project layers: Python code, SQL, YAML, CSV."""

    # Clean stale RSTs from previous runs
    for rst in SOURCE_DIR.glob("*.rst"):
        if rst.stem not in ("index",):
            rst.unlink()

    # ── 4__app entry point via sphinx-apidoc ─────────────────────────────────
    subprocess.run([
        "sphinx-apidoc",
        "-o", str(SOURCE_DIR),
        str(ROOT / "4__app"),
        str(ROOT / "4__app" / "1__pages"),
        str(ROOT / "4__app" / "2__services"),
        "--force", "--module-first", "--no-toc", "--implicit-namespaces",
    ], check=True)
    print("[DOCS] apidoc done")

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _py_rst(rst_name, module_name, title):
        """Write an ``automodule`` RST for a Python module pre-registered in sys.modules."""
        (SOURCE_DIR / f"{rst_name}.rst").write_text(
            f"{title}\n{'=' * len(title)}\n\n"
            f".. automodule:: {module_name}\n"
            f"   :members:\n   :undoc-members:\n   :private-members:\n   :show-inheritance:\n",
            encoding="utf-8",
        )

    def _file_rst(rst_name, title, rel_path, lang, lines=""):
        """Write a ``literalinclude`` RST for a non-Python file (SQL, YAML, CSV)."""
        lines_line = f"   :lines: {lines}\n" if lines else ""
        (SOURCE_DIR / f"{rst_name}.rst").write_text(
            f"{title}\n{'=' * len(title)}\n\n"
            f".. literalinclude:: {rel_path}\n"
            f"   :language: {lang}\n   :linenos:\n{lines_line}",
            encoding="utf-8",
        )

    # ── Pages (4__app/1__pages) ───────────────────────────────────────────────
    _page_titles = {
        "overview": "Overview Page", "explorer": "Explorer Page",
        "ai_insights": "AI Insights Page", "admin": "Admin Panel Page",
    }
    for f in sorted((ROOT / "4__app" / "1__pages").glob("R__*.py")):
        name = f.stem.split("__")[-1]
        _py_rst(name, name, _page_titles.get(name, name.replace("_", " ").title()))

    # ── Services (4__app/2__services) ─────────────────────────────────────────
    _svc_titles = {"queries": "Queries Service", "snowflake_client": "Snowflake Client"}
    for f in sorted((ROOT / "4__app" / "2__services").glob("R__*.py")):
        name = f.stem.split("__")[-1]
        _py_rst(name, name, _svc_titles.get(name, name.replace("_", " ").title()))

    # ── Tests (5__tests) ──────────────────────────────────────────────────────
    _test_titles = {"data_quality": "Data Quality Tests", "sql_utils": "SQL Utilities"}
    for f in sorted((ROOT / "5__tests").glob("R__*.py")):
        name = f.stem.split("__")[-1]
        _py_rst(f"test_{name}", name, _test_titles.get(name, name.replace("_", " ").title()))

    # ── Scripts ───────────────────────────────────────────────────────────────
    _py_rst("script_deploy",     "deploy",     "Deploy Pipeline")
    _py_rst("script_build_docs", "build_docs", "Build Docs Script")

    # ── Infrastructure DDL (2__infra/migrations) ──────────────────────────────
    _infra_titles = {
        "roles": "Roles", "databases": "Databases", "schemas": "Schemas",
        "event_table": "Event Table", "warehouses": "Warehouses",
        "resource_monitors": "Resource Monitors", "grants": "Grants",
        "ingest_objects": "Ingest Objects",
        "sp_universal_batch_raw_ingest": "SP — Universal Batch Raw Ingest",
        "sp_create_silver_views": "SP — Create Silver Views",
        "create_users": "Create Users",
    }
    for f in sorted((ROOT / "2__infra" / "migrations").glob("*.sql")):
        name = f.stem.split("__")[-1] if "__" in f.stem else f.stem
        title = _infra_titles.get(name, name.replace("_", " ").title())
        _file_rst(f"infra_{name}", title, f"../../2__infra/migrations/{f.name}", "sql")

    # ── Data Models SQL (3__models) ───────────────────────────────────────────
    _models = [
        ("model_raw_products",    "Raw Ingest — Products",     "1__raw/V3.1.1__call_sp_universal_raw_ingest_tb_products.sql"),
        ("model_raw_reviews",     "Raw Ingest — Reviews",      "1__raw/V3.1.2__call_sp_universal_raw_ingest_tb_reviews.sql"),
        ("model_silver_views",    "Silver Views",              "2__silver/V3.2.1__call_sp_create_silver_views.sql"),
        ("model_gold_views",      "Gold Views",                "3__gold/3.1__setup/V3.3.1__call_sp_create_gold_views.sql"),
        ("model_reviews_enriched","Reviews Enriched Table",    "3__gold/3.1__setup/V3.3.2__create_tb_reviews_enriched.sql"),
        ("model_ai_enrich",       "AI Enrichment — Sentiment", "3__gold/3.2__enrichment/V3.3.3__ai_enrich_reviews.sql"),
        ("model_ai_keywords",     "AI Enrichment — Keywords",  "3__gold/3.2__enrichment/V3.3.4__ai_keywords_strategic.sql"),
    ]
    for rst_name, title, rel in _models:
        _file_rst(rst_name, title, f"../../3__models/{rel}", "sql")

    # ── Configuration ─────────────────────────────────────────────────────────
    _file_rst("config_docs_config", "Documentation Config",
              "../../1__config/docs_config.yaml", "yaml")

    # ── Seed Data ─────────────────────────────────────────────────────────────
    _file_rst("data_products", "Seed Data — Products",
              "../../7__data/seeds/PRODUCTS.csv", "text", "1-5")
    _file_rst("data_reviews",  "Seed Data — Reviews",
              "../../7__data/seeds/REVIEWS.csv",  "text", "1-5")

    print("[DOCS] all RSTs generated")


# =====================================================
# MODULES TOC  (kept for reference; not called — index.rst now owns all toctrees)
# =====================================================
def generate_modules_toc():
    """Generate modules.rst grouped by project layer."""
    content = """\
API Reference
=============

.. toctree::
   :maxdepth: 2
   :caption: Configuration (1__config)

   config_docs_config

.. toctree::
   :maxdepth: 2
   :caption: Infrastructure DDL (2__infra)

   infra_roles
   infra_databases
   infra_schemas
   infra_event_table
   infra_warehouses
   infra_resource_monitors
   infra_grants
   infra_ingest_objects
   infra_sp_universal_batch_raw_ingest
   infra_sp_create_silver_views
   infra_create_users

.. toctree::
   :maxdepth: 2
   :caption: Data Models (3__models)

   model_raw_products
   model_raw_reviews
   model_silver_views
   model_gold_views
   model_reviews_enriched
   model_ai_enrich
   model_ai_keywords

.. toctree::
   :maxdepth: 4
   :caption: Application (4__app)

   4__app
   overview
   explorer
   ai_insights
   admin
   queries
   snowflake_client

.. toctree::
   :maxdepth: 3
   :caption: Tests (5__tests)

   test_data_quality
   test_sql_utils

.. toctree::
   :maxdepth: 3
   :caption: Scripts (6__scripts)

   script_build_docs

.. toctree::
   :maxdepth: 2
   :caption: Seed Data (7__data)

   data_products
   data_reviews

.. toctree::
   :maxdepth: 3
   :caption: Deploy Script

   script_deploy
"""
    (SOURCE_DIR / "modules.rst").write_text(content, encoding="utf-8")
    print("[DOCS] modules.rst generated (grouped by layer)")


# =====================================================
# BUILD
# =====================================================
def build():
    """Run ``sphinx-build`` to compile RST sources into HTML."""
    cmd = [
        "sphinx-build",
        "-b", "html",
        str(SOURCE_DIR),
        str(HTML_DIR),
    ]

    subprocess.run(cmd, check=True)
    print(f"[DOCS] generated -> {HTML_DIR / 'index.html'}")


# =====================================================
# PIPELINE
# =====================================================
def main():
    """Run the full documentation pipeline: conf → index → css → clean → apidoc → build."""
    print("=== DOCS PIPELINE ===")

    generate_conf()
    generate_index()
    generate_css()

    clean()
    run_apidoc()
    build()


if __name__ == "__main__":
    main()