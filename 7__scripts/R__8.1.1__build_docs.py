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
        str(ROOT / "3__models"),
        str(ROOT / "2__infra"),
        str(ROOT / "7__scripts"),
    ]

    path_lines = "\n".join([f'sys.path.insert(0, r"{p}")' for p in sys_paths])

    conf = f"""
import sys
import os

{path_lines}

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

# Mockear dependencias externas que no están disponibles en el entorno Sphinx
autodoc_mock_imports = [
    "snowflake",
    "snowflake.connector",
    "snowflake.connector.pandas_tools",
    "snowflake.snowpark",
    "snowflake.snowpark.functions",
    "pandas",
    "streamlit",
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
"""

    CONF_FILE.write_text(conf.strip(), encoding="utf-8")
    print("[DOCS] conf.py generated")


# =====================================================
# INDEX
# =====================================================
def generate_index():
    """Generate ``index.rst``, the Sphinx documentation entry point."""
    name = config["project"]["name"]

    content = f"""
{name}
{'=' * len(name)}

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   modules
"""

    INDEX_FILE.write_text(content.strip(), encoding="utf-8")
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
    """
    Only scan REAL python packages, not entire repo.
    Excludes subdirectories whose filenames contain dots (R__4.x.x style),
    which break Python's module import system during autodoc.
    """

    # Clean stale RSTs from previous runs before regenerating
    for rst in SOURCE_DIR.glob("*.rst"):
        if rst.stem not in ("index",):
            rst.unlink()

    # Only document the app entry point; exclude subdirs with dotted filenames
    subprocess.run([
        "sphinx-apidoc",
        "-o", str(SOURCE_DIR),
        str(ROOT / "4__app"),
        str(ROOT / "4__app" / "1__pages"),    # excluded: dotted filenames break autodoc
        str(ROOT / "4__app" / "2__services"),  # excluded: dotted filenames break autodoc
        "--force",
        "--module-first",
        "--no-toc",
        "--implicit-namespaces",
    ], check=True)

    print("[DOCS] apidoc done")


# =====================================================
# MODULES TOC  (requerido porque apidoc corre con --no-toc)
# =====================================================
def generate_modules_toc():
    """Genera modules.rst listando todos los RST descubiertos por apidoc."""
    rst_files = sorted([
        f.stem for f in SOURCE_DIR.glob("*.rst")
        if f.stem not in ("index", "modules")
    ])

    lines = ["API Reference\n=============\n\n.. toctree::\n   :maxdepth: 4\n\n"]
    for name in rst_files:
        lines.append(f"   {name}\n")

    (SOURCE_DIR / "modules.rst").write_text("".join(lines), encoding="utf-8")
    print(f"[DOCS] modules.rst generated ({len(rst_files)} modules)")


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
    """Run the full documentation pipeline: conf → index → css → clean → apidoc → toc → build."""
    print("=== DOCS PIPELINE ===")

    generate_conf()
    generate_index()
    generate_css()

    clean()
    run_apidoc()
    generate_modules_toc()
    build()


if __name__ == "__main__":
    main()