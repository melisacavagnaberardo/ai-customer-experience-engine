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
import sys

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
        str(ROOT / "8__scripts"),
    ]

    path_lines = "\n".join([f'sys.path.insert(0, r"{p}")' for p in sys_paths])

    conf = f"""
import sys
import os

{path_lines}

project = "{project}"
author = "{author}"
release = "1.0"

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

html_theme = "alabaster"

autodoc_default_options = {{
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}}

napoleon_google_docstring = True
todo_include_todos = False
"""

    CONF_FILE.write_text(conf.strip(), encoding="utf-8")
    print("[DOCS] conf.py generated")


# =====================================================
# INDEX
# =====================================================
def generate_index():
    name = config["project"]["name"]

    content = f"""
{name}
{'=' * len(name)}

.. toctree::
   :maxdepth: 2
   :caption: Modules

   modules
"""

    INDEX_FILE.write_text(content.strip(), encoding="utf-8")
    print("[DOCS] index.rst generated")


# =====================================================
# CSS
# =====================================================
def generate_css():
    STATIC_DIR.mkdir(parents=True, exist_ok=True)

    (STATIC_DIR / "custom.css").write_text(
        "body { font-family: Arial; }",
        encoding="utf-8"
    )

    print("[DOCS] css generated")


# =====================================================
# CLEAN
# =====================================================
def _force_remove(func, path, _exc):
    os.chmod(path, stat.S_IWRITE)
    func(path)


def clean():
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
    """

    targets = [
        ROOT / "4__app",
        ROOT / "3__models",
        ROOT / "2__infra",
        ROOT / "8__scripts",
    ]

    for t in targets:
        if not t.exists():
            continue

        cmd = [
            "sphinx-apidoc",
            "-o", str(SOURCE_DIR),
            str(t),
            "--force",
            "--module-first",
            "--no-toc"
        ]

        subprocess.run(cmd, check=True)

    print("[DOCS] apidoc done")


# =====================================================
# BUILD
# =====================================================
def build():
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
    print("=== DOCS PIPELINE ===")

    generate_conf()
    generate_index()
    generate_css()

    clean()
    run_apidoc()
    build()


if __name__ == "__main__":
    main()