import sys
import os

sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\4__app")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\4__app\1__pages")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\4__app\2__services")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\5__tests")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\3__models")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\2__infra")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\6__scripts")

from pathlib import Path as _Path
import importlib.util as _ilu
from unittest.mock import MagicMock as _MM

for _m in ["snowflake", "snowflake.connector", "snowflake.connector.pandas_tools",
           "snowflake.snowpark", "snowflake.snowpark.functions",
           "snowflake.snowpark.context", "pandas", "streamlit", "pytest"]:
    sys.modules.setdefault(_m, _MM())

_ROOT = _Path(r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine")
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

project = "AI CUSTOMER EXPERIENCE ENGINE"
author = "Melisa Cavagna"
release = "1.0"
html_title = "AI CUSTOMER EXPERIENCE ENGINE — Documentation"

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
html_theme_options = {
    "navigation_depth": 4,
    "collapse_navigation": False,
    "sticky_navigation": True,
    "titles_only": False,
}

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

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "private-members": False,
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
todo_include_todos = False

# Suppress Pygments warnings for SQL files containing Jinja { environment } tokens
suppress_warnings = ["misc.highlighting_failure"]