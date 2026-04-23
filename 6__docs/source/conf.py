import sys
import os

sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\4__app")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\3__models")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\2__infra")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\7__scripts")

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

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "private-members": False,
}

napoleon_google_docstring = True
napoleon_numpy_docstring = False
todo_include_todos = False