import sys
import os

sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\4__app")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\3__models")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\2__infra")
sys.path.insert(0, r"C:\Users\melisa.cavagna\OneDrive - Accenture\Desktop\AI Customer Experience Engine\8__scripts")

project = "AI CUSTOMER EXPERIENCE ENGINE"
author = "Melisa Cavagna"
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

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

napoleon_google_docstring = True
todo_include_todos = False