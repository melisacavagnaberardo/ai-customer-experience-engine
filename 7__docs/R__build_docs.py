##import os
##import subprocess
##import sys
##from pathlib import Path
##
##ROOT = Path(__file__).resolve().parents[1]
##DOCS_DIR = ROOT / "docs"
##SOURCE_DIR = DOCS_DIR / "source"
##BUILD_DIR = DOCS_DIR / "build"
##
##def run(cmd, cwd=None):
##    print(f"Ejecutando: {' '.join(cmd)}")
##    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
##
##    if result.returncode != 0:
##        print(result.stdout)
##        print(result.stderr)
##        raise Exception("Error en build de documentación")
##
##    return result.stdout
##
##
##def main():
##    print("=== GENERANDO DOCUMENTACIÓN SPHINX ===")
##
##    # 1. Verifica que Sphinx está instalado
##    try:
##        import sphinx
##    except ImportError:
##        raise Exception("Sphinx no está instalado. Ejecuta pip install sphinx")
##
##    # 2. Limpia build anterior
##    if BUILD_DIR.exists():
##        print("Limpiando build anterior...")
##        subprocess.run(["rm", "-rf", str(BUILD_DIR)], shell=True)
##
##    # 3. Ejecuta build HTML
##    cmd = [
##        "sphinx-build",
##        "-b", "html",
##        str(SOURCE_DIR),
##        str(BUILD_DIR / "html")
##    ]
##
##    run(cmd, cwd=ROOT)
##
##    print("\n=== DOCUMENTACIÓN GENERADA ===")
##    print(f"Abre: {BUILD_DIR / 'html' / 'index.html'}")
##
##
##if __name__ == "__main__":
##    main()


import os