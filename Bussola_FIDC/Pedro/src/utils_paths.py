import os
import sys

def resource_path(relative_path: str) -> str:
    """
    Caminho compatível com execução normal e com PyInstaller.
    - No EXE: usa sys._MEIPASS
    - Em dev: usa a raiz do projeto (pai da pasta src)
    """
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS
    else:
        # este arquivo está em: <projeto>/src/utils_paths.py (por exemplo)
        # então a raiz do projeto é o pai da pasta src
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    return os.path.join(base_path, relative_path)
