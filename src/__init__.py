import os
import sys
from pathlib import Path

import rarfile

if sys.platform.startswith("win"):
    rarfile.UNRAR_TOOL = str(
        Path(os.path.dirname(__file__)).parent / "suporte/WinRAR/unrar.exe"
    )
