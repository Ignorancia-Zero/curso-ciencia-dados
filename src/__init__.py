import os
import sys
import warnings
from pathlib import Path

import rarfile

if sys.platform.startswith("win"):
    rarfile.UNRAR_TOOL = str(
        Path(os.path.dirname(__file__)).parent / "suporte/WinRAR/unrar.exe"
    )

# ignora warning que vem de pacotes importados
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    # deprecation warning vindo do uso de imp ao invés de importlib
    # noinspection PyUnresolvedReferences
    from pydrive2.auth import GoogleAuth
