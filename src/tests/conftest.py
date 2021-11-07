import os
import sys
from pathlib import Path

import pytest

try:
    from src.io.data_store import DataStore
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))
finally:
    from src.io.data_store import DataStore


@pytest.fixture(scope="session")
def test_path():
    return Path(os.path.dirname(__file__))


@pytest.fixture(scope="session")
def dados_path(test_path):
    return test_path / "dados"


@pytest.fixture(scope="session")
def ds():
    return DataStore("teste")


@pytest.fixture(scope="session")
def ano():
    return 2020
