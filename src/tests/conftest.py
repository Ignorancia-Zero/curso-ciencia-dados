import os
import sys
from pathlib import Path

import pytest

try:
    from src.aquisicao.inep.censo_escola import EscolaETL
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))
    from src.aquisicao.inep.censo_escola import EscolaETL


@pytest.fixture(scope="session")
def test_path():
    return Path(os.path.dirname(__file__))


@pytest.fixture(scope="session")
def dados_path(test_path):
    return test_path / "dados"


@pytest.fixture(scope="session")
def saida_path(test_path):
    return test_path / "saida"


@pytest.fixture(scope="session")
def escola_etl(dados_path, saida_path):
    etl = EscolaETL(
        entrada=str(dados_path),
        saida=str(saida_path / "aquisicao"),
    )
    etl._inep = {k: "" for k in os.listdir(dados_path / "censo_escolar") if int(k[3]) % 2 == 0}

    return etl
