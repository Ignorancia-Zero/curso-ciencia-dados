import os
import sys
from pathlib import Path

import pytest

try:
    from src.aquisicao.inep.censo_escola import EscolaETL
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))
    sys.path.append(
        "C:\\ProgramData\\Anaconda3\\envs\\curso-ciencia-dados\\Lib\\site-packages"
    )
finally:
    from src.configs import COLECAO_DADOS_WEB
    from src.aquisicao.inep.censo_escola import EscolaETL
    from src.io.data_store import DataStore, Documento


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
    etl = EscolaETL(ds=DataStore("teste"))
    etl._inep = {
        Documento(
            etl._ds,
            referencia=dict(
                nome=k,
                colecao=COLECAO_DADOS_WEB,
                pasta=etl._base,
            ),
        ): ""
        for k in os.listdir(dados_path / f"{COLECAO_DADOS_WEB}/censo_escolar")
        if int(k[3]) % 2 == 0
    }

    return etl
