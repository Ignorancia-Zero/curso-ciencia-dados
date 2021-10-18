import os
import sys
from pathlib import Path

import pytest

try:
    from src.aquisicao.inep.censo_escola import EscolaETL
except ModuleNotFoundError:
    sys.path.append(str(Path(os.path.dirname(__file__)).parent.parent))
finally:
    from src.configs import COLECAO_DADOS_WEB
    from src.aquisicao.inep.censo_escola import EscolaETL
    from src.aquisicao.inep.censo_gestor import GestorETL
    from src.aquisicao.inep.censo_turma import TurmaETL
    from src.io.data_store import DataStore, Documento


@pytest.fixture(scope="session")
def test_path():
    return Path(os.path.dirname(__file__))


@pytest.fixture(scope="session")
def dados_path(test_path):
    return test_path / "dados"


@pytest.fixture(scope="session")
def escola_etl(dados_path):
    etl = EscolaETL(ds=DataStore("teste"), ano="ultimo")
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
    }

    return etl


@pytest.fixture(scope="session")
def gestor_etl(dados_path):
    etl = GestorETL(ds=DataStore("teste"), ano="ultimo")
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
    }

    return etl


@pytest.fixture(scope="session")
def turma_etl(dados_path):
    etl = TurmaETL(ds=DataStore("teste"), ano="ultimo")
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
    }

    return etl
