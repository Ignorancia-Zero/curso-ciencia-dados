import os
import unittest

import pandas as pd
import pytest

from src.aquisicao.inep.censo_matricula import MatriculaETL
from src.aquisicao.inep.censo_matricula import _MatriculaRegiaoETL
from src.configs import COLECAO_DADOS_WEB
from src.io.data_store import Documento


@pytest.fixture(scope="module")
def matricula_reg_etl(ds, dados_path):
    etl = _MatriculaRegiaoETL(ds=ds, regiao="CO", ano="ultimo")
    etl._inep = {
        Documento(
            etl._ds,
            referencia=dict(nome=k, colecao=COLECAO_DADOS_WEB, pasta=etl._base),
        ): ""
        for k in os.listdir(dados_path / f"{COLECAO_DADOS_WEB}/censo_escolar")
    }

    return etl


@pytest.fixture(scope="module")
def matricula_etl(ds, dados_path):
    etl = MatriculaETL(ds=ds, ano="ultimo")
    etl._inep = {
        Documento(
            etl._ds,
            referencia=dict(nome=k, colecao=COLECAO_DADOS_WEB, pasta=etl._base),
        ): ""
        for k in os.listdir(dados_path / f"{COLECAO_DADOS_WEB}/censo_escolar")
    }

    return etl


@pytest.mark.run(order=1)
def test_extract_reg(matricula_reg_etl) -> None:
    matricula_reg_etl.extract()

    assert matricula_reg_etl.dados_entrada is not None
    assert len(matricula_reg_etl.dados_entrada) == 1
    assert matricula_reg_etl.ano == 2020
    assert matricula_reg_etl.dados_entrada[0].nome == "2020.zip"
    assert isinstance(matricula_reg_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_gera_dt_nascimento(matricula_reg_etl) -> None:
    for base in matricula_reg_etl.dados_entrada:
        matricula_reg_etl.gera_dt_nascimento(base)

    for d in matricula_reg_etl.dados_entrada:
        assert "DT_NASCIMENTO" in d.data
        assert d.data["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=3)
def test_processa_tp(matricula_reg_etl) -> None:
    for base in matricula_reg_etl.dados_entrada:
        matricula_reg_etl.processa_tp(base)

    for d in matricula_reg_etl.dados_entrada:
        for c in matricula_reg_etl._configs["DEPARA_TP"]:
            if c in d.data:
                assert "category" == d.data[c].dtype


@pytest.mark.run(order=4)
def test_remove_duplicatas_gera_documento_saida(matricula_reg_etl) -> None:
    base_id = matricula_reg_etl.remove_duplicatas(
        matricula_reg_etl.documentos_entrada[0]
    )

    assert base_id is not None

    matricula_reg_etl.gera_documento_saida(
        matricula_reg_etl.documentos_entrada[0], base_id
    )
    assert len(matricula_reg_etl.dados_saida) == 2
    assert (
        matricula_reg_etl.dados_saida[0].data.shape[0]
        == matricula_reg_etl.dados_saida[0].data["ID_ALUNO"].nunique()
    )


@pytest.mark.run(order=5)
def test_ajusta_schema(matricula_reg_etl) -> None:
    matricula_reg_etl.ajusta_schema(
        base=matricula_reg_etl.documentos_saida[0],
        fill=matricula_reg_etl._configs["PREENCHER_NULOS"],
        schema=matricula_reg_etl._configs["DADOS_SCHEMA"],
    )
    for c in matricula_reg_etl._configs["PREENCHER_NULOS"]:
        if c in matricula_reg_etl._configs["DADOS_SCHEMA"]:
            assert (
                matricula_reg_etl.dados_saida[0].data.shape[0]
                == matricula_reg_etl.dados_saida[0].data[c].count()
            )
    assert set(matricula_reg_etl.dados_saida[0].data) == set(
        matricula_reg_etl._configs["DADOS_SCHEMA"]
    )
    for col, dtype in matricula_reg_etl._configs["DADOS_SCHEMA"].items():
        if dtype == "str":
            assert matricula_reg_etl.dados_saida[0].data[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert matricula_reg_etl.dados_saida[0].data[col].dtype == dtype

    matricula_reg_etl.ajusta_schema(
        base=matricula_reg_etl.documentos_saida[1],
        fill=matricula_reg_etl._configs["PREENCHER_NULOS"],
        schema=matricula_reg_etl._configs["DEPARA_SCHEMA"],
    )
    for c in matricula_reg_etl._configs["PREENCHER_NULOS"]:
        if c in matricula_reg_etl._configs["DEPARA_SCHEMA"]:
            assert (
                matricula_reg_etl.dados_saida[1].data.shape[0]
                == matricula_reg_etl.dados_saida[1].data[c].count()
            )
    assert set(matricula_reg_etl.dados_saida[1].data) == set(
        matricula_reg_etl._configs["DEPARA_SCHEMA"]
    )
    for col, dtype in matricula_reg_etl._configs["DEPARA_SCHEMA"].items():
        if dtype == "str":
            assert matricula_reg_etl.dados_saida[1].data[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert matricula_reg_etl.dados_saida[1].data[col].dtype == dtype


@pytest.mark.run(order=6)
def test_extract(matricula_etl) -> None:
    matricula_etl.extract()

    assert matricula_etl.dados_entrada is not None
    assert len(matricula_etl.dados_entrada) == 0
    assert matricula_etl.ano == 2020


@pytest.mark.run(order=7)
def test_transform(matricula_etl) -> None:
    matricula_etl.transform()

    for etl in matricula_etl._etls:
        assert len(etl.dados_saida) == 2
        assert isinstance(etl.dados_saida[0].data, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
