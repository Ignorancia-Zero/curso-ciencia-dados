import os
import unittest

import pandas as pd
import pytest

from src.aquisicao.inep.censo_docente import DocenteETL
from src.configs import COLECAO_DADOS_WEB
from src.io.data_store import Documento


@pytest.fixture(scope="module")
def docente_etl(ds, dados_path):
    etl = DocenteETL(ds=ds, ano="ultimo")
    etl._inep = {
        Documento(
            etl._ds,
            referencia=dict(nome=k, colecao=COLECAO_DADOS_WEB, pasta=etl._base),
        ): ""
        for k in os.listdir(dados_path / f"{COLECAO_DADOS_WEB}/censo_escolar")
    }

    return etl


@pytest.mark.run(order=1)
def test_extract(docente_etl) -> None:
    docente_etl.extract()

    assert docente_etl.dados_entrada is not None
    assert len(docente_etl.dados_entrada) == 1
    assert docente_etl.ano == 2020
    assert docente_etl.dados_entrada[0].nome == "2020.zip"
    assert isinstance(docente_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_gera_dt_nascimento(docente_etl) -> None:
    for base in docente_etl.dados_entrada:
        docente_etl.gera_dt_nascimento(base)

    for d in docente_etl.dados_entrada:
        assert "DT_NASCIMENTO" in d.data
        assert d.data["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=3)
def test_processa_in(docente_etl) -> None:
    for base in docente_etl.dados_entrada:
        docente_etl.processa_in(base)

        if (
            "IN_INTERCULTURAL_OUTROS" in base.data
            and "IN_ESPECIFICO_OUTROS" not in base.data
        ):
            assert "IN_ESPECIFICO_OUTROS" in base.data
            assert "IN_INTERCULTURAL_OUTROS" not in base.data


@pytest.mark.run(order=4)
def test_processa_tp(docente_etl) -> None:
    for base in docente_etl.dados_entrada:
        docente_etl.processa_tp(base)

        if "TP_ESCOLARIDADE" not in base.data and "TP_ESCOLARIDADE_0" in base.data:
            assert "TP_ESCOLARIDADE" in base.data
        if "TP_ENSINO_MEDIO" not in base.data and "TP_ESCOLARIDADE_0" in base.data:
            assert "TP_ENSINO_MEDIO" in base.data

    for d in docente_etl.dados_entrada:
        for c in docente_etl._configs["DEPARA_TP"]:
            if c in d.data:
                assert "category" == d.data[c].dtype


@pytest.mark.run(order=5)
def test_remove_duplicatas_gera_documento_saida(docente_etl) -> None:
    base_id = docente_etl.remove_duplicatas(docente_etl.documentos_entrada[0])

    assert base_id is not None

    docente_etl.gera_documento_saida(docente_etl.documentos_entrada[0], base_id)
    assert len(docente_etl.dados_saida) == 2
    assert (
        docente_etl.dados_saida[0].data.shape[0]
        == docente_etl.dados_saida[0].data["ID_DOCENTE"].nunique()
    )


@pytest.mark.run(order=6)
def test_ajusta_schema(docente_etl) -> None:
    docente_etl.ajusta_schema(
        base=docente_etl.documentos_saida[0],
        fill=docente_etl._configs["PREENCHER_NULOS"],
        schema=docente_etl._configs["DADOS_SCHEMA"],
    )
    for c in docente_etl._configs["PREENCHER_NULOS"]:
        if c in docente_etl._configs["DADOS_SCHEMA"]:
            assert (
                docente_etl.dados_saida[0].data.shape[0]
                == docente_etl.dados_saida[0].data[c].count()
            )
    assert set(docente_etl.dados_saida[0].data) == set(
        docente_etl._configs["DADOS_SCHEMA"]
    )
    for col, dtype in docente_etl._configs["DADOS_SCHEMA"].items():
        if not dtype.startswith("pd."):
            if dtype == "str":
                assert docente_etl.dados_saida[0].data[col].dtype == "object"
            else:
                assert docente_etl.dados_saida[0].data[col].dtype == dtype

    docente_etl.ajusta_schema(
        base=docente_etl.documentos_saida[1],
        fill=docente_etl._configs["PREENCHER_NULOS"],
        schema=docente_etl._configs["DEPARA_SCHEMA"],
    )
    for c in docente_etl._configs["PREENCHER_NULOS"]:
        if c in docente_etl._configs["DEPARA_SCHEMA"]:
            assert (
                docente_etl.dados_saida[1].data.shape[0]
                == docente_etl.dados_saida[1].data[c].count()
            )
    assert set(docente_etl.dados_saida[1].data) == set(
        docente_etl._configs["DEPARA_SCHEMA"]
    )
    for col, dtype in docente_etl._configs["DEPARA_SCHEMA"].items():
        if not dtype.startswith("pd."):
            assert docente_etl.dados_saida[1].data[col].dtype == dtype


if __name__ == "__main__":
    unittest.main()
