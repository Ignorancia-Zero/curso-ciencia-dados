import os
import unittest

import pandas as pd
import pytest

from src.aquisicao.inep.censo_turma import TurmaETL
from src.configs import COLECAO_DADOS_WEB
from src.io.data_store import Documento


@pytest.fixture(scope="module")
def turma_etl(ds, dados_path):
    etl = TurmaETL(ds=ds, ano="ultimo")
    etl._inep = {
        Documento(
            etl._ds,
            referencia=dict(nome=k, colecao=COLECAO_DADOS_WEB, pasta=etl._base),
        ): ""
        for k in os.listdir(dados_path / f"{COLECAO_DADOS_WEB}/censo_escolar")
    }

    return etl


@pytest.mark.run(order=1)
def test_extract(turma_etl) -> None:
    turma_etl.extract()

    assert turma_etl.dados_entrada is not None
    assert len(turma_etl.dados_entrada) == 1
    assert turma_etl.ano == 2020
    assert turma_etl.dados_entrada[0].nome == "2020.zip"
    assert isinstance(turma_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_processa_in(turma_etl) -> None:
    for base in turma_etl.dados_entrada:
        ad = {"IN_DISC_EST_SOCIAIS_SOCIOLOGIA", "IN_ESPECIAL_EXCLUSIVA"} - set(
            base.data
        )
        turma_etl.processa_in(base)
        if len(ad) > 0:
            assert ad.issubset(set(base.data))


@pytest.mark.run(order=3)
def test_processa_tp(turma_etl) -> None:
    for base in turma_etl.dados_entrada:
        ad = {"TP_MEDIACAO_DIDATICO_PEDAGO", "TP_TIPO_ATENDIMENTO_TURMA"} - set(
            base.data
        )
        turma_etl.processa_tp(base)
        if len(ad) > 0:
            assert ad.issubset(set(base.data))

    for d in turma_etl.dados_entrada:
        for c in turma_etl._configs["DEPARA_TP"]:
            if c in d.data:
                assert "category" == d.data[c].dtype


@pytest.mark.run(order=4)
def test_remove_duplicatas(turma_etl) -> None:
    base_id = turma_etl.remove_duplicatas(turma_etl.documentos_entrada[0])

    assert base_id is None


@pytest.mark.run(order=5)
def test_gera_documento_saida(turma_etl) -> None:
    turma_etl.gera_documento_saida(turma_etl.documentos_entrada[0], None)
    assert len(turma_etl.dados_saida) == 1
    assert (
        turma_etl.dados_saida[0].data.shape[0]
        == turma_etl.dados_saida[0].data["ID_TURMA"].nunique()
    )


@pytest.mark.run(order=6)
def test_ajusta_schema(turma_etl) -> None:
    turma_etl.ajusta_schema(
        base=turma_etl.documentos_saida[0],
        fill=turma_etl._configs["PREENCHER_NULOS"],
        schema=turma_etl._configs["DADOS_SCHEMA"],
    )

    for c in turma_etl._configs["PREENCHER_NULOS"]:
        assert (
            turma_etl.dados_saida[0].data.shape[0]
            == turma_etl.dados_saida[0].data[c].count()
        )

    assert set(turma_etl.dados_saida[0].data) == set(turma_etl._configs["DADOS_SCHEMA"])
    for col, dtype in turma_etl._configs["DADOS_SCHEMA"].items():
        if dtype == "str":
            assert turma_etl.dados_saida[0].data[col].dtype == "object"
        elif not dtype.startswith("pd."):
            assert turma_etl.dados_saida[0].data[col].dtype == dtype


if __name__ == "__main__":
    unittest.main()
