import re
import unittest

import numpy as np
import pandas as pd
import pytest


@pytest.mark.run(order=1)
def test_extract(escola_etl) -> None:
    escola_etl.extract()

    assert escola_etl.dados_entrada is not None
    assert len(escola_etl.dados_entrada) == 1
    assert escola_etl.ano == 2020
    assert escola_etl.dados_entrada[0].nome == "2020.zip"
    assert isinstance(escola_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_renomeia_colunas(escola_etl) -> None:
    renomear = {
        d.nome: set(escola_etl._configs["RENOMEIA_COLUNAS"]).intersection(
            set(d.data.columns)
        )
        for d in escola_etl.dados_entrada
    }

    for base in escola_etl.dados_entrada:
        escola_etl.renomeia_colunas(base)

    i = 0
    for k, cols in renomear.items():
        for c in cols:
            d = escola_etl.dados_entrada[i]
            assert c not in d.data
            assert escola_etl._configs["RENOMEIA_COLUNAS"][c] in d.data
        i += 1


@pytest.mark.run(order=3)
def test_dropa_colunas(escola_etl) -> None:
    for base in escola_etl.dados_entrada:
        escola_etl.dropa_colunas(base)

    for d in escola_etl.dados_entrada:
        for c in escola_etl._configs["DROPAR_COLUNAS"]:
            assert c not in d.data


@pytest.mark.run(order=4)
def test_processa_dt(escola_etl) -> None:
    for base in escola_etl.dados_entrada:
        escola_etl.processa_dt(base)

    for d in escola_etl.dados_entrada:
        for c in d.data:
            if c.startswith("DT_"):
                assert d.data[c].dtype == "datetime64[ns]"


@pytest.mark.run(order=5)
def test_processa_qt(escola_etl) -> None:
    for base in escola_etl.dados_entrada:
        escola_etl.processa_qt(base)

    for d in escola_etl.dados_entrada:
        if d.nome >= "2019.zip":
            for c in escola_etl._configs["COLS_88888"]:
                if c in d.data:
                    assert 88888 not in d.data[c].values


@pytest.mark.run(order=6)
def test_processa_in(escola_etl) -> None:
    cols = set([c for d in escola_etl.dados_entrada for c in d.data])
    criar_qt = [
        set([c for c in d.data if c.startswith("QT_") and f"IN{c[2:]}" in cols])
        for d in escola_etl.dados_entrada
    ]
    criar_comp = [
        set(
            [
                criar
                for criar, t in escola_etl._configs["TRATAMENTO_IN"].items()
                for c in d.data
                if re.search(t[0], c)
            ]
        )
        for d in escola_etl.dados_entrada
    ]

    for base in escola_etl.dados_entrada:
        escola_etl.processa_in(base)

    for k, cols in enumerate(criar_qt):
        assert cols.issubset(set(escola_etl.dados_entrada[k].data))

    for k, cols in enumerate(criar_comp):
        assert cols.issubset(set(escola_etl.dados_entrada[k].data))

    for d in escola_etl.dados_entrada:
        if "IN_ENERGIA_INEXISTENTE" in d.data:
            assert "IN_ENERGIA_OUTROS" in d.data
        if "TP_OCUPACAO_GALPAO" in d.data:
            assert "IN_LOCAL_FUNC_GALPAO" in d.data
        if "TP_INDIGENA_LINGUA" in d.data:
            assert "IN_LINGUA_INDIGENA" in d.data
            assert "IN_LINGUA_PORTUGUESA" in d.data
        if "IN_BIBLIOTECA" in d.data:
            assert "IN_BIBLIOTECA_SALA_LEITURA" in d.data
        if "IN_AGUA_FILTRADA" in d.data:
            assert "IN_AGUA_POTAVEL" in d.data

        for c in d.data:
            if c.startswith("IN_"):
                assert {0, 1, np.nan}, set(d.data[c].unique())


@pytest.mark.run(order=7)
def test_processa_tp(escola_etl) -> None:
    for base in escola_etl.dados_entrada:
        escola_etl.processa_tp(base)

    for d in escola_etl.dados_entrada:
        if "IN_LINGUA_INDIGENA" in d.data and "IN_LINGUA_PORTUGUESA" in d.data:
            assert "TP_INDIGENA_LINGUA" in d.data
            assert {
                "SEM EDUCAÇÃO INDÍGENA",
                "EM LÍNGUA INDÍGENA E EM LÍNGUA PORTUGUESA",
                "SOMENTE EM LÍNGUA INDÍGENA",
                "SOMENTE EM LÍNGUA PORTUGUESA",
            }.issuperset(set(d.data["TP_INDIGENA_LINGUA"].dropna().astype(str)))

        if "TP_OCUPACAO_GALPAO" in d.data:
            assert d.data["TP_OCUPACAO_GALPAO"].nunique() > 2
        if "TP_OCUPACAO_PREDIO_ESCOLAR" in d.data:
            assert d.data["TP_OCUPACAO_PREDIO_ESCOLAR"].nunique() > 2

        for c in escola_etl._configs["DEPARA_TP"]:
            if c in d.data:
                assert "category" == d.data[c].dtype


@pytest.mark.run(order=8)
def test_concatena_bases(escola_etl) -> None:
    escola_etl.concatena_bases()

    assert len(escola_etl.dados_saida) == 1
    assert (
        escola_etl.dados_saida[0].data.shape[0]
        == escola_etl.dados_saida[0].data["CO_ENTIDADE"].nunique()
    )


@pytest.mark.run(order=9)
def test_ajustes_finais(escola_etl) -> None:
    antes = escola_etl.dados_saida[0].data.count()

    escola_etl.ajustes_finais()

    for c in escola_etl._configs["COLS_FBFILL"]:
        if c in antes:
            assert escola_etl.dados_saida[0].data[c].count() >= antes[c]

    for c in escola_etl._configs["REMOVER_COLS"]:
        assert c not in escola_etl.dados_saida[0].data

    for c in escola_etl._configs["PREENCHER_NULOS"]:
        assert (
            escola_etl.dados_saida[0].data.shape[0]
            == escola_etl.dados_saida[0].data[c].count()
        )

    assert set(escola_etl.dados_saida[0].data) == set(escola_etl._configs["DS_SCHEMA"])


if __name__ == "__main__":
    unittest.main()
