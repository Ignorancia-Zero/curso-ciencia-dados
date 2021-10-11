import re
import unittest

import numpy as np
import pandas as pd
import pytest


@pytest.mark.order1
def test_extract(dados_path, escola_etl) -> None:
    escola_etl.extract()

    assert escola_etl.dados_entrada is not None
    assert set(escola_etl._inep) == set(escola_etl.dados_entrada)
    assert isinstance(escola_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.order2
def test_renomeia_colunas(escola_etl) -> None:
    renomear = {
        d.nome: set(escola_etl._configs["RENOMEIA_COLUNAS"]).intersection(
            set(d.data.columns)
        )
        for d in escola_etl.dados_entrada
    }

    escola_etl.renomeia_colunas()

    i = 0
    for k, cols in renomear.items():
        for c in cols:
            d = escola_etl.dados_entrada[i]
            assert c not in d.data
            assert escola_etl._configs["RENOMEIA_COLUNAS"][c] in d.data
        i += 1


@pytest.mark.order3
def test_dropa_colunas(escola_etl) -> None:
    escola_etl.dropa_colunas()

    for d in escola_etl.dados_entrada:
        for c in escola_etl._configs["DROPAR_COLUNAS"]:
            assert c not in d.data


@pytest.mark.order4
def test_processa_dt(escola_etl) -> None:
    escola_etl.processa_dt()

    for d in escola_etl.dados_entrada:
        for c in d.data:
            if c.startswith("DT_"):
                assert d.data[c].dtype == "datetime64[ns]"


@pytest.mark.order5
def test_processa_qt(escola_etl) -> None:
    escola_etl.processa_qt()

    for d in escola_etl.dados_entrada:
        if d.nome >= "2019.zip":
            for c in escola_etl._configs["COLS_88888"]:
                if c in d.data:
                    assert 88888 not in d.data[c].values


@pytest.mark.order6
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

    escola_etl.processa_in()

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


@pytest.mark.order7
def test_processa_tp(escola_etl) -> None:
    escola_etl.processa_tp()

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


@pytest.mark.order8
def test_concatena_bases(escola_etl) -> None:
    escola_etl.concatena_bases()

    assert len(escola_etl.dados_saida) == 2

    assert (
        escola_etl.dados_saida[1].data.shape[0]
        == escola_etl.dados_saida[1].data["CO_ENTIDADE"].nunique()
    )

    assert set(escola_etl._configs["COLS_ATEMPORAL"]).issubset(
        set(escola_etl.dados_saida[1].data)
    )


@pytest.mark.order9
def test_preenche_nulos(escola_etl) -> None:
    antes = escola_etl.dados_saida[0].data.count()

    escola_etl.preenche_nulos()

    for c in escola_etl._configs["COLS_FBFILL"]:
        assert escola_etl.dados_saida[0].data[c].count() >= antes[c]

    for c in escola_etl._configs["REMOVER_COLS"]:
        assert c not in escola_etl.dados_saida[0].data

    for c in escola_etl._configs["PREENCHER_NULOS"]:
        assert (
            escola_etl.dados_saida[0].data.shape[0]
            == escola_etl.dados_saida[0].data[c].count()
        )


if __name__ == "__main__":
    unittest.main()
