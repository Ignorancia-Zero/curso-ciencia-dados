import os
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
    assert isinstance(escola_etl.dados_entrada["2020.zip"], pd.DataFrame)


@pytest.mark.order2
def test_renomeia_colunas(escola_etl) -> None:
    renomear = {
        k: set(escola_etl._configs["RENOMEIA_COLUNAS"]).intersection(set(d.columns))
        for k, d in escola_etl.dados_entrada.items()
    }

    escola_etl.renomeia_colunas()

    for k, cols in renomear.items():
        for c in cols:
            d = escola_etl.dados_entrada[k]
            assert c not in d
            assert escola_etl._configs["RENOMEIA_COLUNAS"][c] in d


@pytest.mark.order3
def test_dropa_colunas(escola_etl) -> None:
    escola_etl.dropa_colunas()

    for d in escola_etl.dados_entrada.values():
        for c in escola_etl._configs["DROPAR_COLUNAS"]:
            assert c not in d


@pytest.mark.order4
def test_processa_dt(escola_etl) -> None:
    escola_etl.processa_dt()

    for d in escola_etl.dados_entrada.values():
        for c in d:
            if c.startswith("DT_"):
                assert d[c].dtype == "datetime64[ns]"


@pytest.mark.order5
def test_processa_qt(escola_etl) -> None:
    escola_etl.processa_qt()

    for k, d in escola_etl.dados_entrada.items():
        if k >= "2019.zip":
            for c in escola_etl._configs["COLS_88888"]:
                if c in d:
                    assert 88888 not in d[c].values


@pytest.mark.order6
def test_processa_in(escola_etl) -> None:
    cols = set([c for d in escola_etl.dados_entrada.values() for c in d])
    criar_qt = {
        k: set([c for c in d if c.startswith("QT_") and f"IN{c[2:]}" in cols])
        for k, d in escola_etl.dados_entrada.items()
    }
    criar_comp = {
        k: set(
            [
                criar
                for criar, t in escola_etl._configs["TRATAMENTO_IN"].items()
                for c in d
                if re.search(t[0], c)
            ]
        )
        for k, d in escola_etl.dados_entrada.items()
    }

    escola_etl.processa_in()

    for k, cols in criar_qt.items():
        assert cols.issubset(set(escola_etl.dados_entrada[k]))

    for k, cols in criar_comp.items():
        assert cols.issubset(set(escola_etl.dados_entrada[k]))

    for d in escola_etl.dados_entrada.values():
        if "IN_ENERGIA_INEXISTENTE" in d:
            assert "IN_ENERGIA_OUTROS" in d
        if "TP_OCUPACAO_GALPAO" in d:
            assert "IN_LOCAL_FUNC_GALPAO" in d
        if "TP_INDIGENA_LINGUA" in d:
            assert "IN_LINGUA_INDIGENA" in d
            assert "IN_LINGUA_PORTUGUESA" in d
        if "IN_BIBLIOTECA" in d:
            assert "IN_BIBLIOTECA_SALA_LEITURA" in d
        if "IN_AGUA_FILTRADA" in d:
            assert "IN_AGUA_POTAVEL" in d

        for c in d:
            if c.startswith("IN_"):
                assert {0, 1, np.nan}, set(d[c].unique())


@pytest.mark.order7
def test_processa_tp(escola_etl) -> None:
    escola_etl.processa_tp()

    for d in escola_etl.dados_entrada.values():
        if "IN_LINGUA_INDIGENA" in d and "IN_LINGUA_PORTUGUESA" in d:
            assert "TP_INDIGENA_LINGUA" in d
            assert {
                "SEM EDUCAÇÃO INDÍGENA",
                "EM LÍNGUA INDÍGENA E EM LÍNGUA PORTUGUESA",
                "SOMENTE EM LÍNGUA INDÍGENA",
                "SOMENTE EM LÍNGUA PORTUGUESA",
            }.issuperset(set(d["TP_INDIGENA_LINGUA"].dropna().astype(str)))

        if "TP_OCUPACAO_GALPAO" in d:
            assert d["TP_OCUPACAO_GALPAO"].nunique() > 2
        if "TP_OCUPACAO_PREDIO_ESCOLAR" in d:
            assert d["TP_OCUPACAO_PREDIO_ESCOLAR"].nunique() > 2

        for c in escola_etl._configs["DEPARA_TP"]:
            if c in d:
                assert "category" == d[c].dtype


@pytest.mark.order8
def test_concatena_bases(escola_etl) -> None:
    escola_etl.concatena_bases()

    assert {"escola_temp", "escola_atemp"} == set(escola_etl.dados_saida)

    assert (
        escola_etl.dados_saida["escola_atemp"].shape[0]
        == escola_etl.dados_saida["escola_atemp"]["CO_ENTIDADE"].nunique()
    )

    assert set(escola_etl._configs["COLS_ATEMPORAL"]).issubset(
        set(escola_etl.dados_saida["escola_atemp"])
    )


@pytest.mark.order9
def test_preenche_nulos(escola_etl) -> None:
    antes = escola_etl.dados_saida["escola_temp"].count()

    escola_etl.preenche_nulos()

    for c in escola_etl._configs["COLS_FBFILL"]:
        assert escola_etl.dados_saida["escola_temp"][c].count() >= antes[c]

    for c in escola_etl._configs["REMOVER_COLS"]:
        assert c not in escola_etl.dados_saida["escola_temp"]

    for c in escola_etl._configs["PREENCHER_NULOS"]:
        assert (
            escola_etl.dados_saida["escola_temp"].shape[0]
            == escola_etl.dados_saida["escola_temp"][c].count()
        )


if __name__ == "__main__":
    unittest.main()
