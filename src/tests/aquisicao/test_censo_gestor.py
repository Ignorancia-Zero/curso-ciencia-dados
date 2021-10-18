import unittest

import pandas as pd
import pytest


@pytest.mark.run(order=1)
def test_extract(gestor_etl) -> None:
    gestor_etl.extract()

    assert gestor_etl.dados_entrada is not None
    assert len(gestor_etl.dados_entrada) == 1
    assert gestor_etl.ano == 2020
    assert gestor_etl.dados_entrada[0].nome == "2020.zip"
    assert isinstance(gestor_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_renomeia_colunas(gestor_etl) -> None:
    renomear = {
        d.nome: set(gestor_etl._configs["RENOMEIA_COLUNAS"]).intersection(
            set(d.data.columns)
        )
        for d in gestor_etl.dados_entrada
    }

    for base in gestor_etl.dados_entrada:
        gestor_etl.renomeia_colunas(base)

    i = 0
    for k, cols in renomear.items():
        for c in cols:
            d = gestor_etl.dados_entrada[i]
            assert c not in d.data
            assert gestor_etl._configs["RENOMEIA_COLUNAS"][c] in d.data
        i += 1


@pytest.mark.run(order=3)
def test_gera_dt_nascimento(gestor_etl) -> None:
    for base in gestor_etl.dados_entrada:
        gestor_etl.gera_dt_nascimento(base)

    for d in gestor_etl.dados_entrada:
        assert "DT_NASCIMENTO" in d.data
        assert d.data["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=4)
def test_dropa_colunas(gestor_etl) -> None:
    for base in gestor_etl.dados_entrada:
        gestor_etl.dropa_colunas(base)

    for d in gestor_etl.dados_entrada:
        for c in gestor_etl._configs["DROPAR_COLUNAS"]:
            assert c not in d.data


@pytest.mark.run(order=5)
def test_processa_tp(gestor_etl) -> None:
    for base in gestor_etl.dados_entrada:
        gestor_etl.processa_tp(base)

    for d in gestor_etl.dados_entrada:
        for c in gestor_etl._configs["DEPARA_TP"]:
            if c in d.data:
                assert "category" == d.data[c].dtype


@pytest.mark.run(order=6)
def test_concatena_bases(gestor_etl) -> None:
    gestor_etl.concatena_bases()

    assert len(gestor_etl.dados_saida) == 1


@pytest.mark.run(order=7)
def test_ajustes_finais(gestor_etl) -> None:
    gestor_etl.ajustes_finais()

    for c in gestor_etl._configs["PREENCHER_NULOS"]:
        assert (
            gestor_etl.dados_saida[0].data.shape[0]
            == gestor_etl.dados_saida[0].data[c].count()
        )

    assert set(gestor_etl.dados_saida[0].data) == set(gestor_etl._configs["DS_SCHEMA"])

    for col, dtype in gestor_etl._configs["DS_SCHEMA"].items():
        if not dtype.startswith("pd."):
            assert gestor_etl.dados_saida[0].data[col].dtype == dtype


if __name__ == "__main__":
    unittest.main()
