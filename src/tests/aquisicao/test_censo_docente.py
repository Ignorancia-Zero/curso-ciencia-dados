import unittest

import pandas as pd
import pytest


@pytest.mark.run(order=1)
def test_extract(docente_etl) -> None:
    docente_etl.extract()

    assert docente_etl.dados_entrada is not None
    assert len(docente_etl.dados_entrada) == 1
    assert docente_etl.ano == 2020
    assert docente_etl.dados_entrada[0].nome == "2020.zip"
    assert isinstance(docente_etl.dados_entrada[0].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_renomeia_colunas(docente_etl) -> None:
    renomear = {
        d.nome: set(docente_etl._configs["RENOMEIA_COLUNAS"]).intersection(
            set(d.data.columns)
        )
        for d in docente_etl.dados_entrada
    }

    for base in docente_etl.dados_entrada:
        docente_etl.renomeia_colunas(base)

    i = 0
    for k, cols in renomear.items():
        for c in cols:
            d = docente_etl.dados_entrada[i]
            assert c not in d.data
            assert docente_etl._configs["RENOMEIA_COLUNAS"][c] in d.data
        i += 1


@pytest.mark.run(order=3)
def test_gera_dt_nascimento(docente_etl) -> None:
    for base in docente_etl.dados_entrada:
        docente_etl.gera_dt_nascimento(base)

    for d in docente_etl.dados_entrada:
        assert "DT_NASCIMENTO" in d.data
        assert d.data["DT_NASCIMENTO"].dtype == "datetime64[ns]"


@pytest.mark.run(order=4)
def test_dropa_colunas(docente_etl) -> None:
    for base in docente_etl.dados_entrada:
        docente_etl.dropa_colunas(base)

    for d in docente_etl.dados_entrada:
        for c in docente_etl._configs["DROPAR_COLUNAS"]:
            assert c not in d.data


@pytest.mark.run(order=5)
def test_processa_in(docente_etl) -> None:
    for base in docente_etl.dados_entrada:
        docente_etl.processa_in(base)

        if (
            "IN_INTERCULTURAL_OUTROS" in base.data
            and "IN_ESPECIFICO_OUTROS" not in base.data
        ):
            assert "IN_ESPECIFICO_OUTROS" in base.data
            assert "IN_INTERCULTURAL_OUTROS" not in base.data


@pytest.mark.run(order=6)
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


@pytest.mark.run(order=7)
def test_remove_duplicatas_gera_documento_saida(docente_etl) -> None:
    base_id = docente_etl.remove_duplicatas(docente_etl.documentos_entrada[0])

    assert base_id is not None

    docente_etl.gera_documento_saida(docente_etl.documentos_entrada[0], base_id)
    assert len(docente_etl.dados_saida) == 2
    assert (
        docente_etl.dados_saida[0].data.shape[0]
        == docente_etl.dados_saida[0].data["ID_DOCENTE"].nunique()
    )


@pytest.mark.run(order=8)
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
