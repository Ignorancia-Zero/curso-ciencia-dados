import os
import unittest

import pandas as pd
import pytest

from src.aquisicao.inep.ideb import IDEBETL
from src.configs import COLECAO_DADOS_WEB
from src.io.data_store import Documento


@pytest.fixture(scope="module")
def ideb_etl(ds, dados_path):
    etl = IDEBETL(ds=ds)
    etl._links = {
        Documento(
            etl._ds,
            referencia=dict(nome=k, colecao=COLECAO_DADOS_WEB, pasta="ideb"),
        ): ""
        for k in os.listdir(dados_path / f"{COLECAO_DADOS_WEB}/ideb")
    }

    return etl


@pytest.fixture(scope="module")
def data() -> dict:
    return dict()


@pytest.mark.run(order=1)
def test_extract(ideb_etl) -> None:
    ideb_etl.extract()

    assert ideb_etl.dados_entrada is not None
    assert len(ideb_etl.dados_entrada) == 3
    assert ideb_etl.dados_entrada[0].nome == "divulgacao_anos_finais_escolas_2019.zip"
    assert ideb_etl.dados_entrada[1].nome == "divulgacao_anos_iniciais_escolas_2019.zip"
    assert ideb_etl.dados_entrada[2].nome == "divulgacao_ensino_medio_escolas_2019.zip"
    assert isinstance(ideb_etl.dados_entrada[0].data, pd.DataFrame)
    assert isinstance(ideb_etl.dados_entrada[1].data, pd.DataFrame)
    assert isinstance(ideb_etl.dados_entrada[2].data, pd.DataFrame)


@pytest.mark.run(order=2)
def test_extrai_turma(ideb_etl) -> None:
    assert ideb_etl.extrai_turma(ideb_etl.dados_entrada[0]) == "AF"
    assert ideb_etl.extrai_turma(ideb_etl.dados_entrada[1]) == "AI"
    assert ideb_etl.extrai_turma(ideb_etl.dados_entrada[2]) == "EM"


@pytest.mark.run(order=3)
def test_seleciona_dados(ideb_etl, data) -> None:
    data["df"] = ideb_etl.seleciona_dados(ideb_etl.dados_entrada[0])
    assert (
        len(
            {"SG_UF", "CO_MUNICIPIO", "NO_MUNICIPIO", "NO_ESCOLA", "REDE"}.intersection(
                set(data["df"].columns)
            )
        )
        == 0
    )
    assert data["df"].shape[0] == ideb_etl.dados_entrada[0].data.shape[0] - 3


@pytest.mark.run(order=4)
def test_obtem_metricas(ideb_etl, data) -> None:
    data["dados"] = ideb_etl.obtem_metricas(data["df"], "AF")

    assert set(data["df"].iloc[:, 1:].columns) == set(data["dados"]["COLUNA"])
    assert set(range(2005, 2022, 2)) == set(data["dados"]["ANO"])
    assert {
        "APROVACAO_AF",
        "APROVACAO_AF_1",
        "APROVACAO_AF_2",
        "APROVACAO_AF_3",
        "APROVACAO_AF_4",
        "IDEB_AF",
        "IDEB_META_AF",
        "NOTA_MATEMATICA_AF",
        "NOTA_MEDIA_AF",
        "NOTA_PORTUGUES_AF",
        "REND_AF",
    } == set(data["dados"]["METRICA"])


@pytest.mark.run(order=5)
def test_formata_resultados(ideb_etl, data) -> None:
    data["df"] = ideb_etl.formata_resultados(data["df"], data["dados"])

    assert set(["ID_ESCOLA", "ANO"] + list(data["dados"]["METRICA"].unique())) == set(
        data["df"].columns
    )
    assert all(
        [
            "float32" == data["df"][c].dtype
            for c in data["df"]
            if c != "ID_ESCOLA" and c != "ANO"
        ]
    )


@pytest.mark.run(order=6)
def test_concatena_saidas(ideb_etl, data) -> None:
    df2 = ideb_etl.seleciona_dados(ideb_etl.dados_entrada[1])
    dados = ideb_etl.obtem_metricas(df2, "AI")
    df2 = ideb_etl.formata_resultados(df2, dados)

    df3 = ideb_etl.seleciona_dados(ideb_etl.dados_entrada[2])
    dados = ideb_etl.obtem_metricas(df3, "EM")
    df3 = ideb_etl.formata_resultados(df3, dados)

    res = ideb_etl.concatena_saidas([data["df"], df2, df3])

    assert {
        "ID_ESCOLA",
        "ANO",
        "APROVACAO_AI",
        "APROVACAO_AI_1",
        "APROVACAO_AI_2",
        "APROVACAO_AI_3",
        "APROVACAO_AI_4",
        "IDEB_AI",
        "IDEB_META_AI",
        "NOTA_MATEMATICA_AI",
        "NOTA_MEDIA_AI",
        "NOTA_PORTUGUES_AI",
        "REND_AI",
        "APROVACAO_AF",
        "APROVACAO_AF_1",
        "APROVACAO_AF_2",
        "APROVACAO_AF_3",
        "APROVACAO_AF_4",
        "IDEB_AF",
        "IDEB_META_AF",
        "NOTA_MATEMATICA_AF",
        "NOTA_MEDIA_AF",
        "NOTA_PORTUGUES_AF",
        "REND_AF",
        "APROVACAO_EM",
        "APROVACAO_EM_1",
        "APROVACAO_EM_2",
        "APROVACAO_EM_3",
        "APROVACAO_EM_4",
        "IDEB_EM",
        "IDEB_META_EM",
        "NOTA_MATEMATICA_EM",
        "NOTA_MEDIA_EM",
        "NOTA_PORTUGUES_EM",
        "REND_EM",
    } == set(res.columns)


if __name__ == "__main__":
    unittest.main()
