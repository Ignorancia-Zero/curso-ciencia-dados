import typing

import pandas as pd
import pytest

import src.datamart.escola as dm_escola
from src.io.data_store import Documento, DataStore, CatalogoAquisicao


@pytest.fixture(scope="module")
def dados():
    return dict(dm=pd.DataFrame())


def test_processa_censo_escola(
    dados: typing.Dict[str, pd.DataFrame], ds: DataStore, ano: int
):
    dados["dm"] = dm_escola.processa_censo_escola(ds, ano)

    assert set(dados["dm"]["ANO"]) == {ano}
    assert 0 < dados["dm"].shape[0] <= 1000


def test_processa_turmas(
    dados: typing.Dict[str, pd.DataFrame], ds: DataStore, ano: int
):
    turma = ds.carrega_como_objeto(
        Documento(ds, dict(CatalogoAquisicao.TURMA)),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    dados["dm"] = dm_escola.processa_turmas(dados["dm"], ds, ano)

    assert "QT_TURMAS" in dados["dm"].columns
    assert "PC_TURMA_ESPECIAL_EXCLUSIVA" in dados["dm"].columns

    in_cols = [f"IN_TURMA_{c[3:]}" for c in turma if c.startswith("IN_")]
    in_cols += [c.replace("IN_TURMA_", "QT_TURMA_") for c in in_cols]
    assert set(in_cols).issubset(set(dados["dm"].columns))

    for (tp_col, pf) in [
        ("TP_MEDIACAO_DIDATICO_PEDAGO", "TURMA_MEDIACAO"),
        ("TP_TIPO_ATENDIMENTO_TURMA", "TURMA_ATEND"),
        ("TP_TIPO_LOCAL_TURMA", "TURMA_LOCAL"),
        ("TP_MOD_ENSINO", "TURMA_MOD"),
        ("TP_TIPO_TURMA", "TURMA_TIPO"),
    ]:
        assert tp_col in dados["dm"].columns
        for cat in turma[tp_col].dtype.categories:
            cat = cat.replace(" ", "_").replace("-", "_").replace("___", "_")
            assert f"QT_{pf}_{cat}" in dados["dm"].columns
            assert f"PC_{pf}_{cat}" in dados["dm"].columns
        assert f"QT_{pf}_NULO" in dados["dm"].columns

    assert "PC_TURMA_ESPECIAL_EXCLUSIVA" in dados["dm"].columns
    assert "NU_TURMA_MEAN_DURACAO" in dados["dm"].columns
    assert "NU_TURMA_MEDIAN_DURACAO" in dados["dm"].columns
    assert "QT_TURMA_ATIVIDADE_COMP" in dados["dm"].columns
    for tipo in [
        "REGULAR",
        "INFANTIL",
        "FUNDAMENTAL",
        "AI",
        "AF",
        "MEDIO",
        "EJA",
        "PROFISSIONALIZANTE",
        "TECNICO",
        "FIC",
    ]:
        assert f"QT_TURMA_{tipo}" in dados["dm"].columns
        assert f"IN_TURMA_{tipo}" in dados["dm"].columns


def test_processa_docentes(
    dados: typing.Dict[str, pd.DataFrame], ds: DataStore, ano: int
):
    dados["dm"] = dm_escola.processa_docentes(dados["dm"], ds, ano)


def test_processa_gestor(
    dados: typing.Dict[str, pd.DataFrame], ds: DataStore, ano: int
):
    dados["dm"] = dm_escola.processa_gestor(dados["dm"], ds, ano)


def test_processa_matricula(
    dados: typing.Dict[str, pd.DataFrame], ds: DataStore, ano: int
):
    dados["dm"] = dm_escola.processa_matricula(dados["dm"], ds, ano)


def test_processa_ideb(dados: typing.Dict[str, pd.DataFrame], ds: DataStore, ano: int):
    dados["dm"] = dm_escola.processa_ideb(dados["dm"], ds, ano)


def test_gera_metricas_adicionais(dados: typing.Dict[str, pd.DataFrame]):
    dados["dm"] = dm_escola.gera_metricas_adicionais(dados["dm"])
