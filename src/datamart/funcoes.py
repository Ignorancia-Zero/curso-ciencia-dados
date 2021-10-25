import typing

import numpy as np
import pandas as pd


def processa_coluna_in(
    df: pd.DataFrame, id_col: str, val_col: str, prefixo: str, perc: bool = True
) -> pd.DataFrame:
    """
    Toma todas as colunas IN_ de um determinado data frame e as
    agrega ao nível de {id_col} somando seus valores.

    Depois as transforma para colunas do tipo QT_ e recria colunas
    IN_ com base no valor das colunas QT_

    :param df: data frame com dados a serem agregados
    :param id_col: coluna com nível de granularidade de agrega
    :param val_col: coluna usada para calcular os valor
    :param prefixo: prefixo a ser aplicado no nome da coluna final
    :param perc: flag para criar colunas percentuais
    :return: data frame com dados QT_ e IN_
    """
    # calcula a soma dos elementos totais
    res = df.groupby([id_col]).agg({val_col: "count"}).reset_index()
    res.columns = [id_col, "TOTAL"]

    # gera as colunas de agregação simples
    agg = {c: "sum" for c in df if c.startswith("IN_")}
    res = res.merge(df.groupby([id_col]).agg(agg).reset_index())

    # renomeia as colunas IN_ para quantidade
    rename = {c: c.replace("IN_", f"QT_{prefixo}_") for c in agg}
    res.rename(columns=rename, inplace=True)

    # cria as flags equivalentes
    for c1, c2 in rename.items():
        tc = c2.replace(f"QT_{prefixo}_", f"IN_{prefixo}_")
        if df[c1].count() == 0:
            res[c2] = np.nan
            res[tc] = np.nan
        else:
            res[tc] = (res[c2] > 0).astype("float16")

    # calcula os campos percentuais
    if perc:
        for c in rename.values():
            res[f"PC_{c[3:]}"] = res[c] / res["TOTAL"]

    return res.drop(columns=["TOTAL"])


def processa_coluna_tp(
    df: pd.DataFrame, id_col: str, tp_col: str, val_col: str, prefixo: str, recriar: bool = True
) -> pd.DataFrame:
    """
    Processa uma coluna {tp_col} de forma a obter colunas com a quantidade
    de {val_col} por {id_col} para cada categoria e gerar uma nova coluna
    {tp_col} considerando os valores reportados em {tp_col} para cada grupo

    :param df: data frame com dados a serem agregados
    :param id_col: coluna com nível de granularidade de agrega
    :param tp_col: coluna TP a ser processada
    :param val_col: coluna usada para calcular os valor
    :param prefixo: prefixo a ser aplicado no nome da coluna final
    :param recriar: flag se devemos recriar as colunas TP a partir das pivôs
    :return: data frame com coluna {tp_col} processada por {id_col}
    """
    # se esse for o caso faz a pivô da coluna
    tp = df.pivot_table(index=id_col, columns=tp_col, values=val_col, aggfunc="nunique")
    if df[tp_col].count() == 0:
        tp.replace({0: np.nan}, inplace=True)

    # renomeia as coluna de acordo com o sufixo
    replace = f"QT_{prefixo}_" + (
        tp.columns.str.replace(" ", "_").str.replace("-", "_").str.replace("___", "_")
    )
    col_dict = dict(zip(list(tp.columns), replace))
    tp.columns = replace

    # realiza a criação de uma coluna tp equivalente com base
    # nos resultados das colunas
    if recriar:
        ser = np.array([np.nan] * tp.shape[0])
        for val, c in col_dict.items():
            ser = np.where((tp[c] >= 1) & (tp[c] == tp.sum(axis=1)), val, ser)
        ser = np.where((tp.sum(axis=1) > 0) & (ser == "nan"), "MÚLTIPLOS", ser)
        ser = pd.Series(data=ser, name=tp_col, index=tp.index)
        ser = ser.replace({"nan": None}).astype("category")
        tp[tp_col] = ser

    # retorna os dados gerados
    return tp.reset_index()


def processa_coluna_qt_nu(
    df: pd.DataFrame,
    id_col: str,
    prefixo: str,
    metricas: typing.Sequence[str] = ("sum", "mean"),
) -> pd.DataFrame:
    """
    Processa as colunas númericas aplicando funções de agregação
    matemática conforme a lista {metricas}

    :param df: data frame com dados a serem agregados
    :param id_col: coluna com nível de granularidade de agrega
    :param prefixo: prefixo a ser aplicado no nome da coluna final
    :param metricas: lista de métricas de agregação a serem aplicadas
    :return: base de dados agregada
    """
    # obtém uma lista de funções de agregação a serem aplicadas
    # sobre as colunas
    funcs = {
        "count": "count",
        "sum": "sum",
        "min": "min",
        "q1": lambda x: x.quantile(0.25),
        "median": "median",
        "mean": "mean",
        "q3": lambda x: x.quantile(0.75),
        "max": "max",
        "std": "std"
    }

    # realiza a agregação dos dados
    agg = {
        f"{c[:2]}_{m.upper()}_{prefixo}_{c[3:]}": pd.NamedAgg(c, funcs[m])
        for c in df
        if c.startswith("QT_") or c.startswith("NU_")
        for m in metricas
    }

    return df.groupby([id_col]).agg(**agg).reset_index()
