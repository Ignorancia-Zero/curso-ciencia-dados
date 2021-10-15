import json
import pickle
import typing
from io import StringIO

import geopandas as gpd
import pandas as pd
import yaml

from src.io.configs import ESCREVE_PANDAS, ESCREVE_GEOPANDAS, EXTENSOES_TEXTO
from src.utils.interno import obtem_argumentos_objeto


def save_json(dados: dict, buffer: typing.BinaryIO) -> None:
    """
    Realiza a escrita de dados json para um buffer

    :param dados: dados a serem salvos
    :param buffer: buffer para escrever
    """
    up = StringIO()
    json.dump(dados, up)
    up.seek(0)
    buffer.write(up.read().encode("UTF-8"))


def save_yaml(dados: dict, buffer: typing.BinaryIO) -> None:
    """
    Realiza a escrita de dados yaml para um buffer

    :param dados: dados a serem salvos
    :param buffer: buffer para escrever
    """
    yaml.dump(dados, buffer)


def save_pickle(dados: typing.Any, buffer: typing.BinaryIO) -> None:
    """
    Realiza a escrita de dados pickle para um buffer

    :param dados: dados a serem salvos
    :param buffer: buffer para escrever
    """
    pickle.dump(dados, buffer)


def escreve_para_buffer(
    dados: typing.Any,
    buffer: typing.BinaryIO,
    ext: str,
    **kwargs: typing.Any,
) -> None:
    """
    Escreve um determinado objeto de dados para um buffer

    :param dados: objeto python a ser exportado
    :param buffer: buffer que irá reter o conteúdo
    :param ext: tipo de saída a ser gerado
    :param kwargs: argumentos de escrita dos dados
    """
    if isinstance(dados, gpd.GeoDataFrame):
        assert ext in ESCREVE_GEOPANDAS, f"{ext} não foi configurado para geopandas"
        if ext == "geojson":
            kwargs["driver"] = "GeoJSON"
        elif ext == "topojson":
            kwargs["driver"] = "TopoJSON"
        elif ext == "json":
            kwargs["driver"] = (
                kwargs.get("driver") if kwargs.get("driver") else "GeoJSON"
            )
        elif ext == "shp":
            raise NotImplementedError(
                "Nós não implementamos uma maneira de exportar os dados de shp para um buffer"
            )
        ESCREVE_GEOPANDAS[ext](
            dados, buffer, **obtem_argumentos_objeto(ESCREVE_GEOPANDAS[ext], kwargs)
        )
    elif isinstance(dados, pd.DataFrame):
        assert ext in ESCREVE_PANDAS, f"{ext} não foi configurado para pandas"
        if ext == "ods":
            kwargs["engine"] = "odf"
        ESCREVE_PANDAS[ext](
            dados, buffer, **obtem_argumentos_objeto(ESCREVE_PANDAS[ext], kwargs)
        )
    else:
        if ext == "json":
            save_json(dados, buffer)
        elif ext == "yml":
            save_yaml(dados, buffer)
        elif ext == "pkl":
            save_pickle(dados, buffer)
        elif ext in EXTENSOES_TEXTO:
            buffer.write(dados.encode(kwargs.get("encoding")))
        else:
            raise NotImplementedError(
                f"Não implementamos um método de escrita para {type(dados)} no formato {ext}"
            )
