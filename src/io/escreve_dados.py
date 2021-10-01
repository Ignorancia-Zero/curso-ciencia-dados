import json
import pickle
import typing

import geopandas as gpd
import pandas as pd
import yaml

from src.io.configs import ESCREVE_PANDAS, ESCREVE_GEOPANDAS, EXTENSOES_TEXTO
from src.utils.interno import obtem_argumentos_objeto


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
    elif isinstance(dados, str):
        if "encoding" not in kwargs:
            kwargs["encoding"] = "UTF-8"
        buffer.write(dados.encode(kwargs["encoding"]))
    else:
        if ext == "json":
            json.dump(dados, buffer)
        elif ext == "yml":
            yaml.dump(dados, buffer)
        elif ext == "pkl":
            pickle.dump(dados, buffer)
        elif ext in EXTENSOES_TEXTO:
            if "encoding" in kwargs:
                buffer.write(dados.encode(kwargs["encoding"]))
            else:
                buffer.write(dados.encode("UTF-8"))
        else:
            raise NotImplementedError(
                f"Não implementamos um método de escrita para {type(dados)} no formato {ext}"
            )
