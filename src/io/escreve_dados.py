import typing
from io import BytesIO
from io import StringIO

import geopandas as gpd
import pandas as pd

from src.utils.interno import obtem_argumentos_objeto

ESCREVE_PANDAS: typing.Dict[str, typing.Callable[..., pd.DataFrame]] = {
    "csv": pd.DataFrame.to_csv,
    "tsv": pd.DataFrame.to_csv,
    "parquet": pd.DataFrame.to_parquet,
    "feather": pd.DataFrame.to_feather,
    "hdf": pd.DataFrame.to_hdf,
    "xls": pd.DataFrame.to_excel,
    "xlsx": pd.DataFrame.to_excel,
    "ods": pd.DataFrame.to_excel,
    "json": pd.DataFrame.to_json,
    "pkl": pd.DataFrame.to_pickle,
    "html": pd.DataFrame.to_html,
    "xml": pd.DataFrame.to_xml,
}

ESCREVE_GEOPANDAS: typing.Dict[str, typing.Callable[..., pd.DataFrame]] = {
    "parquet": gpd.GeoDataFrame.to_parquet,
    "feather": gpd.GeoDataFrame.to_feather,
    "shp": gpd.GeoDataFrame.to_file,
    "geojson": gpd.GeoDataFrame.to_file,
    "topojson": gpd.GeoDataFrame.to_file,
}


def escreve_para_buffer(dados: typing.Any, ext: str, **kwargs: typing.Any) -> BytesIO:
    """
    Escreve um determinado objeto de dados para um buffer

    :param dados:
    :param ext: tipo de saída a ser gerado
    :param kwargs: argumentos de escrita dos dados
    :return:
    """
    if isinstance(dados, gpd.GeoDataFrame):
        if ext == "geojson":
            kwargs["driver"] = "GeoJSON"
        elif ext == "topojson":
            kwargs["driver"] = "TopoJSON"
        ESCREVE_GEOPANDAS[ext](
            dados, saida, **obtem_argumentos_objeto(ESCREVE_GEOPANDAS[ext], kwargs)
        )
    elif isinstance(dados, pd.DataFrame):
        saida = BytesIO()
        if ext == "ods":
            kwargs["engine"] = "odf"
        ESCREVE_PANDAS[ext](
            dados, saida, **obtem_argumentos_objeto(ESCREVE_PANDAS[ext], kwargs)
        )
    elif isinstance(dados, str):
        saida = StringIO()
        pass
    else:
        pass

    return saida
