import typing

import geopandas as gpd
import pandas as pd

EXTENSOES_TEXTO = ["txt", "html", "xml"]

ESCREVE_PANDAS: typing.Dict[str, typing.Callable[[pd.DataFrame, ...], None]] = {
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

ESCREVE_GEOPANDAS: typing.Dict[str, typing.Callable[[gpd.GeoDataFrame, ...], None]] = {
    "parquet": gpd.GeoDataFrame.to_parquet,
    "feather": gpd.GeoDataFrame.to_feather,
    "shp": gpd.GeoDataFrame.to_file,
    "json": gpd.GeoDataFrame.to_file,
    "geojson": gpd.GeoDataFrame.to_file,
    "topojson": gpd.GeoDataFrame.to_file,
}

# lista de extensões que devem ser nativamente interpretadas como data frames
EXTENSAO_DF = {"csv", "tsv", "parquet", "hdf", "xls", "xlsx", "ods", "feather"}

# dicionário de extensões e funções do pandas para ler conteúdos
LEITOR_PANDAS: typing.Dict[str, typing.Callable[..., pd.DataFrame]] = {
    "csv": pd.read_csv,
    "tsv": pd.read_csv,
    "parquet": pd.read_parquet,
    "feather": pd.read_feather,
    "hdf": pd.read_hdf,
    "xls": pd.read_excel,
    "xlsx": pd.read_excel,
    "ods": pd.read_excel,
    "json": pd.read_json,
    "pkl": pd.read_pickle,
    "html": pd.read_html,
    "xml": pd.read_xml,
}

# dicionário de extensões e funções do pandas para ler conteúdos no geopandas
LEITOR_GEOPANDAS: typing.Dict[str, typing.Callable[..., pd.DataFrame]] = {
    "parquet": gpd.read_parquet,
    "feather": gpd.read_feather,
    "shp": gpd.read_file,
    "json": gpd.read_file,
    "geojson": gpd.read_file,
    "topojson": gpd.read_file,
}
