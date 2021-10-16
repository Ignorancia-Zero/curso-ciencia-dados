import os
import typing
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.configs import PASTA_DADOS

# caminhos para ambientes
PATH_LOCAL = str(Path(__file__).parent.parent.parent)
PATH_GDRIVE = "Projetos/IZ/Cursos/Ciência de Dados/Compartilhar"

# Dicionário com as configurações dos ambientes de DS
DS_ENVS: typing.Dict[str, str] = {
    "teste": os.path.join(PATH_LOCAL, "src", "tests", PASTA_DADOS),
    "local_amostra": os.path.join(PATH_LOCAL, PASTA_DADOS, "amostra"),
    "local_completo": os.path.join(PATH_LOCAL, PASTA_DADOS, "completo"),
    "gdrive_amostra": f"gdrive://{PATH_GDRIVE}/{PASTA_DADOS}/amostra",
    "gdrive_completo": f"gdrive://{PATH_GDRIVE}/{PASTA_DADOS}/completo",
}

# extensões de arquivos que são considerados como textos
EXTENSOES_TEXTO = ["txt", "html", "xml"]

# extensões de arquivo que fazem parte do shapefile
EXTENSOES_SHAPE = ["shp", "shx", "dbf", "prj", "xml", "sbn", "sbx", "cpg"]

# lista de extensões que devem ser nativamente interpretadas como data frames
EXTENSAO_DF = {"csv", "tsv", "parquet", "hdf", "xls", "xlsx", "ods", "feather"}

# funções para exportar data frames pandas
ESCREVE_PANDAS: typing.Dict[str, typing.Callable] = {
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
    "xml": pd.DataFrame.to_xml, # type: ignore
}

# funções para exportar data frames de geopandas
ESCREVE_GEOPANDAS: typing.Dict[str, typing.Callable] = {
    "parquet": gpd.GeoDataFrame.to_parquet,
    "feather": gpd.GeoDataFrame.to_feather,
    "shp": gpd.GeoDataFrame.to_file,
    "json": gpd.GeoDataFrame.to_file,
    "geojson": gpd.GeoDataFrame.to_file,
    "topojson": gpd.GeoDataFrame.to_file,
}

# dicionário de extensões e funções do pandas para ler conteúdos
LEITOR_PANDAS: typing.Dict[str, typing.Callable] = {
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
LEITOR_GEOPANDAS: typing.Dict[str, typing.Callable] = {
    "parquet": gpd.read_parquet,
    "feather": gpd.read_feather,
    "shp": gpd.read_file,
    "json": gpd.read_file,
    "geojson": gpd.read_file,
    "topojson": gpd.read_file,
}
