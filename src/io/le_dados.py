import json
import os
import pickle
import re
import shutil
import typing
from io import BytesIO
from io import StringIO
from pathlib import Path
from zipfile import ZipFile

import geopandas
import pandas as pd
import pyunpack
import yaml
from charamel import Detector
from rarfile import RarFile

from src.utils.interno import obtem_argumentos_objeto, obtem_extencao

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
    "parquet": geopandas.read_parquet,
    "feather": geopandas.read_feather,
    "shp": geopandas.read_file,
    "geojson": geopandas.read_file,
}


def le_como_df(dados: typing.BinaryIO, ext: str, **kwargs: typing.Any) -> pd.DataFrame:
    """
    Le os dados contidos num buffer como um objeto data frame

    :param dados: bytes IO com dados de entrada
    :param ext: extenção / formato do arquivo
    :param kwargs: parâmetros de leitura
    :return: objeto python
    """
    # para arquivos csv, tsv e txt, verifica a codificação
    # do mesmo caso não tenha sido fornecido
    if ext in ["csv", "tsv", "txt"]:
        if "encoding" not in kwargs:
            file_bytes = dados.read()
            det = Detector(min_confidence=0.5)
            kwargs["encoding"] = det.detect(file_bytes)
            if kwargs["encoding"] is None:
                kwargs["encoding"] = "latin-1"
            dados = BytesIO(file_bytes)

    # para arquivos ods garante que a engine de leitura
    # esteja adequada
    elif ext == "ods":
        kwargs["engine"] = "odf"

    # obtém a função de leitura adequada e obtém os dados
    # como pandas
    return LEITOR_PANDAS[ext](
        dados, **obtem_argumentos_objeto(LEITOR_PANDAS[ext], kwargs)
    )


def converte_em_objeto(
    dados: typing.BinaryIO, ext: str, **kwargs: typing.Any
) -> typing.Any:
    """
    Converte um buffer de dados em um objeto de acordo com os dados
    devolvidos, a extenção do arquivo e os argumentos de leitura fornecidos

    :param dados: bytes IO com dados de entrada
    :param ext: extenção / formato do arquivo
    :param kwargs: parâmetros de leitura
    :return: objeto python
    """
    if "conv_df" in kwargs or ext in EXTENSAO_DF:
        le_como_df(dados, ext, **kwargs)
    elif ext == "json":
        return json.load(dados)
    elif ext == "yml":
        return yaml.load(dados, Loader=yaml.FullLoader)
    elif ext == "pkl":
        objs = []
        while True:
            try:
                o = pickle.load(dados)
            except EOFError:
                break
            objs.append(o)
        if len(objs) > 1:
            return objs
        else:
            return objs[0]
    elif ext in ["txt", "xml", "html"]:
        if "encoding" in kwargs:
            return dados.read().decode(kwargs["encoding"])
        else:
            return dados.read().decode("UTF-8")
    else:
        raise NotImplementedError(f"Não implementamos leitura de arquivos {ext}")


def le_dados_comprimidos(
    arquivo: typing.Union[str, Path, typing.IO[bytes], BytesIO], ext: str, **kwargs
) -> typing.Dict[str, typing.Any]:
    """
    Lê dados comprimidos em um arquivo zip ou rar e devolve um
    dicionário de arquivos com os conteúdos carregados de acordo
    com o padrão de leitura selecionado

    :param arquivo: caminho para, caminho aberto ou dados a serem processados
    :param ext: extensão do arquivo
    :param padrao_comp: expressão regular para filtrar arquivos em comprimidos
    :param kwargs: argumentos de leitura
    :return: dicionário de arquivos carregados
    """
    # obtém o padrão de nome de arquivo
    if "padrao_comp" in kwargs:
        padrao_comp = kwargs["padrao_comp"]
        del kwargs["padrao_comp"]
    else:
        padrao_comp = None

    # selecionamos o objeto de leitura adequado
    obj_l = RarFile if ext == "rar" else ZipFile

    # abre o arquivo a ser processado
    try:
        with obj_l(arquivo, **obtem_argumentos_objeto(obj_l, kwargs)) as z:
            # obtém a lista de arquivos que deve ser lida
            arqs = [
                f
                for f in z.namelist()
                if re.search(padrao_comp, f) is not None and obtem_extencao(f) != ""
            ]

            # lê os arquivos para o dicionários
            objs = {arq: converte_em_objeto(z.open(arq), ext, **kwargs) for arq in arqs}

    except ValueError:
        # cria um diretório temporário
        caminho = Path(os.path.dirname(__file__))
        temp = caminho / "temp"
        temp.mkdir(parents=True, exist_ok=True)

        # cria um arquivo, caso tenha sido fornecido um bytes IO
        if not isinstance(arquivo, str) and not isinstance(arquivo, Path):
            arquivo.seek(0)
            with open(temp / f"arq_temp.{ext}", "wb") as f:
                f.write(arquivo.read())
            arquivo = temp / f"arq_temp.{ext}"

        # realiza o unpack dos conteúdos do zip
        azip = pyunpack.Archive(arquivo)
        azip.extractall(temp)

        # lista os arquivos a serem carregados
        arqs = [
            os.path.join(path, f)
            for path, directories, files in os.walk(temp)
            for f in files
            if re.search(padrao_comp, f) is not None
            and obtem_extencao(f) != ""
            and f != f"arq_temp.{ext}"
        ]

        # lê os arquivos para o dicionários
        objs = {arq: converte_em_objeto(z.open(arq), ext, **kwargs) for arq in arqs}

        # apaga o diretório temporário
        shutil.rmtree(temp)

    # retorna o objeto adequado de acordo com a quantidade de arquivos
    if len(objs) > 1:
        return objs
    else:
        return list(objs.values())[0]


def carrega_arquivo(
    arquivo: typing.Union[
        str, Path, typing.IO[bytes], typing.IO[str], StringIO, BytesIO
    ],
    ext: str,
    **kwargs: typing.Any,
) -> typing.Any:
    """
    Realiza o carregamento de um determinado arquivo, que pode ser
    uma string, path, IO, ou objeto buffer para os conteúdos a serem
    carregados

    :param arquivo: caminho para, caminho aberto ou dados a serem processados
    :param ext: extensão do arquivo
    :param kwargs: argumentos de leitura
    :return: dados que podem estar empacotados em listas ou dicionários
    """
    # caso tenhamos uma extensão de arquivo comprimido, teremos que
    # ler-lo utilizando objetos específicos, e pode ser que haja algum
    # padrão de arquivo para extrair os conteúdos do mesmo
    if ext == "zip" or ext == "rar":
        return le_dados_comprimidos(arquivo, ext, **kwargs)

    # se for um caminho para o arquivo converte-o para bytes e manda ler o arquivo
    elif isinstance(arquivo, str) or isinstance(arquivo, Path):
        # para arquivos de geopandas, entretanto, não é possível ler os dados
        # por um bytes IO, então nós iremos tentar ler-lo como um data frame
        # deste tipo, e somente em caso de erro iremos fazer a conversão
        if ext in LEITOR_GEOPANDAS:
            try:
                return LEITOR_GEOPANDAS[ext](
                    arquivo, **obtem_argumentos_objeto(LEITOR_GEOPANDAS[ext], kwargs)
                )
            except ValueError:
                pass

        with open(arquivo, "rb") as f:
            return converte_em_objeto(f, ext, **kwargs)

    # se já for um arquivo de IO extraí diretamente
    else:
        return converte_em_objeto(arquivo, ext, **kwargs)
