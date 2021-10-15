import json
import logging
import os
import pickle
import re
import tempfile
import typing
from io import BytesIO
from io import StringIO
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pyunpack
import yaml
from charamel import Detector
from rarfile import RarFile

from src.io.configs import LEITOR_PANDAS, LEITOR_GEOPANDAS, EXTENSOES_TEXTO
from src.utils.interno import obtem_argumentos_objeto, obtem_extencao


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
            file_bytes = dados.read(10000)
            det = Detector(min_confidence=0.5)
            kwargs["encoding"] = det.detect(file_bytes)
            if kwargs["encoding"] is None:
                kwargs["encoding"] = "latin-1"
            dados.seek(0)

    # para arquivos ods garante que a engine de leitura
    # esteja adequada
    elif ext == "ods":
        kwargs["engine"] = "odf"

    # obtém a função de leitura adequada e obtém os dados
    # como pandas
    return LEITOR_PANDAS[ext](
        dados, **obtem_argumentos_objeto(LEITOR_PANDAS[ext], kwargs)
    )


def load_json(buffer: typing.BinaryIO) -> typing.Dict:
    """
    Lê os dados de um buffer como um objeto json

    :param buffer: buffer para dados json
    :return: dicionário de dados carregado
    """
    dados = json.load(buffer)
    buffer.close()
    return dados


def load_yaml(buffer: typing.BinaryIO) -> typing.Dict:
    """
    Lê os dados de um buffer como um objeto yaml

    :param buffer: buffer para dados yaml
    :return: dicionário de dados carregado
    """
    dados = yaml.load(buffer, Loader=yaml.FullLoader)
    buffer.close()
    return dados


def load_pickle(buffer: typing.BinaryIO) -> typing.Any:
    """
    Lê os dados de um buffer como um série de objetos pickle

    :param buffer: buffer para dados pickle
    :return: lista ou objeto carregado
    """
    objs = []
    while True:
        try:
            o = pickle.load(buffer)
        except EOFError:
            break
        objs.append(o)

    buffer.close()
    if len(objs) > 1:
        return objs
    else:
        return objs[0]


def converte_buffer_em_objeto(
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
    if kwargs.get("como_df"):
        return le_como_df(dados, ext, **kwargs)
    elif kwargs.get("como_gdf"):
        return LEITOR_GEOPANDAS[ext](
            dados, **obtem_argumentos_objeto(LEITOR_GEOPANDAS[ext], kwargs)
        )
    else:
        if ext == "json":
            return load_json(dados)
        elif ext == "yml":
            return load_yaml(dados)
        elif ext == "pkl":
            return load_pickle(dados)
        elif ext in EXTENSOES_TEXTO:
            return dados.read().decode(kwargs.get("encoding"))
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
        padrao_comp = "^"

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
            objs = {
                arq: carrega_arquivo(z.open(arq), obtem_extencao(arq), **kwargs)
                for arq in arqs
            }

    except ValueError as e:
        logging.debug(
            f"Obtivemos um erro {e} ao carregar o zip, fazendo leitura do disco"
        )

        # cria um diretório temporário
        with tempfile.TemporaryDirectory() as tmpdirname:
            temp = Path(tmpdirname)

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
            objs = {
                arq: converte_buffer_em_objeto(z.open(arq), ext, **kwargs)
                for arq in arqs
            }

    # retorna o objeto adequado de acordo com a quantidade de arquivos
    if len(objs) > 1:
        return objs
    elif len(objs) == 1:
        return list(objs.values())[0]


def carrega_arquivo(
    arquivo: typing.Union[
        str, Path, typing.IO[bytes], typing.IO[str], StringIO, BytesIO
    ],
    ext: str,
    **kwargs: typing.Any,
) -> typing.Any:
    """
    Realiza o carregamento de um determinado arquivo, que pode ser uma string,
    path, IO, ou objeto buffer para os conteúdos a serem carregados

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
        arquivo = open(str(arquivo), "rb")

    # lê os dados e fecha o buffer gerado
    dados = converte_buffer_em_objeto(arquivo, ext, **kwargs)
    arquivo.close()

    return dados
