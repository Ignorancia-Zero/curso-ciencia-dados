import typing
from pathlib import Path
import urllib.request

import bs4
import requests


def obtem_pagina(url: str) -> bs4.BeautifulSoup:
    """
    Lê uma página Web utilizando a biblioteca resquests

    :param url: url para processar
    :return: objeto BeautifulSoup com resultado da página
    """
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    res = urllib.request.urlopen(req).read()
    return bs4.BeautifulSoup(res)


def download_dados_web(
    caminho: typing.Union[str, Path, typing.IO[bytes], typing.BinaryIO], url: str
) -> typing.Union[typing.IO[bytes], typing.BinaryIO]:
    """
    Realiza o download dos dados em um link da Web

    :param caminho: caminho para extração dos dados
    :param url: endereço do site a ser baixado
    """
    r = requests.get(url, stream=True)
    if isinstance(caminho, str) or isinstance(caminho, Path):
        arq: typing.Union[typing.IO[bytes], typing.BinaryIO] = open(caminho, "wb")
    else:
        arq = caminho
    arq.write(r.content)
    arq.close()
    return arq
