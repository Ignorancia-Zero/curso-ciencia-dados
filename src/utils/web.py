import typing
from pathlib import Path

import requests


def download_dados_web(
    caminho: typing.Union[str, Path, typing.BinaryIO], url: str
) -> typing.BinaryIO:
    """
    Realiza o download dos dados em um link da Web

    :param caminho: caminho para extração dos dados
    :param url: endereço do site a ser baixado
    """
    r = requests.get(url, stream=True)
    if isinstance(caminho, str) or isinstance(caminho, Path):
        arq = open(caminho, "wb")
    else:
        arq = caminho
    arq.write(r.content)
    arq.close()
    return arq
