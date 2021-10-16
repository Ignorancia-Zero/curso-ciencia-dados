import typing
from pathlib import Path

import requests


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
