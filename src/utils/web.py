import requests
from pathlib import Path
import typing
import typing
from pathlib import Path

import requests


def download_dados_web(caminho: typing.Union[str, Path], url: str) -> None:
    """
    Realiza o download dos dados em um link da Web

    :param caminho: caminho para extração dos dados
    :param url: endereço do site a ser baixado
    """
    r = requests.get(url, stream=True)
    with open(caminho, "wb") as arq:
        arq.write(r.content)
