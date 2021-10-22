import typing
from pathlib import Path

import pandas as pd
import yaml

CAMINHO_INFO = Path(__file__).parent.parent / "info"


def carrega_yaml(nome_yaml: str) -> typing.Dict[str, typing.Any]:
    """
    Carrega arquivo YAML da pasta info da ferramenta

    :param nome_yaml: nome do arquivo yaml
    :return: dicionário com conteúdo do arquivo
    """
    global CAMINHO_INFO
    with open(CAMINHO_INFO / f"{nome_yaml}", "r", encoding="UTF-8") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def carrega_excel(nome_excel: str, **kwargs) -> pd.DataFrame:
    """
    Carrega arquivo excel da pasta info da ferramenta

    :param nome_excel: nome do arquivo excel
    :param kwargs: argumentos de carregamento
    :return: data frame pandas com conteúdo
    """
    global CAMINHO_INFO
    return pd.read_excel(CAMINHO_INFO / nome_excel, **kwargs)
