import yaml
import typing
from pathlib import Path


CAMINHO_INFO = Path(__file__).parent / "info"


def carrega_yaml(nome_yaml: str) -> typing.Dict[str, typing.Any]:
    """
    Carrega arquivo YAML da pasta info da ferramenta

    :param nome_yaml: nome do arquivo yaml
    :return: dicionário com conteúdo do arquivo
    """
    global CAMINHO_INFO
    with open(CAMINHO_INFO / f"{nome_yaml}", "r") as f:
        return yaml.load(f)
