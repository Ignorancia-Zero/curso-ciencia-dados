from .caminho_base import _CaminhoBase
from .caminho_local import CaminhoLocal


def obtem_objeto_caminho(caminho: str, criar_caminho: bool = False) -> _CaminhoBase:
    """
    Retorna o objeto caminho adequado com base na string de caminho selecionada

    :param caminho: caminho relativo de interesse
    :param criar_caminho: flag se devemos criar a pasta
    :return: objeto caminho inicializado
    """
    if caminho.startswith("s3://"):
        caminho = caminho[5:]
        bucket = caminho.split("/")[0]
        prefix = "/".join(caminho.split("/")[1:])
        raise NotImplementedError
    elif "gdrive://" in caminho:
        caminho = caminho[9:]
        raise NotImplementedError
    return CaminhoLocal(caminho, criar_caminho=criar_caminho)
