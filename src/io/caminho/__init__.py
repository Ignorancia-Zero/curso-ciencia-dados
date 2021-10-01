import typing

from .caminho_local import CaminhoLocal
from .caminho_s3 import CaminhoS3


def obtem_objeto_caminho(
    caminho: str, criar_caminho: bool = False
) -> typing.Union[CaminhoS3, CaminhoLocal]:
    """
    Retorna o objeto caminho adequado com base na string de caminho selecionada

    :param caminho: caminho relativo de interesse
    :param criar_caminho: flag se devemos criar a pasta
    :return: objeto caminho inicializado
    """
    if caminho.startswith("s3://"):
        return CaminhoS3(caminho, criar_caminho)
    elif caminho.startswith("gdrive://"):
        raise NotImplementedError
    else:
        return CaminhoLocal(caminho, criar_caminho)
