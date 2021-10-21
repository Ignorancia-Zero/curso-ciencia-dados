import typing

from .gdrive import CaminhoGDrive
from .local import CaminhoLocal
from .s3 import CaminhoS3
from .sqlite import CaminhoSQLite


def obtem_objeto_caminho(
    caminho: str, criar_caminho: bool = False
) -> typing.Union[CaminhoLocal, CaminhoGDrive, CaminhoS3, CaminhoSQLite]:
    """
    Retorna o objeto caminho adequado com base na string de caminho selecionada

    :param caminho: caminho relativo de interesse
    :param criar_caminho: flag se devemos criar a pasta
    :return: objeto caminho inicializado
    """
    if caminho.startswith("s3://"):
        return CaminhoS3(caminho, criar_caminho)
    elif caminho.startswith("gdrive://"):
        return CaminhoGDrive(caminho, criar_caminho)
    elif caminho.startswith("sqlite://"):
        return CaminhoSQLite(caminho, criar_caminho)
    else:
        return CaminhoLocal(caminho, criar_caminho)
