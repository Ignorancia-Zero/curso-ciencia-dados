import typing
from enum import Enum

from src.aquisicao.inep.base_inep import BaseINEPETL
from src.aquisicao.inep.censo_escola import EscolaETL
from src.aquisicao.inep.censo_gestor import GestorETL
from src.aquisicao.inep.censo_turma import TurmaETL
from src.io.data_store import DataStore


class INEP_ETL(Enum):
    ESCOLA = "escola"
    GESTOR = "gestor"
    TURMA = "turma"


# chave = Enum
# valor = Classe de objeto ETL INEP
INEP_DICT: typing.Dict[
    INEP_ETL, typing.Callable[[DataStore, typing.Union[str, int], bool], BaseINEPETL]
] = {
    INEP_ETL.ESCOLA: EscolaETL,
    INEP_ETL.GESTOR: GestorETL,
    INEP_ETL.TURMA: TurmaETL
}
