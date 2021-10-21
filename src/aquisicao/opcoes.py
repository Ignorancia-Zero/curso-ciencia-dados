import typing
from enum import Enum

from src.aquisicao.inep.base_inep import BaseINEPETL
from src.aquisicao.inep.censo_docente import DocenteETL
from src.aquisicao.inep.censo_escola import EscolaETL
from src.aquisicao.inep.censo_gestor import GestorETL
from src.aquisicao.inep.censo_matricula import MatriculaETL
from src.aquisicao.inep.censo_turma import TurmaETL
from src.io.data_store import DataStore


class INEP_ETL(Enum):
    ESCOLA = "escola"
    GESTOR = "gestor"
    TURMA = "turma"
    DOCENTE = "docente"
    MATRICULA = "matricula"


# chave = Enum
# valor = Classe de objeto ETL INEP
INEP_DICT: typing.Dict[
    INEP_ETL,
    typing.Callable[[DataStore, typing.Union[str, int], bool, bool], BaseINEPETL],
] = {
    INEP_ETL.ESCOLA: EscolaETL,
    INEP_ETL.GESTOR: GestorETL,
    INEP_ETL.TURMA: TurmaETL,
    INEP_ETL.DOCENTE: DocenteETL,
    INEP_ETL.MATRICULA: MatriculaETL,
}
