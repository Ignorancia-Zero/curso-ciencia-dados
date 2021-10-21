import typing
from enum import Enum

from src.aquisicao._etl import BaseETL
from src.aquisicao.inep._micro_inep import BaseINEPETL
from src.aquisicao.inep.censo_docente import DocenteETL
from src.aquisicao.inep.censo_escola import EscolaETL
from src.aquisicao.inep.censo_gestor import GestorETL
from src.aquisicao.inep.censo_matricula import MatriculaETL
from src.aquisicao.inep.censo_turma import TurmaETL
from src.aquisicao.inep.ideb import IDEBETL
from src.io.data_store import DataStore


class ETL(Enum):
    ESCOLA = "escola"
    GESTOR = "gestor"
    TURMA = "turma"
    DOCENTE = "docente"
    MATRICULA = "matricula"
    IDEB = "ideb"


class MicroINEPETL(Enum):
    ESCOLA = ETL.ESCOLA.value
    GESTOR = ETL.GESTOR.value
    TURMA = ETL.TURMA.value
    DOCENTE = ETL.DOCENTE.value
    MATRICULA = ETL.MATRICULA.value


class ETLClass(typing.Protocol):
    def __call__(
        self, ds: DataStore, criar_caminho: bool, reprocessar: bool
    ) -> BaseETL:
        ...


class INEPMicroClass(typing.Protocol):
    def __call__(
        self,
        ds: DataStore,
        criar_caminho: bool,
        reprocessar: bool,
        ano: typing.Union[str, int],
    ) -> BaseINEPETL:
        ...


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT: typing.Dict[ETL, ETLClass] = {
    ETL.ESCOLA: EscolaETL,
    ETL.GESTOR: GestorETL,
    ETL.TURMA: TurmaETL,
    ETL.DOCENTE: DocenteETL,
    ETL.MATRICULA: MatriculaETL,
    ETL.IDEB: IDEBETL,
}

MD_INEP_DICT: typing.Dict[MicroINEPETL, INEPMicroClass] = {
    MicroINEPETL.ESCOLA: EscolaETL,
    MicroINEPETL.GESTOR: GestorETL,
    MicroINEPETL.TURMA: TurmaETL,
    MicroINEPETL.DOCENTE: DocenteETL,
    MicroINEPETL.MATRICULA: MatriculaETL,
}
