from enum import Enum
import typing

from src.aquisicao.inep.base_inep import BaseINEPETL
from src.aquisicao.inep.censo_escola import EscolaETL
from src.io.data_store import DataStore


class INEP_ETL(Enum):
    ESCOLA = "escola"


# chave = Enum
# valor = Classe de objeto ETL INEP
INEP_DICT: typing.Dict[
    INEP_ETL, typing.Callable[[DataStore, typing.Union[str, int], bool], BaseINEPETL]
] = {INEP_ETL.ESCOLA: EscolaETL}
