from enum import Enum

from src.aquisicao.inep.censo_escola import EscolaETL
from src.io.data_store import DataStore
from src.utils.logs import log_erros


class EnumETL(Enum):
    ESCOLA = "escola"


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {EnumETL.ESCOLA: EscolaETL}


@log_erros
def executa_etl(etl: str, ds: DataStore, criar_caminho: bool) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param ds: instância de objeto data store
    :param criar_caminho: flag indicando se devemos criar os caminhos
    """
    global ETL_DICT

    objeto = ETL_DICT[EnumETL(etl)](ds, criar_caminho)
    objeto.pipeline()
