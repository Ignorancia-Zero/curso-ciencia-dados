from enum import Enum

from src.aquisicao.inep.censo_escola import EscolaETL
from src.utils.logs import log_erros


class EnumETL(Enum):
    ESCOLA = "escola"


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {EnumETL.ESCOLA: EscolaETL}


@log_erros
def executa_etl(etl: str, entrada: str, saida: str, criar_caminho: bool) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param entrada: string com caminho para pasta de entrada
    :param saida: string com caminho para pasta de saída
    :param criar_caminho: flag indicando se devemos criar os caminhos
    """
    global ETL_DICT

    objeto = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho)
    objeto.pipeline()
