from enum import Enum
from src.utils.logs import log_erros


class EnumETL(Enum):
    censo_escolar = "CENSO_ESCOLAR"


# chave = Enum
# valor = Classe de objeto ETL
ETL_DICT = {}


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
