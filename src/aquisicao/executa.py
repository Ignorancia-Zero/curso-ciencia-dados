import typing

from src.aquisicao.opcoes import INEP_ETL, INEP_DICT
from src.io.data_store import DataStore
from src.utils.logs import log_erros


@log_erros
def executa_etl_inep(
    etl: str, ds: DataStore, ano: typing.Union[int, str], criar_caminho: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param ds: instância de objeto data store
    :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
    :param criar_caminho: flag indicando se devemos criar os caminhos
    """
    if ano.isnumeric():
        ano = int(ano)
    objeto = INEP_DICT[INEP_ETL(etl)](ds, ano, criar_caminho)
    objeto.pipeline()
