from src.datamart.config import DM_GRAN
from src.io.data_store import DataStore
from src.utils.logs import log_erros


@log_erros
def executa_datamart(granularidade: str, ds: DataStore, ano: str) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param granularidade: nome do ETL a ser executado
    :param ds: instância de objeto data store
    :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
    """
    granularidade = DM_GRAN(granularidade)
    if granularidade == DM_GRAN.MATRICULA:
        pass

    objeto.pipeline()
