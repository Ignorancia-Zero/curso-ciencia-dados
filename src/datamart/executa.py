from src.datamart.config import DMGran
from src.io.data_store import DataStore
from src.utils.logs import log_erros
from src.datamart.escola import controi_datamart_escola


@log_erros
def executa_datamart(granularidade: str, ds: DataStore, ano: int) -> None:
    """
    Constrói um datamart a um determinado nível de granularidade para um
    dado ano de dados

    :param granularidade: nível do datamart a ser gerado
    :param ds: instância de objeto data store
    :param ano: ano da pesquisa a ser processado
    """
    granularidade = DMGran(granularidade)
    if granularidade == DMGran.ESCOLA:
        controi_datamart_escola(ds, ano)
    else:
        raise NotImplementedError(f"Nós ainda temos que desenvolver o datamart para {granularidade}")
