from src.aquisicao.opcoes import ETL
from src.aquisicao.opcoes import ETL_DICT
from src.aquisicao.opcoes import MD_INEP_DICT
from src.aquisicao.opcoes import MicroINEPETL
from src.io.data_store import DataStore
from src.utils.logs import log_erros


@log_erros
def executa_etl(
    etl: str, ds: DataStore, criar_caminho: bool, reprocessar: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param ds: instância de objeto data store
    :param criar_caminho: flag indicando se devemos criar os caminhos
    :param reprocessar: flag indicando se devemos reprocessar a base
    """
    objeto = ETL_DICT[ETL(etl)](
        ds=ds, criar_caminho=criar_caminho, reprocessar=reprocessar
    )
    objeto.pipeline()


@log_erros
def executa_etl_microdado_inep(
    etl: str, ds: DataStore, ano: str, criar_caminho: bool, reprocessar: bool
) -> None:
    """
    Executa o pipeline de ETL de uma determinada base de Microdados do INEP

    :param etl: nome do ETL a ser executado
    :param ds: instância de objeto data store
    :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
    :param criar_caminho: flag indicando se devemos criar os caminhos
    :param reprocessar: flag indicando se devemos reprocessar a base
    """
    objeto = MD_INEP_DICT[MicroINEPETL(etl)](
        ds=ds,
        criar_caminho=criar_caminho,
        reprocessar=reprocessar,
        ano=int(ano) if ano.isnumeric() else ano,
    )
    objeto.pipeline()
