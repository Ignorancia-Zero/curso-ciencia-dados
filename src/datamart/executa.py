from src.datamart.config import DMGran
from src.datamart.escola import controi_datamart_escola
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.logs import log_erros


@log_erros
def executa_datamart(granularidade: str, ds: DataStore, ano: str) -> None:
    """
    Constrói um datamart a um determinado nível de granularidade para um
    dado ano de dados

    :param granularidade: nível do datamart a ser gerado
    :param ds: instância de objeto data store
    :param ano: ano da pesquisa a ser processado
    """
    # obtém a granularidade
    gran = DMGran(granularidade)

    # obtém o ano
    if ano == "ultimo":
        cam = ds.gera_caminho(Documento(ds, dict(CatalogoAquisicao.ESCOLA)))
        cam = cam.__class__(cam.obtem_caminho(CatalogoAquisicao.ESCOLA["nome"]))
        ano = cam.lista_conteudo()[-1].replace("ANO=", "")
    ano_int = int(ano)

    if gran == DMGran.ESCOLA:
        controi_datamart_escola(ds, ano_int)
    else:
        raise NotImplementedError(
            f"Nós ainda temos que desenvolver o datamart para {granularidade}"
        )
