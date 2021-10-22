import pandas as pd

from src.io.data_store import Documento, DataStore
from src.io.data_store import CatalogoDatamart
from src.io.data_store import CatalogoAquisicao


def processa_censo_escola(ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Carrega os dados de escola filtrados para escolas em atividade

    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: data frame de escolas em atividade
    """
    doc = Documento(ds, CatalogoAquisicao.ESCOLA)
    dm = ds.carrega_como_objeto(doc, filters=[("ANO", "=", ano)])
    return dm.loc[lambda f: f["TP_ATIVIDADE"] == "EM ATIVIDADE"]


def processa_turmas(dm: pd.DataFrame, ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Carrega a base de turmas e gera as seguintes métricas:
    - XXX
    - XXX

    :param dm: datamart em seu estado atual
    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: data frame de escolas com dados de turma adicionados
    """
    doc = Documento(ds, CatalogoAquisicao.TURMA)
    turma = ds.carrega_como_objeto(doc, filters=[("ANO", "=", ano)])

    return dm


def controi_datamart_escola(ds: DataStore, ano: int) -> None:
    """
    Constrói o datamart de escola para o ano selecionado e exporta
    os dados conforme a configuração do catalogo de dados

    :param ds: instância do data store
    :param ano: ano de processamento da base
    """
    # carregar dados
    dm = processa_censo_escola(ds, ano)
    dm = processa_turmas(dm, ds, ano)
    dm = processa_docentes(dm, ds, ano)
    dm = processa_gestor(dm, ds, ano)
    dm = processa_matricula(dm, ds, ano)
    dm = gera_metricas_ideb(dm, ds, ano)

    # exportar dados
    doc = Documento(ds, referencia=CatalogoDatamart.ESCOLA, data=dm)
    doc.pasta = f"{doc.nome}/ANO={ano}"
    doc.nome = f"{ano}.parquet"
    doc.data.drop(columns=["ANO"], inplace=True)
    ds.salva_documento(doc)
