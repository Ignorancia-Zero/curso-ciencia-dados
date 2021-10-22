import pandas as pd

import src.datamart.funcoes as fn
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import CatalogoDatamart
from src.io.data_store import Documento, DataStore


def processa_censo_escola(ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Carrega os dados de escola filtrados para escolas em atividade

    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: data frame de escolas em atividade
    """
    doc = Documento(ds, CatalogoAquisicao.ESCOLA)
    dm = ds.carrega_como_objeto(doc, como_df=True, filters=[("ANO", "=", ano)])
    return dm.loc[lambda f: f["TP_ATIVIDADE"] == "EM ATIVIDADE"]


def processa_turmas(dm: pd.DataFrame, ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Carrega a base de turmas e gera as seguintes métricas:
    - Contagem de turmas que a escola tem
    - Soma de colunas IN -> N. de Turmas com oferecimentos distintos
    - Soma de colunas IN > 0 -> Escola oferece turma do tipo X
    - Soma de colunas TP para cada categoria

    :param dm: datamart em seu estado atual
    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: data frame de escolas com dados de turma adicionados
    """
    # carrega os dados de turma
    doc = Documento(ds, CatalogoAquisicao.TURMA)
    turma = ds.carrega_como_objeto(doc, como_df=True, filters=[("ANO", "=", ano)])

    # soma todas as turmas
    res = turma.groupby(["ID_ESCOLA", "ANO"]).agg({"ID_TURMA": "count"}).reset_index()
    res.columns = ["ID_ESCOLA", "ANO", "QT_TURMAS"]

    # processa as colunas IN_
    res = res.merge(fn.processa_coluna_in(turma, "ID_ESCOLA", "TURMA"), how="left")

    # processa as colunas TP
    for (tp_col, pf) in [
        ("TP_MEDIACAO_DIDATICO_PEDAGO", "TURMA"),
        ("TP_TIPO_ATENDIMENTO_TURMA", "TURMA_ATEND"),
        ("TP_TIPO_LOCAL_TURMA", "TURMA_LOCAL"),
        ("TP_MOD_ENSINO", "TURMA_MOD"),
        ("TP_TIPO_TURMA", "TURMA_TIPO"),
    ]:
        res = res.merge(
            fn.processa_coluna_tp(turma, "ID_ESCOLA", tp_col, "ID_TURMA", pf),
            on="ID_ESCOLA",
            how="left",
        )

    # processa colunas numéricas
    res = res.merge(
        fn.processa_coluna_tp_nu(
            turma, "ID_ESCOLA", "TURMA", metricas=("sum", "mean", "median")
        ).rename(
            columns={
                "NU_TURMA_SUM_DURACAO_TURMA": "NU_TURMA_SUM_DURACAO",
                "NU_TURMA_MEAN_DURACAO_TURMA": "NU_TURMA_MEAN_DURACAO",
                "NU_TURMA_MEDIAN_DURACAO_TURMA": "NU_TURMA_MEDIAN_DURACAO",
            }
        ),
        how="left",
    )

    # calcula as turmas com atividades complementares
    df = turma.groupby(["ID_ESCOLA"])[
        [f"CO_TIPO_ATIVIDADE_{i}" for i in range(1, 7)]
    ].count()
    df["QT_TURMA_ATIVIDADE_COMP"] = df.sum(axis=1)
    res = res.merge(
        df[["QT_TURMA_ATIVIDADE_COMP"]].reset_index(),
        how="left"
    )

    # adiciona os dados de etapa ensino, e gera as colunas informando
    # os tipos de turmas disponíveis
    df_qt = turma.merge(ds.df_ee, how="left").groupby(["ID_ESCOLA"])[[
        "INFANTIL", "FUNDAMENTAL", "AI", "AF", "MEDIO", "TECNICO", "EJA", "FIC"
    ]].sum()
    df_in = df_qt[df_qt > 0].astype("uint8")
    df_in.columns = "QT_TURMA_" + df_in.columns
    df_qt.columns = "QT_TURMA_" + df_qt.columns
    res = res.merge(df_in.reset_index(), how="left")
    res = res.merge(df_qt.reset_index(), how="left")

    # adiciona os dados ao datamart
    dm = dm.merge(res, how="left")

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
