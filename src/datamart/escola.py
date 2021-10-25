import numpy as np
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
    return dm.loc[lambda f: f["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE"].drop(
        columns=["TP_SITUACAO_FUNCIONAMENTO"]
    )


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
    res = turma.groupby(["ID_ESCOLA"]).agg({"ID_TURMA": "count"}).reset_index()
    res.columns = ["ID_ESCOLA", "QT_TURMAS"]

    # processa as colunas IN_
    res = res.merge(
        fn.processa_coluna_in(turma, "ID_ESCOLA", "ID_TURMA", "TURMA", perc=False),
        how="left",
    )
    res["PC_TURMA_ESPECIAL_EXCLUSIVA"] = (
        res["QT_TURMA_ESPECIAL_EXCLUSIVA"] / res["QT_TURMAS"]
    )

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
        fn.processa_coluna_qt_nu(
            turma, "ID_ESCOLA", "TURMA", metricas=("mean", "median")
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
    res = res.merge(df[["QT_TURMA_ATIVIDADE_COMP"]].reset_index(), how="left")

    # adiciona os dados de etapa ensino, e gera as colunas informando
    # os tipos de turmas disponíveis
    df_qt = (
        turma.merge(ds.df_ee, how="left")
        .groupby(["ID_ESCOLA"])[
            [
                "REGULAR",
                "INFANTIL",
                "FUNDAMENTAL",
                "AI",
                "AF",
                "MEDIO",
                "EJA",
                "PROFISSIONALIZANTE",
                "TECNICO",
                "FIC",
            ]
        ]
        .sum()
        .reset_index()
    )
    df_qt = res[["ID_ESCOLA"]].merge(df_qt, how="left").fillna(0).astype("uint32")
    df_in = pd.concat(
        [df_qt[["ID_ESCOLA"]], (df_qt.iloc[:, 1:] > 0).astype("uint8")], axis=1
    )
    df_in.columns = ["ID_ESCOLA"] + [f"IN_TURMA_{c}" for c in df_in.columns[1:]]
    df_qt.columns = ["ID_ESCOLA"] + [f"QT_TURMA_{c}" for c in df_qt.columns[1:]]
    res = res.merge(df_in, how="left")
    res = res.merge(df_qt, how="left")

    # adiciona os dados ao datamart
    dm = dm.merge(res, how="left")

    return dm


def processa_docentes(dm: pd.DataFrame, ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Incorpora os dados de docentes ao datamart de escola

    :param dm: datamart em seu estado atual
    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: datamart com dados de docente incorporados
    """
    # carrega os dados de docente e de turma
    docente = ds.carrega_como_objeto(
        Documento(ds, CatalogoAquisicao.DOCENTE),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    depara = (
        ds.carrega_como_objeto(
            Documento(ds, CatalogoAquisicao.DOCENTE_TURMA),
            como_df=True,
            filters=[("ANO", "=", ano)],
        )
        .merge(
            ds.carrega_como_objeto(
                Documento(ds, CatalogoAquisicao.TURMA),
                como_df=True,
                filters=[("ANO", "=", ano)],
                columns=["ID_TURMA", "ID_ESCOLA"],
            )
        )
        .drop(columns=["ID_TURMA"])
        .drop_duplicates()
    )

    # duplica linhas de docente por escola
    docente = docente.merge(depara[["ID_ESCOLA", "ID_DOCENTE"]].drop_duplicates())

    # soma o total de docentes por escola
    res = depara.groupby(["ID_ESCOLA"]).agg({"ID_DOCENTE": "nunique"}).reset_index()
    res.columns = ["ID_ESCOLA", "QT_DOCENTES"]

    # processa as colunas IN_
    res = res.merge(
        fn.processa_coluna_in(docente, "ID_ESCOLA", "ID_DOCENTE", "DOCENTE"),
        how="left",
    )

    # processa as colunas TP com características pessoais do docente
    for (tp_col, pf, df) in [
        ("TP_SEXO", "DOCENTE_SEXO", docente),
        ("TP_COR_RACA", "DOCENTE_COR", docente),
        ("TP_NACIONALIDADE", "DOCENTE", docente),
        ("TP_ZONA_RESIDENCIAL", "DOCENTE_ZONA", docente),
        ("TP_LOCAL_RESID_DIFERENCIADA", "DOCENTE_LDIF", docente),
        ("TP_ESCOLARIDADE", "DOCENTE", docente),
        ("TP_ENSINO_MEDIO", "DOCENTE_EM", docente),
        ("TP_TIPO_CONTRATACAO", "DOCENTE", depara),
        ("TP_TIPO_DOCENTE", "DOCENTE", depara),
    ]:
        res = res.merge(
            fn.processa_coluna_tp(
                df, "ID_ESCOLA", tp_col, "ID_DOCENTE", pf, recriar=False
            ),
            on="ID_ESCOLA",
            how="left",
        )

    # cria a coluna de docentes com formação complementar
    if docente["CO_AREA_COMPL_PEDAGOGICA_1"].count() > 0:
        res = res.merge(
            docente.reindex(
                columns=["ID_DOCENTE", "ID_ESCOLA", "CO_AREA_COMPL_PEDAGOGICA_1"]
            )
            .drop_duplicates()
            .assign(
                QT_DOCENTE_COMPL_PEDAGOGICA=lambda f: f[
                    "CO_AREA_COMPL_PEDAGOGICA_1"
                ].notnull()
            )
            .groupby(["ID_ESCOLA"])["QT_DOCENTE_COMPL_PEDAGOGICA"]
            .sum()
            .reset_index(),
            how="left",
        )
    else:
        res["QT_DOCENTE_COMPL_PEDAGOGICA"] = np.nan

    # verifica docentes que estão em municipios diferentes da escola
    if docente["CO_MUNICIPIO_END"].count() > 0:
        res = res.merge(
            docente[["ID_DOCENTE", "ID_ESCOLA", "CO_MUNICIPIO_END"]]
            .merge(dm[["ID_ESCOLA", "CO_MUNICIPIO"]], how="left")
            .assign(
                QT_DOCENTE_MUN_DIF=lambda f: (
                    (f["CO_MUNICIPIO_END"] != f["CO_MUNICIPIO"])
                    & (f["CO_MUNICIPIO_END"].notnull())
                ).astype("int")
            )
            .groupby(["ID_ESCOLA"])["QT_DOCENTE_MUN_DIF"]
            .sum()
            .reset_index(),
            how="left",
        )
    else:
        res["QT_DOCENTE_MUN_DIF"] = np.nan

    # TODO: Construir um conjunto de colunas relacionadas a formação do docente
    # "CO_IES_1": "uint32"
    # "CO_AREA_CURSO_1": "uint8"
    # "CO_CURSO_1": "str"
    # "NU_ANO_INICIO_1": "float32"
    # "NU_ANO_CONCLUSAO_1": "float32"

    # adiciona os dados ao datamart
    dm = dm.merge(res, how="left")

    return dm


def processa_gestor(dm: pd.DataFrame, ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Incorpora os dados de gestores ao datamart de escola

    :param dm: datamart em seu estado atual
    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: datamart de escola com os dados de gestor
    """
    # carrega os dados de gestor
    gestor = ds.carrega_como_objeto(
        Documento(ds, CatalogoAquisicao.GESTOR),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    depara = ds.carrega_como_objeto(
        Documento(ds, CatalogoAquisicao.GESTOR_ESCOLA),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    gestor = gestor.merge(
        depara[["ID_GESTOR", "ID_ESCOLA"]].drop_duplicates(), how="left"
    )

    # soma todas as turmas
    res = gestor.groupby(["ID_ESCOLA"]).agg({"ID_GESTOR": "count"}).reset_index()
    res.columns = ["ID_ESCOLA", "QT_GESTORES"]

    # processa as colunas IN_
    res = res.merge(
        fn.processa_coluna_in(gestor, "ID_ESCOLA", "ID_GESTOR", "GESTOR", perc=False),
        how="left",
    )

    # obtém a área do curso do gestor
    gestor = gestor.merge(
        ds.df_cr, left_on=["CO_CURSO_1"], right_on=["CO_CURSO"], how="left"
    ).drop(columns=["CO_CURSO"])

    # processa as colunas TP
    for (tp_col, pf, df) in [
        ("TP_SEXO", "GESTOR_SEXO", gestor),
        ("TP_COR_RACA", "GESTOR_COR", gestor),
        ("TP_NACIONALIDADE", "GESTOR", gestor),
        ("TP_ESCOLARIDADE", "GESTOR", gestor),
        ("TP_ENSINO_MEDIO", "GESTOR_EM", gestor),
        ("TP_AREA_CURSO", "GESTOR_FORMACAO", gestor),
        ("TP_CARGO_GESTOR", "GESTOR_CARGO", depara),
        ("TP_TIPO_ACESSO_CARGO", "GESTOR_ACESSO", depara),
        ("TP_TIPO_CONTRATACAO", "GESTOR_CONT", depara),
    ]:
        res = res.merge(
            fn.processa_coluna_tp(df, "ID_ESCOLA", tp_col, "ID_GESTOR", pf, recriar=False),
            on="ID_ESCOLA",
            how="left",
        )

    # adiciona os dados ao datamart
    dm = dm.merge(res, how="left")

    return dm


def processa_ideb(dm: pd.DataFrame, ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Adiciona os dados de IDEB do último censo a base de escola

    :param dm: datamart em seu estado atual
    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: datamart com os dados de IDEB incorporados
    """
    # carrega os dados de ideb, entretanto se o ano for par
    # nós vamos pegar os dados do ano anterior, caso contrário
    # vamos pegar do ano em si. Isso é feito porque o IDEB ocorre
    # a cada 2 anos e apenas em anos ímpares
    ano = ano if ano % 2 == 1 else ano - 1
    ideb = ds.carrega_como_objeto(
        Documento(ds, CatalogoAquisicao.IDEB),
        como_df=True,
        filters=[("ANO", "=", ano)],
    ).drop(columns="ANO")

    return dm.merge(ideb, how="left")


def gera_metricas_adicionais(dm: pd.DataFrame) -> pd.DataFrame:

    return dm.assign(
        NU_ALUNO_POR_TURMA=lambda f: f["QT_ALUNOS"] / f["QT_TURMAS"],
        NU_ALUNO_POR_DOCENTE=lambda f: f["QT_ALUNOS"] / f["QT_DOCENTES"],

        CO_REGIAO=lambda f: f["CO_MUNICIPIO"] // 1000000,
        CO_UF=lambda f: f["CO_MUNICIPIO"] // 100000,
    )


def controi_datamart_escola(ds: DataStore, ano: int) -> None:
    """
    Constrói o datamart de escola para o ano selecionado e exporta
    os dados conforme a configuração do catalogo de dados

    :param ds: instância do data store
    :param ano: ano de processamento da base
    """
    # carrega e processa as bases de dados
    dm = processa_censo_escola(ds, ano)
    dm = processa_turmas(dm, ds, ano)
    dm = processa_docentes(dm, ds, ano)
    dm = processa_gestor(dm, ds, ano)
    dm = processa_matricula(dm, ds, ano)
    dm = processa_ideb(dm, ds, ano)
    dm = gera_metricas_adicionais(dm)

    # exportar dados
    doc = Documento(ds, referencia=CatalogoDatamart.ESCOLA, data=dm)
    ds.salva_documento(
        doc, partition_cols=["ANO", "CO_REGIAO", "CO_UF", "CO_MUNICIPIO", "ID_ESCOLA"]
    )
