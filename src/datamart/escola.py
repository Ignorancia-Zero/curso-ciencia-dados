import logging

import numpy as np
import pandas as pd
from tqdm import tqdm

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
    doc = Documento(ds, dict(CatalogoAquisicao.ESCOLA))
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
    doc = Documento(ds, dict(CatalogoAquisicao.TURMA))
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
        ("TP_MEDIACAO_DIDATICO_PEDAGO", "TURMA_MEDIACAO"),
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
        Documento(ds, dict(CatalogoAquisicao.DOCENTE)),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    depara = (
        ds.carrega_como_objeto(
            Documento(ds, dict(CatalogoAquisicao.DOCENTE_TURMA)),
            como_df=True,
            filters=[("ANO", "=", ano)],
        )
        .merge(
            ds.carrega_como_objeto(
                Documento(ds, dict(CatalogoAquisicao.TURMA)),
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
        ("TP_NACIONALIDADE", "DOCENTE_NASC", docente),
        ("TP_ZONA_RESIDENCIAL", "DOCENTE_ZONA", docente),
        ("TP_LOCAL_RESID_DIFERENCIADA", "DOCENTE_LDIF", docente),
        ("TP_ESCOLARIDADE", "DOCENTE_ESC", docente),
        ("TP_ENSINO_MEDIO", "DOCENTE_EM", docente),
        ("TP_TIPO_CONTRATACAO", "DOCENTE_CONT", depara),
        ("TP_TIPO_DOCENTE", "DOCENTE_TIPO", depara),
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
        Documento(ds, dict(CatalogoAquisicao.GESTOR)),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    depara = ds.carrega_como_objeto(
        Documento(ds, dict(CatalogoAquisicao.GESTOR_ESCOLA)),
        como_df=True,
        filters=[("ANO", "=", ano)],
    )
    gestor = gestor.merge(
        depara[["ID_GESTOR", "ID_ESCOLA"]].drop_duplicates(), how="left"
    )

    # soma todos os gestores
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
        ("TP_NACIONALIDADE", "GESTOR_NASC", gestor),
        ("TP_ESCOLARIDADE", "GESTOR_ESC", gestor),
        ("TP_ENSINO_MEDIO", "GESTOR_EM", gestor),
        ("TP_AREA_CURSO", "GESTOR_FORMACAO", gestor),
        ("TP_CARGO_GESTOR", "GESTOR_CARGO", depara),
        ("TP_TIPO_ACESSO_CARGO", "GESTOR_ACESSO", depara),
        ("TP_TIPO_CONTRATACAO", "GESTOR_CONT", depara),
    ]:
        res = res.merge(
            fn.processa_coluna_tp(
                df, "ID_ESCOLA", tp_col, "ID_GESTOR", pf, recriar=False
            ),
            on="ID_ESCOLA",
            how="left",
        )

    # adiciona os dados ao datamart
    dm = dm.merge(res, how="left")

    return dm


def processa_matricula(dm: pd.DataFrame, ds: DataStore, ano: int) -> pd.DataFrame:
    """
    Incorpora os dados de alunos e matrículas ao datamart de escola

    :param dm: datamart em seu estado atual
    :param ds: instância do data store
    :param ano: ano de processamento da base
    :return: datamart de escola com os dados de alunos e matriculas
    """
    # carrega o depara de turma e escola
    turma_escola = ds.carrega_como_objeto(
        Documento(ds, dict(CatalogoAquisicao.TURMA)),
        como_df=True,
        filters=[("ANO", "=", ano)],
        columns=["ID_TURMA", "ID_ESCOLA"],
    )

    dados = list()
    for regiao in tqdm(["CO", "NORDESTE", "NORTE", "SUDESTE", "SUL"]):
        # carrega os dados de aluno
        aluno = ds.carrega_como_objeto(
            Documento(ds, dict(CatalogoAquisicao.ALUNO)),
            como_df=True,
            filters=[("ANO", "=", ano), ("REGIAO", "=", regiao)],
        )
        matricula = ds.carrega_como_objeto(
            Documento(ds, dict(CatalogoAquisicao.MATRICULA)),
            como_df=True,
            filters=[("ANO", "=", ano), ("REGIAO", "=", regiao)],
        )

        # adiciona a informação de escola a base de alunos
        matricula = matricula.merge(turma_escola)
        aluno = aluno.merge(matricula[["ID_ALUNO", "ID_ESCOLA"]].drop_duplicates())

        # soma todos os alunos e matriculas
        res = aluno.groupby(["ID_ESCOLA"]).agg({"ID_ALUNO": "count"}).reset_index()
        res.columns = ["ID_ESCOLA", "QT_ALUNOS"]
        res = res.merge(
            matricula.groupby(["ID_ESCOLA"])
            .agg({"ID_MATRICULA": "nunique"})
            .reset_index()
            .rename(columns={"ID_MATRICULA": "QT_MATRICULAS"}),
            how="left",
        )

        # processa as colunas IN_
        res = res.merge(
            fn.processa_coluna_in(aluno, "ID_ESCOLA", "ID_ALUNO", "ALUNO", perc=True),
            how="left",
        )

        # processa as colunas de transporte
        res = res.merge(
            fn.processa_coluna_in(
                df=matricula[
                    ["ID_ALUNO", "ID_ESCOLA"]
                    + [c for c in matricula.columns if c.startswith("IN_TRANSP")]
                ].drop_duplicates(),
                id_col="ID_ESCOLA",
                val_col="ID_ALUNO",
                prefixo="ALUNO",
                perc=True,
            ),
            how="left",
        )

        # processa as colunas de matriculas AEE
        res = res.merge(
            fn.processa_coluna_in(
                df=matricula[
                    ["ID_MATRICULA", "ID_ESCOLA"]
                    + [c for c in matricula.columns if c.startswith("IN_AEE_")]
                ].drop_duplicates(),
                id_col="ID_ESCOLA",
                val_col="ID_MATRICULA",
                prefixo="MATRICULA",
                perc=True,
            ),
            how="left",
        )

        # processa as colunas TP
        for (tp_col, pf, df) in [
            ("TP_SEXO", "ALUNO_SEXO", aluno),
            ("TP_COR_RACA", "ALUNO_COR", aluno),
            ("TP_NACIONALIDADE", "ALUNO_NASC", aluno),
            ("TP_ZONA_RESIDENCIAL", "ALUNO_ZONA", aluno),
            ("TP_LOCAL_RESID_DIFERENCIADA", "ALUNO_LDIF", aluno),
            ("TP_INGRESSO_FEDERAIS", "ALUNO_INGRESSO", aluno),
            ("TP_RESPONSAVEL_TRANSPORTE", "RESP_TRANSP", matricula),
        ]:
            res = res.merge(
                fn.processa_coluna_tp(
                    df, "ID_ESCOLA", tp_col, "ID_ALUNO", pf, recriar=False
                ),
                on="ID_ESCOLA",
                how="left",
            )

        # verifica docentes que estão em municipios diferentes da escola
        if aluno["CO_MUNICIPIO_END"].count() > 0:
            res = res.merge(
                aluno[["ID_ALUNO", "ID_ESCOLA", "CO_MUNICIPIO_END"]]
                .merge(dm[["ID_ESCOLA", "CO_MUNICIPIO"]], how="left")
                .assign(
                    QT_ALUNO_MUN_DIF=lambda f: (
                        (f["CO_MUNICIPIO_END"] != f["CO_MUNICIPIO"])
                        & (f["CO_MUNICIPIO_END"].notnull())
                    ).astype("int")
                )
                .groupby(["ID_ESCOLA"])["QT_ALUNO_MUN_DIF"]
                .sum()
                .reset_index(),
                how="left",
            )
        else:
            res["QT_ALUNO_MUN_DIF"] = np.nan

        # gera a dispersão de idade
        res = res.merge(
            fn.processa_coluna_qt_nu(
                df=aluno,
                id_col="ID_ESCOLA",
                prefixo="ALUNO",
                metricas=("min", "q1", "mean", "median", "q3", "max"),
            ),
            how="left",
        )

        # adiciona a lista de dados
        dados.append(res)

    # concatena os dados
    res = pd.concat(dados)

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
        Documento(ds, dict(CatalogoAquisicao.IDEB)),
        como_df=True,
        filters=[("ANO", "=", ano)],
    ).drop(columns="ANO")

    return dm.merge(ideb, how="left")


def gera_metricas_adicionais(dm: pd.DataFrame) -> pd.DataFrame:
    """
    Gera métricas cruzando dados da base de escola com as
    demais informações do censo

    :param dm: datamart em seu estado atual
    :return: datamart com métricas adicionais
    """
    return dm.assign(
        NU_COMP_PORTATIL_ALUNO=lambda f: f["QT_COMP_PORTATIL_ALUNO"] / f["QT_ALUNOS"],
        NU_DESKTOP_TOTAL_POR_ALUNO=lambda f: f["QT_DESKTOP"] / f["QT_ALUNOS"],
        NU_DESKTOP_ADM_POR_ALUNO=lambda f: f["QT_DESKTOP_ADM"] / f["QT_ALUNOS"],
        NU_DESKTOP_ALUNO_POR_ALUNO=lambda f: f["QT_DESKTOP_ALUNO"] / f["QT_ALUNOS"],
        NU_TABLET_POR_ALUNO=lambda f: f["QT_TABLET_ALUNO"] / f["QT_ALUNOS"],
        NU_EQUIP_COPIADORA_POR_ALUNO=lambda f: f["QT_EQUIP_COPIADORA"] / f["QT_ALUNOS"],
        NU_EQUIP_DVD_POR_ALUNO=lambda f: f["QT_EQUIP_DVD"] / f["QT_ALUNOS"],
        NU_EQUIP_FAX_POR_ALUNO=lambda f: f["QT_EQUIP_FAX"] / f["QT_ALUNOS"],
        NU_EQUIP_FOTO_POR_ALUNO=lambda f: f["QT_EQUIP_FOTO"] / f["QT_ALUNOS"],
        NU_EQUIP_IMPRESSORA_POR_ALUNO=lambda f: f["QT_EQUIP_IMPRESSORA"]
        / f["QT_ALUNOS"],
        NU_EQUIP_IMPRESSORA_MULT_POR_ALUNO=lambda f: f["QT_EQUIP_IMPRESSORA_MULT"]
        / f["QT_ALUNOS"],
        NU_EQUIP_LOUSA_DIGITAL_POR_ALUNO=lambda f: f["QT_EQUIP_LOUSA_DIGITAL"]
        / f["QT_ALUNOS"],
        NU_EQUIP_MULTIMIDIA_POR_ALUNO=lambda f: f["QT_EQUIP_MULTIMIDIA"]
        / f["QT_ALUNOS"],
        NU_EQUIP_PARABOLICA_POR_ALUNO=lambda f: f["QT_EQUIP_PARABOLICA"]
        / f["QT_ALUNOS"],
        NU_EQUIP_RETRO_POR_ALUNO=lambda f: f["QT_EQUIP_RETRO"] / f["QT_ALUNOS"],
        NU_EQUIP_SOM_POR_ALUNO=lambda f: f["QT_EQUIP_SOM"] / f["QT_ALUNOS"],
        NU_EQUIP_TV_POR_ALUNO=lambda f: f["QT_EQUIP_TV"] / f["QT_ALUNOS"],
        NU_EQUIP_VIDEOCASSETE_POR_ALUNO=lambda f: f["QT_EQUIP_VIDEOCASSETE"]
        / f["QT_ALUNOS"],
        NU_FUNCIONARIOS_POR_ALUNO=lambda f: f["QT_FUNCIONARIOS"] / f["QT_ALUNOS"],
        NU_SALAS_POR_ALUNO=lambda f: f["QT_SALAS_EXISTENTES"] / f["QT_ALUNOS"],
        NU_SALAS_UTILIZADAS_POR_ALUNO=lambda f: f["QT_SALAS_UTILIZADAS"]
        / f["QT_ALUNOS"],
        NU_SALAS_UTILIZADAS_ACESSIVEIS_POR_ALUNO=lambda f: f[
            "QT_SALAS_UTILIZADAS_ACESSIVEIS"
        ]
        / f["QT_ALUNOS"],
        NU_SALAS_UTILIZADAS_DENTRO_POR_ALUNO=lambda f: f["QT_SALAS_UTILIZADAS_DENTRO"]
        / f["QT_ALUNOS"],
        NU_SALAS_UTILIZADAS_FORA_POR_ALUNO=lambda f: f["QT_SALAS_UTILIZADAS_FORA"]
        / f["QT_ALUNOS"],
        NU_SALAS_UTILIZA_CLIMATIZADAS_POR_ALUNO=lambda f: f[
            "QT_SALAS_UTILIZA_CLIMATIZADAS"
        ]
        / f["QT_ALUNOS"],
        NU_ALUNO_POR_TURMA=lambda f: f["QT_MATRICULAS"] / f["QT_TURMAS"],
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
    logger = logging.getLogger(__name__)

    # carrega e processa as bases de dados
    logger.info("Carregando base de escola")
    dm = processa_censo_escola(ds, ano)

    logger.info("Adicionando dados de turmas")
    dm = processa_turmas(dm, ds, ano)

    logger.info("Adicionando dados de docentes")
    dm = processa_docentes(dm, ds, ano)

    logger.info("Adicionando dados de gestor")
    dm = processa_gestor(dm, ds, ano)

    logger.info("Adicionando dados de matricula")
    dm = processa_matricula(dm, ds, ano)

    logger.info("Adicionando dados do IDEB")
    dm = processa_ideb(dm, ds, ano)

    logger.info("Gerando métricas adicionais")
    dm = gera_metricas_adicionais(dm)

    logger.info("Remove float16")
    for c in dm:
        if dm[c].dtype == "float16":
            dm[c] = dm[c].astype("float32")

    logger.info("Exportando datamart")
    doc = Documento(ds, referencia=dict(CatalogoDatamart.ESCOLA), data=dm)
    doc.pasta = f"{doc.nome}/ANO={ano}"
    doc.nome = f"{ano}.parquet"
    doc.data.drop(columns=["ANO"], inplace=True)
    ds.salva_documento(doc)
