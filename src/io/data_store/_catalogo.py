"""
Catalogo com a identificação de documentos que podem ser
compartilhados num ambiente de DataStore
"""
from dataclasses import dataclass

from frozendict import frozendict

from src.configs import COLECAO_AQUISICAO
from src.configs import COLECAO_DATAMART


@dataclass
class CatalogoInfo:
    """
    Catalogo de dados internos que são adicionados manualmente
    a pasta 'info' como parte da ferramenta
    """

    ETAPA_ENSINO = frozendict(
        {"colecao": "__info__", "nome": "censo_escolar_etapa_ensino.xlsx"}
    )
    CURSOS = frozendict({"colecao": "__info__", "nome": "censo_escolar_cursos.xlsx"})
    COMPL_PEDAGOGICA = frozendict(
        {"colecao": "__info__", "nome": "censo_escolar_compl_pedagogica.xlsx"}
    )
    EDUC_PROF = frozendict(
        {"colecao": "__info__", "nome": "censo_escolar_educ_profissional.xlsx"}
    )


@dataclass
class CatalogoAquisicao:
    """
    Catalogo de dados externos processados pelo pacote de
    aquisição que são colocados no DataStore
    """

    ESCOLA = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "escola.parquet"})
    GESTOR = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "gestor.parquet"})
    GESTOR_ESCOLA = frozendict(
        {"colecao": COLECAO_AQUISICAO, "nome": "depara_gestor_escola.parquet"}
    )
    TURMA = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "turma.parquet"})
    DOCENTE = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "docente.parquet"})
    DOCENTE_TURMA = frozendict(
        {"colecao": COLECAO_AQUISICAO, "nome": "depara_docente_turma.parquet"}
    )
    ALUNO = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "aluno.parquet"})
    MATRICULA = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "matricula.parquet"})
    IDEB = frozendict({"colecao": COLECAO_AQUISICAO, "nome": "ideb.parquet"})


@dataclass
class CatalogoDatamart:
    """
    Catalogo com os datamarts por nível de granularidade
    """

    ESCOLA = frozendict({"colecao": COLECAO_DATAMART, "nome": "dm_escola.parquet"})
