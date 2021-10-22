"""
Catalogo com a identificação de documentos que podem ser
compartilhados num ambiente de DataStore
"""
from dataclasses import dataclass

from frozendict import frozendict

from src.configs import COLECAO_AQUISCAO


@dataclass
class CatalogoAquisicao:
    """
    Catalogo de dados externos processados pelo pacote de
    aquisição que são colocados no DataStore
    """

    ESCOLA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "escola.parquet",
        }
    )

    GESTOR = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "gestor.parquet",
        }
    )

    GESTOR_ESCOLA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "depara_gestor_escola.parquet",
        }
    )

    TURMA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "turma.parquet",
        }
    )

    DOCENTE = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "docente.parquet",
        }
    )

    DOCENTE_TURMA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "depara_docente_turma.parquet",
        }
    )

    ALUNO = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "aluno.parquet",
        }
    )

    MATRICULA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "matricula.parquet",
        }
    )

    IDEB = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "ideb.parquet",
        }
    )


@dataclass
class CatalogoInfo:
    """
    Catalogo de dados internos que são adicionados manualmente
    a pasta 'info' como parte da ferramenta
    """

    CONFIG_AQUIS_ESCOLA = frozendict(
        {"colecao": "info", "nome": "aquis_censo_escola", "tipo": "yml"}
    )
