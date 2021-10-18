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

    CENSO_ESCOLA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "censo_escola.parquet",
        }
    )

    CENSO_GESTOR = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "censo_gestor.parquet",
        }
    )

    CENSO_TURMA = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "censo_turma.parquet",
        }
    )

    CENSO_DOCENTE = frozendict(
        {
            "colecao": COLECAO_AQUISCAO,
            "nome": "censo_docente.parquet",
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
