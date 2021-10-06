"""
Catalogo com a identificação de documentos que podem ser
compartilhados num ambiente de DataStore
"""
from dataclasses import dataclass

from frozendict import frozendict


@dataclass
class CatalogoAquisicao:
    """
    Catalogo de dados externos processados pelo pacote de
    aquisição que são colocados no DataStore
    """

    ESCOLA_TEMP = frozendict(
        {
            "colecao": "aquisicao",
            "nome": "escola_temp",
            "tipo": "parquet",
        }
    )

    ESCOLA_ATEMP = frozendict(
        {
            "colecao": "aquisicao",
            "nome": "escola_atemp",
            "tipo": "parquet",
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