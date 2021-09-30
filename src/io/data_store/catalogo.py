from dataclasses import dataclass
from frozendict import frozendict


@dataclass
class Catalogo:
    escola_temp: frozendict = frozendict({
        "colecao": "aquisicao",
        "nome": "escola_temp",
        "tipo": "parquet",
        "pasta": "censo_escolar",
    })