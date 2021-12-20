import os
import tempfile
import typing
from pathlib import Path

from src.configs import COLECAO_RELATORIO
from src.io.caminho import CaminhoLocal
from src.io.data_store import DataStore, Documento


def gera_notebook(
    ds: DataStore, params: typing.Dict, func: typing.Callable, pasta: str
) -> None:
    """
    Executa um notebook que deve ser convertido para o formato especificado

    :param ds: instância do data store
    :param params: parâmetros de geração do notebook
    :param func: função de geração do notebook
    :param pasta: nome da pasta de saídas
    """
    # gera uma pasta temporária local
    with tempfile.TemporaryDirectory() as tdir:
        # executa o notebook para a pasta
        params["env"] = ds._env
        params["pasta_saida"] = Path(tdir)
        notebook = func(**params)
        nome = os.path.basename(str(notebook)).split(".")[0] + ".html"

        # sobe o notebook para o local no data store
        cam_orig = CaminhoLocal(tdir)
        cam_dest = ds.gera_caminho(
            documento=Documento(
                ds, referencia=dict(nome=nome, colecao=COLECAO_RELATORIO, pasta=pasta)
            )
        )
        cam_dest.cria_caminho()
        cam_orig.copia_conteudo(nome, cam_dest)
