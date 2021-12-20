import logging
from pathlib import Path

import papermill as pm
import traitlets.config
from jupyter_contrib_nbextensions.nbconvert_support import TocExporter
from nbconvert.writers import FilesWriter

from src.relatorio.configs import CAMINHO_NOTEBOOK
from src.configs import CAMINHO_CODIGO


def relatorio_quanti_html(var: str, ano: int, env: str, pasta_saida: Path) -> Path:
    """
    Gera um relatório que realiza a análise dos dados quantitativos
    de uma determinada variável selecionada

    :param var: nome da variável a ser exportada
    :param ano: ano de avaliação dos dados
    :param env: ambiente para o data store
    :param pasta_saida: caminho para pasta de saída do relatório
    :return: caminho para o notebook gerado
    """
    logger = logging.getLogger(__name__)

    # configura a exportação
    c = traitlets.config.Config()
    c.CodeFoldingPreprocessor.remove_folded_code = True

    # executa o notebook
    output_notebook = pasta_saida / f"{ano}-relatorio-{var.lower()}.ipynb"
    logger.info(f"Gerando notebook {output_notebook}")
    pm.execute_notebook(
        str(CAMINHO_NOTEBOOK / "relatorio-quantitativo-template.ipynb"),
        str(output_notebook),
        parameters=dict(var=var, ano=ano, env=env, codigo=str(CAMINHO_CODIGO)),
    )

    logger.info(f"Exportando {output_notebook} como HTML")
    out, resources = TocExporter(config=c).from_filename(str(output_notebook))
    wtr = FilesWriter()
    wtr.write(out, resources, output_notebook.stem)

    return output_notebook
