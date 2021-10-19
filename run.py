import click

import src.configs as conf_geral
from src.aquisicao.executa import executa_etl_inep
from src.aquisicao.opcoes import INEP_ETL
from src.io.data_store import DataStore
from src.utils.logs import configura_logs


@click.group()
def cli():
    pass


@cli.group()
def aquisicao():
    """
    Grupo de comandos que executam as funções de aquisição
    """
    pass


@aquisicao.command()
@click.option(
    "--etl",
    type=click.Choice([s.value for s in INEP_ETL]),
    help="Nome do ETL a ser executado",
)
@click.option(
    "--ano",
    type=click.STRING,
    default="ultimo",
    help="Ano dos dados a serem processados (pode ser int ou 'ultimo')",
)
@click.option(
    "--criar_caminho", default=True, help="Flag indicando se devemos criar os caminhos"
)
@click.option(
    "--reprocessar", is_flag=True, help="Flag indicando se devemos reprocessar a base"
)
@click.option(
    "--env",
    default=conf_geral.ENV_DS,
    help="String com caminho para pasta de entrada",
)
def processa_dado_inep(
    etl: str, ano: str, criar_caminho: bool, reprocessar: bool, env: str
) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param ano: Ano dos dados a serem processados (pode ser int ou 'ultimo')
    :param criar_caminho: flag indicando se devemos criar os caminhos
    :param reprocessar: flag indicando se devemos reprocessar a base
    :param env: ambiente do data store
    """
    configura_logs()
    ds = DataStore(env)
    executa_etl_inep(
        etl=etl, ds=ds, ano=ano, criar_caminho=criar_caminho, reprocessar=reprocessar
    )


if __name__ == "__main__":
    cli()
