import click

import src.configs as conf_geral
from src.aquisicao.opcoes import EnumETL
from src.aquisicao.opcoes import executa_etl
from src.utils.logs import configura_logs
from src.io.data_store import DataStore


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
    type=click.Choice([s.value for s in EnumETL]),
    help="Nome do ETL a ser executado",
)
@click.option(
    "--env",
    default=conf_geral.ENV_DS,
    help="String com caminho para pasta de entrada",
)
@click.option(
    "--criar_caminho", default=True, help="Flag indicando se devemos criar os caminhos"
)
def processa_dado(etl: str, env: str, criar_caminho: bool) -> None:
    """
    Executa o pipeline de ETL de uma determinada fonte

    :param etl: nome do ETL a ser executado
    :param env: ambiente do data store
    :param criar_caminho: flag indicando se devemos criar os caminhos
    """
    configura_logs()
    ds = DataStore(env)
    executa_etl(etl, ds, criar_caminho)


if __name__ == "__main__":
    cli()
