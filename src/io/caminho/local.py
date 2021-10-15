from __future__ import annotations

import os
import shutil
import typing
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.utils.interno import obtem_argumentos_objeto
from ._base import _CaminhoBase
from src.utils.interno import obtem_extencao


class CaminhoLocal(_CaminhoBase):
    """
    Objeto caminho que gerencia arquivo contidos no próprio disco
    rígido da máquina executando o código
    """

    caminho: Path

    def __init__(self, caminho: str, criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho local

        :param caminho: string com o caminho desejado
        :param criar_caminho: flag se o caminho deve ser criado
        """
        self.caminho = Path(caminho)
        super().__init__(caminho, criar_caminho)

    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        self.caminho.mkdir(parents=True, exist_ok=True)

    def obtem_caminho(self, destino: typing.Union[str, typing.List[str]]) -> str:
        """
        Obtém uma string com o caminho completo para o destino passado
        que pode ser uma string com um nome de arquivo ou outra pasta,
        ou uma lista de sub-diretórios e um arquivo final

        :param destino: lista ou string de pastas ao destino final
        :return: string com caminho completo para destino
        """
        if isinstance(destino, str):
            destino = [destino]
        return os.path.join(str(self.caminho), *destino)

    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto
        """
        if apaga_conteudo:
            shutil.rmtree(self.caminho)
        else:
            os.remove(str(self.caminho))

    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        return os.listdir(str(self.caminho))

    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado conteúdo contido dentro
        do caminho é um arquivo

        :param nome_conteudo: nome do conteúdo a ser verificado
        :return: True se for um arquivo
        """
        return os.path.isfile(self.obtem_caminho(nome_conteudo))

    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        os.rename(str(self.caminho / nome_origem), str(self.caminho / nome_destino))

    def _copia_conteudo_mesmo_caminho(
        self, nome_conteudo: str, caminho_destino: CaminhoLocal
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        if self.verifica_se_arquivo(nome_conteudo):
            shutil.copyfile(
                self.obtem_caminho(nome_conteudo),
                caminho_destino.obtem_caminho(nome_conteudo),
            )
        else:
            shutil.copytree(
                self.obtem_caminho(nome_conteudo),
                caminho_destino.obtem_caminho(nome_conteudo),
            )

    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        if self.verifica_se_arquivo(nome_conteudo):
            os.remove(self.obtem_caminho(nome_conteudo))
        else:
            shutil.rmtree(self.obtem_caminho(nome_conteudo))

    def read_df(
        self, nome_arq: str, func: typing.Callable, **kwargs: typing.Any
    ) -> typing.Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Lê um arquivo no pandas usando a função read adequada

        :param nome_arq: nome do arquivo a ser carregado
        :param func: função pandas de carregamento
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return func(
            self.obtem_caminho(nome_arq), **obtem_argumentos_objeto(func, kwargs)
        )

    def buffer_para_arquivo(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer de acesso para um conteúdo no caminho

        :param nome_arq: nome do arquivo a ser carregado
        :return: conteúdo baixado
        """
        return open(self.obtem_caminho(nome_arq), "rb")

    def write_df(
        self,
        dados: typing.Union[pd.DataFrame, gpd.GeoDataFrame],
        func: typing.Callable,
        nome_arq: str,
        **kwargs: typing.Any,
    ) -> None:
        """
        Salva o conteúdo de um data frame pandas usando a função adequada

        :param dados: data frame a ser exportado
        :param func: função de escrita dos dados
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        func(
            dados, self.obtem_caminho(nome_arq), **obtem_argumentos_objeto(func, kwargs)
        )

    def buffer_para_escrita(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer para upload de dados para o caminho

        :param nome_arq: nome do arquivo a ser salvo
        :return: buffer para upload do conteúdo
        """
        return open(self.obtem_caminho(nome_arq), "wb")

    def gpd_read_file(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        ext = obtem_extencao(nome_arq)
        cam = self.obtem_caminho(nome_arq)
        if kwargs.get("ext") == "zip" or ext == "zip":
            cam = "zip://" + cam
        return gpd.read_file(cam, **obtem_argumentos_objeto(gpd.read_file, kwargs))
