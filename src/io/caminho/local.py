from __future__ import annotations

import json
import os
import pickle
import shutil
import typing
from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

import src.io.le_dados as le_dados
from src.utils.interno import obtem_argumentos_objeto
from ._base import _CaminhoBase


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

    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto
        """
        if apaga_conteudo:
            shutil.rmtree(self.caminho)
        else:
            os.remove(str(self.caminho))

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

    def _gera_buffer_carregar(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Carrega o arquivo contido no caminho

        :param nome_arquivo: nome do arquivo a ser carregado
        :param kwargs: argumentos específicos para a função de carregamento
        """
        return open(self.obtem_caminho(nome_arquivo), "rb")

    def _gera_buffer_salvar(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Gera um buffer para os dados a serem salvos em algum local que serão
        usados como parte do método de salvar

        :param nome_arquivo: nome do arquivo a ser salvo
        :param kwargs: argumentos específicos para a função de salvamento
        """
        return open(self.obtem_caminho(nome_arquivo), "wb")

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

    def read_parquet(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_parquet, **kwargs)

    def read_feather(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_feather, **kwargs)

    def read_csv(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_csv, **kwargs)

    def read_hdf(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_hdf, **kwargs)

    def read_excel(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_excel, **kwargs)

    def read_html(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_html, **kwargs)

    def read_json(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_json, **kwargs)

    def read_xml(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_xml, **kwargs)

    def read_pickle(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, pd.read_pickle, **kwargs)

    def gpd_read_parquet(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, gpd.read_parquet, **kwargs)

    def gpd_read_feather(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, gpd.read_feather, **kwargs)

    def gpd_read_file(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        cam = self.obtem_caminho(nome_arq)
        if "zip" in kwargs:
            if kwargs["zip"]:
                cam = "zip://" + self.obtem_caminho(nome_arq)
        return gpd.read_file(cam, **obtem_argumentos_objeto(gpd.read_file, kwargs))

    def load_yaml(self, nome_arq: str, **kwargs: typing.Any) -> dict:
        """
        Carrega o arquivo yaml como um dicionário

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: dicionário com dados yaml
        """
        with open(self.obtem_caminho(nome_arq), "rb") as f:
            return le_dados.load_yaml(f)

    def load_json(self, nome_arq: str, **kwargs: typing.Any) -> dict:
        """
        Carrega o arquivo json como um dicionário

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: dicionário com dados json
        """
        with open(self.obtem_caminho(nome_arq), "rb") as f:
            return le_dados.load_json(f)

    def load_pickle(self, nome_arq: str, **kwargs: typing.Any) -> list:
        """
        Carrega os objetos armazenados num arquivo pickle

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: lista de objetos serializados
        """
        with open(self.obtem_caminho(nome_arq), "rb") as f:
            return le_dados.load_pickle(f)

    def load_txt(self, nome_arq: str, **kwargs: typing.Any) -> str:
        """
        Carrega os objetos armazenados num arquivo de texto

        :param nome_arq: nome do arquivo a ser carregado
        :param func: função pandas de carregamento
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: texto carregado
        """
        with open(self.obtem_caminho(nome_arq), "r") as f:
            return f.read()

    def write_df(
        self,
        dados: typing.Union[pd.DataFrame, gpd.GeoDataFrame],
        func: typing.Callable,
        nome_arq: str,
        **kwargs: typing.Any
    ) -> typing.Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Lê um arquivo no pandas usando a função read adequada

        :param dados: data frame a ser exportado
        :param func: função de escrita dos dados
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        return func(
            dados, self.obtem_caminho(nome_arq), **obtem_argumentos_objeto(func, kwargs)
        )

    def to_parquet(
        self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_parquet, nome_arq, **kwargs)

    def to_feather(
        self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_feather, nome_arq, **kwargs)

    def to_csv(self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_csv, nome_arq, **kwargs)

    def to_hdf(self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_hdf, nome_arq, **kwargs)

    def to_excel(
        self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_excel, nome_arq, **kwargs)

    def to_html(self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_html, nome_arq, **kwargs)

    def to_json(self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_json, nome_arq, **kwargs)

    def to_xml(self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_xml, nome_arq, **kwargs)

    def to_pickle(
        self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, pd.DataFrame.to_pickle, nome_arq, **kwargs)

    def gpd_to_parquet(
        self, dados: gpd.GeoDataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o geo data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        """
        self.write_df(dados, gpd.GeoDataFrame.to_parquet, nome_arq, **kwargs)

    def gpd_to_feather(
        self, dados: gpd.GeoDataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o geo data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, gpd.GeoDataFrame.to_feather, nome_arq, **kwargs)

    def gpd_to_file(
        self, dados: gpd.GeoDataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o geo data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        self.write_df(dados, gpd.GeoDataFrame.to_file, nome_arq, **kwargs)

    def save_yaml(self, dados: dict, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o dicionário para o arquivo dentro do caminho selecionado

        :param dados: dicionário a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        with open(self.obtem_caminho(nome_arq), "w") as f:
            yaml.dump(dados, f)

    def save_json(self, dados: dict, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o dicionário para o arquivo dentro do caminho selecionado

        :param dados: dicionário a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        with open(self.obtem_caminho(nome_arq), "w") as f:
            json.dump(dados, f)

    def save_pickle(
        self, dados: typing.Any, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o objeto para o arquivo dentro do caminho selecionado

        :param dados: objeto python a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        with open(self.obtem_caminho(nome_arq), "wb") as f:
            pickle.dump(dados, f)

    def save_txt(self, dados: str, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o texto para o arquivo dentro do caminho selecionado

        :param dados: texto a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        with open(self.obtem_caminho(nome_arq), "w") as f:
            f.write(dados)
