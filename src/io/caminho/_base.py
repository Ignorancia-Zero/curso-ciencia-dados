from __future__ import annotations

import abc
import typing

import geopandas as gpd
import pandas as pd

import src.io.escreve_dados as escreve_dados
import src.io.le_dados as le_dados


class _CaminhoBase(abc.ABC):
    """
    Um objeto caminho contém um conjunto de funções que permite
    gerenciar arquivos e diretórios contidos dentro deste repositório
    de dados, que pode ser local ou remoto
    """

    _caminho: str

    def __init__(self, caminho: str, criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho

        :param caminho: string com o caminho desejado
        :param criar_caminho: flag se o caminho deve ser criado
        """
        self._caminho = caminho
        if criar_caminho:
            self.cria_caminho()

    @abc.abstractmethod
    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def obtem_caminho(self, destino: typing.Union[str, typing.List[str]]) -> str:
        """
        Obtém uma string com o caminho completo para o destino passado
        que pode ser uma string com um nome de arquivo ou outra pasta,
        ou uma lista de sub-diretórios e um arquivo final

        :param destino: lista ou string de pastas ao destino final
        :return: string com caminho completo para destino
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto

        :param apaga_conteudo: flag se devemos apagar o diretório mesmo que
        ele tenha algum conteúdo
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado conteúdo contido dentro
        do caminho é um arquivo

        :param nome_conteudo: nome do conteúdo a ser verificado
        :return: True se for um arquivo
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def _copia_conteudo_mesmo_caminho(
        self, nome_conteudo: str, caminho_destino: _CaminhoBase
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
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
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def buffer_para_arquivo(self, nome_arq: str) -> typing.Any:
        """
        Gera um buffer de acesso para um conteúdo no caminho

        :param nome_arq: nome do arquivo a ser carregado
        :return: conteúdo baixado
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
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
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def buffer_para_escrita(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer para upload de dados para o caminho

        :param nome_arq: nome do arquivo a ser salvo
        :return: buffer para upload do conteúdo
        """
        raise NotImplementedError("É preciso implementar o método")

    def apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto

        :param apaga_conteudo: flag se devemos apagar o diretório mesmo que
        ele tenha algum conteúdo
        """
        if len(self.lista_conteudo()) > 0 and not apaga_conteudo:
            raise PermissionError("O conteúdo do diretório não está vazio")
        else:
            self._apaga_caminho()

    def renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        if nome_origem not in self.lista_conteudo():
            raise FileNotFoundError(
                f"{nome_origem} não está contido em {self._caminho}"
            )
        self._renomeia_conteudo(nome_origem, nome_destino)

    def _copia_conteudo_caminhos_distintos(
        self, nome_conteudo: str, caminho_destino: _CaminhoBase
    ) -> None:
        """
        Realiza a cópia de um determinado conteúdo dentro deste caminho para
        um caminho distinto que seja de uma classe diferente deste objeto

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        if self.verifica_se_arquivo(nome_conteudo):
            dados = self.buffer_para_arquivo(nome_conteudo).read()
            with self.buffer_para_escrita(nome_conteudo) as buffer:
                buffer.write(dados)
        else:
            caminho_origem = self.__class__(
                caminho=self.obtem_caminho(nome_conteudo),
                criar_caminho=False,
            )
            caminho_destino = caminho_destino.__class__(
                caminho=caminho_destino.obtem_caminho(nome_conteudo),
                criar_caminho=True,
            )
            for cont in caminho_origem.lista_conteudo():
                caminho_origem._copia_conteudo_caminhos_distintos(cont, caminho_destino)

    def copia_conteudo(self, nome_conteudo: str, caminho_destino: _CaminhoBase) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        if nome_conteudo not in self.lista_conteudo():
            raise FileNotFoundError(
                f"{nome_conteudo} não está contido em {self._caminho}"
            )
        if isinstance(caminho_destino, self.__class__):
            self._copia_conteudo_mesmo_caminho(nome_conteudo, caminho_destino)
        else:
            self._copia_conteudo_caminhos_distintos(nome_conteudo, caminho_destino)

    def apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        if nome_conteudo not in self.lista_conteudo():
            raise FileNotFoundError(
                f"{nome_conteudo} não está contido em {self._caminho}"
            )
        self._apaga_conteudo(nome_conteudo)

    def carrega_arquivo(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Carrega o arquivo contido no caminho

        :param nome_arquivo: nome do arquivo a ser carregado
        :param kwargs: argumentos específicos para a função de carregamento
        """
        raise NotImplementedError

    def salva_arquivo(
        self, dados: typing.Any, nome_arquivo: str, **kwargs: typing.Any
    ) -> None:
        """
        Faz o upload de um determinado conteúdo para o caminho

        :param dados: bytes, data frame, string, etc. a ser salvo
        :param nome_arquivo: nome do arquivo a ser salvo
        :param kwargs: argumentos específicos para a função de salvamento
        """
        raise NotImplementedError

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

    def gpd_read_shape(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        return self.gpd_read_file(nome_arq, **kwargs)

    def gpd_read_file(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        return self.read_df(nome_arq, gpd.read_file, **kwargs)

    def load_yaml(self, nome_arq: str, **kwargs: typing.Any) -> dict:
        """
        Carrega o arquivo yaml como um dicionário

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: dicionário com dados yaml
        """
        return le_dados.load_yaml(self.buffer_para_arquivo(nome_arq))

    def load_json(self, nome_arq: str, **kwargs: typing.Any) -> dict:
        """
        Carrega o arquivo json como um dicionário

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: dicionário com dados json
        """
        return le_dados.load_json(self.buffer_para_arquivo(nome_arq))

    def load_pickle(self, nome_arq: str, **kwargs: typing.Any) -> list:
        """
        Carrega os objetos armazenados num arquivo pickle

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: lista de objetos serializados
        """
        return le_dados.load_pickle(self.buffer_para_arquivo(nome_arq))

    def load_txt(self, nome_arq: str, **kwargs: typing.Any) -> str:
        """
        Carrega os objetos armazenados num arquivo de texto

        :param nome_arq: nome do arquivo a ser carregado
        :param func: função pandas de carregamento
        :param kwargs: argumentos de carregamento para serem passados para função
        :return: texto carregado
        """
        encoding: str = kwargs["encoding"] if "encoding" in kwargs else "utf-8"
        return self.buffer_para_arquivo(nome_arq).read().decode(encoding)

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
        self.write_df(dados, pd.DataFrame.to_xml, nome_arq, **kwargs)  # type: ignore

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
        if kwargs["ext"] == "geojson":
            kwargs["driver"] = "GeoJSON"
        elif kwargs["ext"] == "topojson":
            kwargs["driver"] = "TopoJSON"
        elif kwargs["ext"] == "json":
            if not kwargs.get("driver"):
                kwargs["driver"] = "GeoJSON"
        self.write_df(dados, gpd.GeoDataFrame.to_file, nome_arq, **kwargs)

    def save_yaml(self, dados: dict, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o dicionário para o arquivo dentro do caminho selecionado

        :param dados: dicionário a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        buffer = self.buffer_para_escrita(nome_arq)
        escreve_dados.save_yaml(dados, buffer)
        buffer.close()

    def save_json(self, dados: dict, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o dicionário para o arquivo dentro do caminho selecionado

        :param dados: dicionário a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        buffer = self.buffer_para_escrita(nome_arq)
        escreve_dados.save_json(dados, buffer)
        buffer.close()

    def save_pickle(
        self, dados: typing.Any, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o objeto para o arquivo dentro do caminho selecionado

        :param dados: objeto python a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        buffer = self.buffer_para_escrita(nome_arq)
        escreve_dados.save_pickle(dados, buffer)
        buffer.close()

    def save_txt(self, dados: str, nome_arq: str, **kwargs: typing.Any) -> None:
        """
        Escreve o texto para o arquivo dentro do caminho selecionado

        :param dados: texto a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        buffer = self.buffer_para_escrita(nome_arq)
        encoding: str = kwargs["encoding"] if "encoding" in kwargs else "utf-8"
        buffer.write(dados.encode(encoding))
        buffer.close()
