from __future__ import annotations

import os
import sqlite3
import typing
from pathlib import Path

import pandas as pd

from ._base import _CaminhoBase


class CaminhoSQLite(_CaminhoBase):
    """
    Objeto caminho que gerencia arquivo contidos no próprio disco
    rígido da máquina executando o código
    """

    database: str
    _path: Path
    _connection: sqlite3.Connection
    _cursor: sqlite3.Cursor

    def __init__(self, caminho: str, criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho sql

        :param caminho: string com o caminho desejado (sqlite://caminho/para/database)
        :param criar_caminho: flag se o caminho deve ser criado
        """
        # remove a string de sql
        caminho = caminho[9:]

        # obtém o nome do database
        self.database = caminho.split("/")[-1] + ".db"

        # obtém o caminho para o database
        self._path = Path("/".join(caminho.split("/")[:-1]))

        # cria as variáveis adicionais
        self._connection = None
        self._cursor = None

        super().__init__(caminho, criar_caminho)

        # cria as conexões ao database caso isto ainda não tenha sido feito
        if os.path.exists(self._path / self.database):
            if self._connection is None:
                self.cria_caminho()

    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        self._connection = sqlite3.connect(self._path / self.database)
        self._cursor = self._connection.cursor()

    def obtem_caminho(self, destino: typing.Union[str, typing.List[str]]) -> str:
        """
        Obtém uma string com o caminho completo para o destino passado
        que pode ser uma string com um nome de arquivo ou outra pasta,
        ou uma lista de sub-diretórios e um arquivo final

        :param destino: lista ou string de pastas ao destino final
        :return: string com caminho completo para destino
        """
        if not isinstance(destino, str):
            raise ValueError(
                "O objeto caminho de sqllite só aceita caminhos que são strings simples"
            )
        return str(self._path / f"{self.database}::{destino}")

    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto
        """
        self._cursor.close()
        self._connection.close()
        os.remove(self._path / self.database)

    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        return self._cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()

    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado conteúdo contido dentro
        do caminho é um arquivo

        :param nome_conteudo: nome do conteúdo a ser verificado
        :return: True se for um arquivo
        """
        return True

    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        self._cursor.execute(f"ALTER TABLE `{nome_origem}` RENAME TO `{nome_destino}`")
        self._connection.commit()

    def _copia_conteudo_mesmo_caminho(
        self, nome_conteudo: str, caminho_destino: CaminhoSQL
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        df = self.carrega_arquivo(nome_conteudo)
        caminho_destino.salva_arquivo(df, nome_conteudo)

    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        self._cursor.execute(f"DROP table `{nome_conteudo}`")
        self._connection.commit()

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

    def buffer_para_arquivo(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer de acesso para um conteúdo no caminho

        :param nome_arq: nome do arquivo a ser carregado
        :return: conteúdo baixado
        """
        raise NotImplementedError("É preciso implementar o método")

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

    def buffer_para_escrita(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer para upload de dados para o caminho

        :param nome_arq: nome do arquivo a ser salvo
        :return: buffer para upload do conteúdo
        """
        raise NotImplementedError("É preciso implementar o método")

    def carrega_arquivo(self, nome_arquivo: str, **kwargs: typing.Any) -> typing.Any:
        """
        Carrega o arquivo contido no caminho

        :param nome_arquivo: nome do arquivo a ser carregado
        :param kwargs: argumentos específicos para a função de carregamento
        """
        return pd.read_sql_query(
            f"SELECT * FROM `{nome_arquivo}`", self._connection, **kwargs
        )

    def salva_arquivo(
        self, dados: typing.Any, nome_arquivo: str, **kwargs: typing.Any
    ) -> None:
        """
        Faz o upload de um determinado conteúdo para o caminho

        :param dados: bytes, data frame, string, etc. a ser salvo
        :param nome_arquivo: nome do arquivo a ser salvo
        :param kwargs: argumentos específicos para a função de salvamento
        """
        if not isinstance(dados, pd.DataFrame):
            raise ValueError("O caminho sql só aceita objetos data frame")
        dados.to_sql(nome_arquivo, self._connection, **kwargs)

    def __del__(self):
        self._cursor.close()
        self._connection.commit()
        self._connection.close()
