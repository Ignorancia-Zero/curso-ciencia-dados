from __future__ import annotations

import os
import tempfile
import typing
from abc import ABC
from io import FileIO
from pathlib import Path

import geopandas as gpd
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.drive import GoogleDriveFile

from src.io.configs import EXTENSOES_SHAPE
from src.utils.info import CAMINHO_INFO
from src.utils.interno import obtem_argumentos_objeto
from src.utils.interno import obtem_extencao
from ._base import _CaminhoBase


class GDriveIO(FileIO):
    """
    Objeto responsável por agir como um buffer temporário na memória
    que ao fechar realiza o upload dos dados para o Google Drive
    """

    cdrive: CaminhoGDrive

    def __init__(self, drive: CaminhoGDrive, file: str, mode: str) -> None:
        self.cdrive = drive
        super(GDriveIO, self).__init__(file=file, mode=mode)

    def close(self) -> None:
        super().close()
        title = os.path.basename(self.name)
        file = self.cdrive._obtem_conteudo(title)
        if file is None:
            file = self.cdrive.drive.CreateFile(
                {"parents": [{"id": self.cdrive._c_id}], "title": title}
            )
        file.SetContentFile(self.name)
        file.Upload()


class CaminhoGDrive(_CaminhoBase, ABC):
    """
    Objeto caminho que gerencia arquivo contidos em uma pasta do google drive

    O controle de todos os métodos é feito pelo objeto GoogleDrive da biblioteca
    pydrive2 (https://docs.iterative.ai/PyDrive2/)

    Para realizar o setup da API veja
    https://www.youtube.com/watch?v=9qHvQafgjY4&ab_channel=TutorFazeel
    """

    gauth: GoogleAuth
    drive: GoogleDrive
    _existe: bool
    _c_id: str
    temp_dir: tempfile.TemporaryDirectory

    def __init__(self, caminho: str = "", criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho local

        :param caminho: string com o caminho desejado dentro do bucket
        :param criar_caminho: flag se o caminho deve ser criado
        """
        # realiza a autenticação do serviço
        self.gauth = GoogleAuth(settings_file=CAMINHO_INFO / "gdrive_settings.yml")
        self.gauth.CommandLineAuth()

        # cria o cliente e o diretório temporário para download de arquivos
        self.drive = GoogleDrive(self.gauth)
        self.temp_dir = tempfile.TemporaryDirectory()

        # ajusta a string de caminho
        if caminho[-1] == "/":
            caminho = caminho[:-1]
        caminho = caminho[9:]

        # obtém o id do caminho utilizado
        self._existe = True
        self._obtem_id_caminho(caminho)

        super().__init__(caminho, criar_caminho)

    def _obtem_id_caminho(self, caminho: str) -> None:
        """
        Obtém o ID equivalente ao caminho passado para o google drive

        :param: hierarquia de pastas que define o caminho dentro do google drive
        """
        # obtém o ID do diretório
        self._c_id = "root"
        for pasta in caminho.split("/"):
            file_list = self.drive.ListFile(
                {"q": f"'{self._c_id}' in parents and trashed=false"}
            ).GetList()
            for file in file_list:
                if file["title"] == pasta:
                    self._c_id = file["id"]
                    break

        # cria uma flag informando se o caminho existe
        try:
            self._existe = file["title"] == pasta
        except ValueError:
            self._existe = caminho == ""

    def _cria_pasta(self, id_pai: str, pasta: str) -> str:
        """
        Cria um novo diretório dentro do ID pai

        :param id_pai: id interno do google drive do diretório pai
        :param pasta: nome da nova pasta a ser criada
        :return: id da nova pasta
        """
        folder = self.drive.CreateFile(
            {
                "parents": [{"id": id_pai}],
                "title": pasta,
                "mimeType": "application/vnd.google-apps.folder",
            }
        )
        folder.Upload()

        return folder["id"]

    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        pai = "root"

        # para cada pasta no caminho
        for pasta in self._caminho.split("/"):
            # obtém os ids do diretório percorrido
            novo_pai = [
                file["id"]
                for file in self.drive.ListFile(
                    {"q": f"'{pai}' in parents and trashed=false"}
                ).GetList()
                if file["title"] == pasta
            ]

            # cria o diretório pai caso o mesmo não exista
            if len(novo_pai) == 0:
                pai = self._cria_pasta(pai, pasta)
            else:
                pai = novo_pai[0]

        # salva os novos diretórios
        self._c_id = pai
        self._existe = True

    def obtem_caminho(self, destino: typing.Union[str, typing.List[str]]) -> str:
        """
        Obtém uma string com o caminho completo para o destino passado
        que pode ser uma string com um nome de arquivo ou outra pasta,
        ou uma lista de sub-diretórios e um arquivo final

        :param destino: lista ou string de pastas ao destino final
        :return: string com caminho completo para destino
        """
        if not isinstance(destino, str):
            destino = "/".join(destino)
        return f"gdrive://{self._caminho}/{destino}"

    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto

        :param apaga_conteudo: flag se devemos apagar o diretório mesmo que
        ele tenha algum conteúdo
        """
        if not self._existe:
            raise ValueError("O caminho não existe")
        folder = self.drive.CreateFile({"id": self._c_id})
        if len(self.lista_conteudo()) > 0 and not apaga_conteudo:
            raise ValueError(f"A pasta {self._caminho} não esta vazia")
        folder.Delete()

    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        return [
            file["title"]
            for file in self.drive.ListFile(
                {"q": f"'{self._c_id}' in parents and trashed=false"}
            ).GetList()
        ]

    def _obtem_conteudo(self, nome_conteudo: str) -> GoogleDriveFile:
        """
        Obtém o ID de um conteúdo selecionado

        :param nome_conteudo: título do conteúdo
        :return: string com ID do conteúdo
        """
        for file in self.drive.ListFile(
            {"q": f"'{self._c_id}' in parents and trashed=false"}
        ).GetList():
            if file["title"] == nome_conteudo:
                return file

    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado conteúdo contido dentro
        do caminho é um arquivo

        :param nome_conteudo: nome do conteúdo a ser verificado
        :return: True se for um arquivo
        """
        file = self._obtem_conteudo(nome_conteudo)
        return file["mimeType"] != "application/vnd.google-apps.folder"

    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        file = self._obtem_conteudo(nome_origem)
        file["title"] = nome_destino
        file.Upload()

    def _copia_conteudo_mesmo_caminho(
        self, nome_conteudo: str, caminho_destino: CaminhoGDrive
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        Fonte: https://stackoverflow.com/questions/43865016/python-copy-a-file-in-google-drive-into-a-specific-folder

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        file = self._obtem_conteudo(nome_conteudo)
        self.drive.auth.service.files().copy(
            fileId=file["id"],
            body={"parents": [{"id": caminho_destino._c_id}], "title": nome_conteudo},
        ).execute()

    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        file = self._obtem_conteudo(nome_conteudo)
        file.Delete()

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
            self.buffer_para_arquivo(nome_arq), **obtem_argumentos_objeto(func, kwargs)
        )

    def buffer_para_arquivo(self, nome_arq: str) -> typing.BinaryIO:
        """
        Carrega um conteúdo específico do bucket s3

        :param nome_arq: nome do arquivo a ser carregado
        :return: conteúdo baixado
        """
        return self._obtem_conteudo(nome_arq).GetContentIOBuffer()

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
        with self.buffer_para_escrita(nome_arq) as f:
            func(dados, f, **obtem_argumentos_objeto(func, kwargs))

    def buffer_para_escrita(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer para upload de dados para o caminho

        :param nome_arq: nome do arquivo a ser salvo
        :return: buffer para upload do conteúdo
        """
        return GDriveIO(self, file=str(Path(self.temp_dir.name) / nome_arq), mode="wb")

    def gpd_read_shape(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        if kwargs.get("ext") != "shp" and obtem_extencao(nome_arq) == "shp":
            return self.gpd_read_file(nome_arq, **kwargs)

        # lista os arquivos associados ao shapefile
        arqs = [
            arq
            for arq in self.lista_conteudo()
            if os.path.splitext(arq)[-1] in EXTENSOES_SHAPE
            and os.path.basename(arq) == os.path.basename(nome_arq)
        ]

        # realiza o download dos mesmos
        for a in arqs:
            file = self._obtem_conteudo(a)
            file.GetContentFile(Path(self.temp_dir.name) / file["title"])

        # le o conteúdo
        return self.gpd_read_file(str(Path(self.temp_dir.name) / nome_arq), **kwargs)

    def gpd_read_file(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        ext = obtem_extencao(nome_arq)
        if kwargs.get("ext") == "zip" or ext == "zip":
            file = self._obtem_conteudo(nome_arq)
            file.GetContentFile(Path(self.temp_dir.name) / file["id"])
            file = "zip://" + str(Path(self.temp_dir.name) / file["id"])
        else:
            file = self.buffer_para_arquivo(nome_arq)
        return gpd.read_file(file, **obtem_argumentos_objeto(gpd.read_file, kwargs))

    def gpd_to_file(
        self, dados: gpd.GeoDataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o geo data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        if kwargs.get("ext") != "shp" and obtem_extencao(nome_arq) == "shp":
            self.write_df(dados, gpd.GeoDataFrame.to_file, nome_arq, **kwargs)
        else:
            with tempfile.TemporaryDirectory(dir=self.temp_dir.name) as tmp:
                tmp = Path(tmp)
                dados.to_file(
                    str(tmp / nome_arq),
                    **obtem_argumentos_objeto(gpd.GeoDataFrame.to_file, kwargs),
                )
                for arq in os.listdir(tmp):
                    file = self._obtem_conteudo(arq)
                    if file is None:
                        file = self.drive.CreateFile(
                            {"parents": [{"id": self._c_id}], "title": arq}
                        )
                    file.SetContentFile(str(tmp / arq))
                    file.Upload()
