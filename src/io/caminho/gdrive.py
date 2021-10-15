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
    temp_dir: tempfile.TemporaryDirectory
    path: Path

    def __init__(self, drive: CaminhoGDrive, filename: str, mode: str) -> None:
        self.cdrive = drive
        self.temp_dir = tempfile.TemporaryDirectory()
        self.path = Path(self.temp_dir.name)
        super(GDriveIO, self).__init__(file=str(self.path / filename), mode=mode)

    def close(self) -> None:
        super().close()
        self.cdrive.upload_conteudo(self.name)
        self.temp_dir.cleanup()


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
        except UnboundLocalError:
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
        return GDriveIO(self, filename=nome_arq, mode="wb")

    def download_conteudo(
        self, nome_conteudo: str, pasta: typing.Union[str, Path]
    ) -> None:
        """
        Realiza o download de um determinado conteúdo para uma pasta no disco

        :param nome_conteudo: nome do conteúdo a ser baixado
        :param pasta: pasta de download
        """
        if self.verifica_se_arquivo(nome_conteudo):
            file = self._obtem_conteudo(nome_conteudo)
            file.GetContentFile(str(Path(pasta) / nome_conteudo))
        else:
            sub = Path(pasta) / nome_conteudo
            sub.mkdir()
            cam = self.__class__(self.obtem_caminho(nome_conteudo))
            for cont in cam.lista_conteudo():
                cam.download_conteudo(cont, sub)

    def upload_conteudo(
        self, nome_conteudo: str, pasta: typing.Union[str, Path]
    ) -> None:
        """
        Realiza o upload de um conteúdo ao drive

        :param nome_conteudo: nome do conteúdo a ser baixado
        :param pasta: pasta onde o arquivo está contido
        """
        pasta = Path(pasta)
        if os.path.isfile(pasta / nome_conteudo):
            file = self._obtem_conteudo(nome_conteudo)
            if file is None:
                file = self.drive.CreateFile(
                    {"parents": [{"id": self._c_id}], "title": nome_conteudo}
                )
            file.SetContentFile(pasta / nome_conteudo)
            file.Upload()
        else:
            for path, directories, files in os.walk(pasta):
                rel = path.replace(str(pasta), "").replace(os.sep, "/")[1:]
                for dir in directories:
                    self.__class__(
                        self.obtem_caminho(rel + "/" + dir), criar_caminho=True
                    )
                for file in files:
                    cam = self.__class__(self.obtem_caminho(rel))
                    cam.upload_conteudo(file, path)

    def from_dir_download(
        self,
        nome_arq: str,
        extensoes: typing.List[str],
        func: typing.Callable,
        **kwargs: typing.Any,
    ) -> typing.Any:
        """
        Para métodos de leitura na qual necessita-se de múltiplos arquivos,
        eles podem ser passados para este método como uma função que irá
        gerar um diretório temporário local, fazer o donwload dos arquivos
        necessários e aplicar a função de leitura

        :param nome_arq: nome do arquivo a ser lido
        :param extensoes: lista de extensões de arquivo que são aceitas na leitura
        :param func: função de leitura dos dados
        :param kwargs: argumentos de escrita para serem passados para função
        """
        arqs = [
            arq
            for arq in self.lista_conteudo()
            if os.path.splitext(arq)[-1][1:] in extensoes
            and os.path.splitext(arq)[0] == os.path.splitext(nome_arq)[0]
        ]
        with tempfile.TemporaryDirectory() as tmp:
            for a in arqs:
                self.download_conteudo(a, tmp)
            return func(
                str(Path(tmp) / nome_arq),
                **obtem_argumentos_objeto(func, kwargs),
            )

    def to_dir_upload(
        self,
        dados: typing.Any,
        func: typing.Callable,
        nome_arq: str,
        **kwargs: typing.Any,
    ) -> None:
        """
        Para métodos de exportação na qual são gerados múltiplos arquivos,
        eles podem ser passados para este métodos como uma função que irá
        gerar um diretório temporário local para exportação e, depois, realizará
        o upload dos dados para o caminho

        :param dados: data frame a ser exportado
        :param func: função de escrita dos dados
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            func(
                dados,
                str(tmp / nome_arq),
                **obtem_argumentos_objeto(func, kwargs),
            )
            for cont in os.listdir(tmp):
                self.upload_conteudo(cont, tmp)

    def read_parquet(self, nome_arq: str, **kwargs: typing.Any) -> pd.DataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função pandas
        :return: data frame com objeto carregado
        """
        if self.verifica_se_arquivo(nome_arq):
            return self.read_df(nome_arq, pd.read_parquet, **kwargs)
        else:
            return self.from_dir_download(nome_arq, ["parquet"], pd.read_parquet, **kwargs)

    def gpd_read_shape(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        if obtem_extencao(nome_arq) != "shp":
            return self.gpd_read_file(nome_arq, **kwargs)
        else:
            return self.from_dir_download(
                nome_arq, EXTENSOES_SHAPE, gpd.read_file, **kwargs
            )

    def gpd_read_file(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        ext = obtem_extencao(nome_arq)
        with tempfile.TemporaryDirectory() as tmp:
            if kwargs.get("ext") == "zip" or ext == "zip":
                self.download_conteudo(nome_arq, tmp)
                file = "zip://" + str(Path(tmp) / nome_arq)
            else:
                file = self.buffer_para_arquivo(nome_arq)
            return gpd.read_file(file, **obtem_argumentos_objeto(gpd.read_file, kwargs))

    def to_parquet(
        self, dados: pd.DataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        if kwargs.get("partition_cols"):
            if nome_arq in self.lista_conteudo():
                self._apaga_conteudo(nome_arq)
            self.to_dir_upload(dados, pd.DataFrame.to_parquet, nome_arq, **kwargs)
        else:
            self.write_df(dados, pd.DataFrame.to_parquet, nome_arq, **kwargs)

    def gpd_to_file(
        self, dados: gpd.GeoDataFrame, nome_arq: str, **kwargs: typing.Any
    ) -> None:
        """
        Escreve o geo data frame para o arquivo dentro do caminho selecionado

        :param dados: data frame a ser exportado
        :param nome_arq: nome do arquivo a ser escrito
        :param kwargs: argumentos de escrita para serem passados para função
        """
        if obtem_extencao(nome_arq) != "shp":
            self.write_df(dados, gpd.GeoDataFrame.to_file, nome_arq, **kwargs)
        else:
            self.to_dir_upload(dados, gpd.GeoDataFrame.to_file, nome_arq, **kwargs)
