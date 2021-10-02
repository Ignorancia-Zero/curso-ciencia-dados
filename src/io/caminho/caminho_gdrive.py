from __future__ import annotations

import os
import tempfile
import typing
from io import FileIO
from pathlib import Path

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.drive import GoogleDriveFile

from src.utils.info import CAMINHO_INFO
from .caminho_base import _CaminhoBase


class GDriveIO(FileIO):
    """
    Objeto responsável por agir como um buffer temporário na memória
    que ao fechar realiza o upload dos dados para o Google Drive
    """

    cdrive: CaminhoGDrive

    def __init__(self, drive: CaminhoGDrive, **kwargs: typing.Any) -> None:
        self.cdrive = drive
        super().__init__(**kwargs)

    def close(self) -> None:
        super().close()
        title = os.path.basename(self.name)
        file = self.cdrive._obtem_conteudo(title)
        if file is None:
            file = self.cdrive.drive.CreateFile(
                {
                    "parents": [{"id": self.cdrive._c_id}],
                    "title": title,
                }
            )
        file.SetContentFile(self.name)
        file.Upload()


class CaminhoGDrive(_CaminhoBase):
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
        return None

    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado arquivo contido dentro
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

    def _gera_buffer_carregar(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Carrega o arquivo contido no caminho

        :param nome_arquivo: nome do arquivo a ser carregado
        :param kwargs: argumentos específicos para a função de carregamento
        """
        file = self._obtem_conteudo(nome_arquivo)
        file.GetContentFile(Path(self.temp_dir.name) / file["id"])
        return open(Path(self.temp_dir.name) / file["id"], "rb")

    def _gera_buffer_salvar(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Gera um buffer para os dados a serem salvos em algum local que serão
        usados como parte do método de salvar

        :param nome_arquivo: nome do arquivo a ser salvo
        :param kwargs: argumentos específicos para a função de salvamento
        """
        return GDriveIO(
            self, file=str(Path(self.temp_dir.name) / nome_arquivo), mode="wb"
        )

    def __del__(self):
        self.temp_dir.cleanup()
