from __future__ import annotations

import os
import typing
from io import BytesIO

import boto3
import geopandas as gpd
import pandas as pd
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

from src.utils.interno import obtem_argumentos_objeto
from ._base import _CaminhoBase


class S3Buffer(BytesIO):
    """
    O S3Buffer gerará um buffer na memória utilizado para
    escrever arquivos que ao ser fechado fará o upload dos
    dados carregados para o bucket S3
    """

    client: BaseClient
    bucket: str
    key: str
    filename: str

    def __init__(
        self,
        client: BaseClient,
        bucket: str,
        key: str,
        filename: str,
        initial_bytes: bytes = None,
    ) -> None:
        self.client = client
        self.bucket = bucket
        self.key = key
        self.filename = filename
        super(S3Buffer, self).__init__(initial_bytes)

    def close(self) -> None:
        self.seek(0)
        self.client.put_object(
            Bucket=self.bucket, Body=self.read(), Key=f"{self.key}/{self.filename}"
        )
        self.close()


# TODO: É preciso testar esse objeto com uma instância S3 para garantir que está funcionando
class CaminhoS3(_CaminhoBase):
    """
    Objeto caminho que gerencia arquivo contidos em um bucket S3
    proprietário da Amazon

    O controle de todos os métodos é feito pelo cliente boto3 e assume que
    as credenciais da Amazon estejam configuradas

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
    """

    # limite de objetos da paginação AWS
    LIMITE_AWS: int = 1000

    bucket: str
    caminho: str
    client: BaseClient
    resource: ServiceResource

    def __init__(self, caminho: str = "", criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho local

        :param caminho: string com o caminho desejado dentro do bucket
        :param criar_caminho: flag se o caminho deve ser criado
        """
        # cria o cliente s3 e o diretório temporário para download de arquivos
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")

        # ajusta a string de caminho
        if caminho[-1] == "/":
            caminho = caminho[:-1]
        caminho = caminho[5:]

        # obtém o nome do bucket
        self.bucket = caminho.split("/")[0]

        # extraí o prefixo
        caminho = "/".join(caminho.split("/")[1:])
        super().__init__(caminho, criar_caminho)

    @property
    def prefixo(self) -> str:
        """
        Renomeia o atributo "caminho" para "prefixo" (nomenclatura AWS)

        :return: string com caminho s3
        """
        return self._caminho

    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        self.client.put_object(Bucket=self.bucket, Key=f"{self.prefixo}/")

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
        return f"s3://{self.bucket}/{self.prefixo}/{destino}"

    def _apaga_diretorio(self, prefixo: str, apaga_conteudo: bool = True) -> None:
        """
        Apaga os conteúdos de um namespace na Amazon
        Fonte: https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder

        :param prefixo: prefixo a ser removido
        :param apaga_conteudo: flag se devemos apagar o diretório mesmo que
        ele tenha algum conteúdo
        """
        # obtém o objeto de paginação do AWS
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=f"{prefixo}/")

        # lista os objetos a serem deletados
        delete_us = dict(Objects=[])
        for item in pages.search("Contents"):
            delete_us["Objects"].append(dict(Key=item["Key"]))

        # realiza a remoção do conteúdo
        if len(delete_us["Objects"]):
            if not apaga_conteudo:
                raise ValueError("O diretório não está vazio")
            for i in range(0, len(delete_us["Objects"]), self.LIMITE_AWS):
                deletar = {"Objects": delete_us["Object"][i : i + self.LIMITE_AWS]}
                self.client.delete_objects(Bucket=self.bucket, Delete=deletar)

    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto

        Fonte: https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder

        :param apaga_conteudo: flag se devemos apagar o diretório mesmo que
        ele tenha algum conteúdo
        """
        self._apaga_diretorio(self.prefixo)

    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        objetos_s3 = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=self.prefixo, Delimiter="/"
        )
        conteudo = objetos_s3.get("Contents")
        if conteudo:
            return [
                os.path.basename(item.get("Key"))
                for item in conteudo
                if not item.get("Key").endswith("/")
            ]
        else:
            return list()

    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado conteúdo contido dentro
        do caminho é um arquivo

        :param nome_conteudo: nome do conteúdo a ser verificado
        :return: True se for um arquivo
        """
        return "Size" in (
            self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"{self.prefixo}/{nome_conteudo}",
                Delimiter="/",
            ).get("Contents")[0]
        )

    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        if self.verifica_se_arquivo(nome_origem):
            origem = {
                "Bucket": self.bucket,
                "Key": self.client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=f"{self.prefixo}/{nome_origem}",
                    Delimiter="/",
                )
                .get("Contents")[0]
                .get("Key"),
            }
            self.client.copy(origem, self.bucket, f"{self.prefixo}/{nome_destino}")
            self.apaga_conteudo(nome_origem)
        else:
            caminho_origem = self.__class__(
                caminho=self.obtem_caminho(nome_origem),
                criar_caminho=False,
            )
            caminho_destino = self.__class__(
                caminho=self.obtem_caminho(nome_destino),
                criar_caminho=True,
            )
            for cont in caminho_origem.lista_conteudo():
                caminho_origem._copia_conteudo_mesmo_caminho(cont, caminho_destino)
            caminho_origem.apaga_caminho(apaga_conteudo=True)

    def _copia_conteudo_mesmo_caminho(
        self, nome_conteudo: str, caminho_destino: CaminhoS3
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        if self.verifica_se_arquivo(nome_conteudo):
            origem = {
                "Bucket": self.bucket,
                "Key": self.client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=f"{self.prefixo}/{nome_conteudo}",
                    Delimiter="/",
                )
                .get("Contents")[0]
                .get("Key"),
            }
            self.client.copy(
                origem,
                caminho_destino.bucket,
                f"{caminho_destino.prefixo}/{nome_conteudo}",
            )
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
                caminho_origem._copia_conteudo_mesmo_caminho(cont, caminho_destino)

    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        if self.verifica_se_arquivo(nome_conteudo):
            objetos_s3 = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=f"{self.prefixo}/{nome_conteudo}",
                Delimiter="/",
            )
            for item in objetos_s3.get("Contents"):
                self.client.delete_object(Bucket=self.bucket, Key=item.get("Key"))
        else:
            if nome_conteudo[-1] == "/":
                nome_conteudo = nome_conteudo[:-1]
            self._apaga_diretorio(f"{self.prefixo}/{nome_conteudo}")

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
        Carrega um conteúdo específico do bucket s3

        :param nome_arq: nome do arquivo a ser carregado
        :return: conteúdo baixado
        """
        return self.client.get_object(
            Bucket=self.bucket,
            Key=f"{self.prefixo}/{nome_arq}",
        )["Body"]

    def write_df(
        self,
        dados: typing.Union[pd.DataFrame, gpd.GeoDataFrame],
        func: typing.Callable,
        nome_arq: str,
        **kwargs: typing.Any,
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

    def buffer_para_escrita(self, nome_arq: str) -> typing.BinaryIO:
        """
        Gera um buffer para upload de dados para o caminho

        :param nome_arq: nome do arquivo a ser salvo
        :return: buffer para upload do conteúdo
        """
        return S3Buffer(
            client=self.client, bucket=self.bucket, key=self.prefixo, filename=nome_arq
        )

    def gpd_read_file(self, nome_arq: str, **kwargs: typing.Any) -> gpd.GeoDataFrame:
        """
        Carrega o arquivo como um dataframe pandas de acordo com o arquivo específicado

        :param nome_arq: nome do arquivo a ser carregado
        :param kwargs: argumentos de carregamento para serem passados para função geopandas
        :return: data frame com objeto carregado
        """
        cam = self.obtem_caminho(nome_arq)
        if kwargs.get("ext") == "zip":
            cam = "zip+" + cam
        return gpd.read_file(cam, **obtem_argumentos_objeto(gpd.read_file, kwargs))
