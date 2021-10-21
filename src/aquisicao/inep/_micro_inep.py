import abc
import tempfile
import typing
from pathlib import Path
from urllib.request import urlopen

from bs4 import BeautifulSoup

from src.aquisicao._etl import BaseETL
from src.configs import COLECAO_DADOS_WEB
from src.io.caminho import obtem_objeto_caminho
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.web import download_dados_web
from src.utils.web import obtem_pagina


class BaseINEPETL(BaseETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do INEP
    """

    URL: str = (
        "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/"
    )

    _base: str
    _url: str
    _inep: typing.Dict[Documento, str]

    def __init__(
        self,
        ds: DataStore,
        base: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL INEP

        :param ds: instância de objeto data store
        :param base: Nome da base que vai na URL do INEP
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(ds, criar_caminho, reprocessar)

        self._base = base.replace("-", "_")
        self._ano = ano
        self._url = f"{self.URL}/{base}"

    @property
    def inep(self) -> typing.Dict[Documento, str]:
        """
        Realiza o web-scraping da página de dados do INEP

        :return: dicionário com nome do arquivo e link para a página
        """
        if not hasattr(self, "_inep"):
            soup = obtem_pagina(self._url)
            self._inep = {
                Documento(
                    self._ds,
                    referencia=dict(
                        nome=tag["href"].split("_")[-1],
                        colecao=COLECAO_DADOS_WEB,
                        pasta=self._base,
                    ),
                ): tag["href"]
                for tag in soup.find_all("a", {"class": "external-link"})
            }
        return self._inep

    @property
    def ano(self) -> int:
        """
        Ano do censo sendo processado pelo objeto

        :return: ano como um núnero inteiro
        """
        if isinstance(self._ano, str):
            if self._ano == "ultimo":
                return max([int(b.nome[:4]) for b in self.inep])
            else:
                raise ValueError(f"Não conseguimos processar ano={self._ano}")
        else:
            return self._ano

    def dicionario_para_baixar(self) -> typing.Dict[Documento, str]:
        """
        Le os conteúdos da pasta de dados e seleciona apenas os arquivos
        a serem baixados como complementares

        :return: dicionário com nome do arquivo e link para a página
        """
        return {
            doc: link
            for doc, link in self.inep.items()
            if not doc.exists() and int(doc.nome[:4]) == self.ano
        }

    @property
    def documentos_entrada(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de entrada

        :return: lista de documentos de entrada
        """
        return [doc for doc in self.inep if int(doc.nome[:4]) == self.ano]

    @property
    @abc.abstractmethod
    def documentos_saida(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de saída

        :return: lista de documentos de saída
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for doc in self.dados_saida:
            doc.pasta = f"{doc.nome}/ANO={self.ano}"
            doc.nome = f"{self.ano}.parquet"
            doc.data.drop(columns=["ANO"], inplace=True)
            self._ds.salva_documento(doc)
