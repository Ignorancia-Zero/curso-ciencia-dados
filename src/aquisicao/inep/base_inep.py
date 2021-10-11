import abc
import tempfile
import typing
import urllib
from pathlib import Path

from bs4 import BeautifulSoup

from src.aquisicao.base_etl import BaseETL
from src.io.caminho import obtem_objeto_caminho
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.web import download_dados_web


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

    def __init__(self, ds: DataStore, base: str, criar_caminho: bool = True) -> None:
        """
        Instância o objeto de ETL INEP

        :param ds: instância de objeto data store
        :param base: Nome da base que vai na URL do INEP
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(ds, criar_caminho)

        self._base = base.replace("-", "_")
        self._url = f"{self.URL}/{base}"
        self._inep = None

    @property
    def inep(self) -> typing.Dict[Documento, str]:
        """
        Realiza o web-scraping da página de dados do INEP

        :return: dicionário com nome do arquivo e link para a página
        """
        if self._inep is None:
            html = urllib.request.urlopen(self._url).read()
            soup = BeautifulSoup(html, features="html.parser")
            self._inep = {
                Documento(
                    self._ds,
                    referencia=dict(
                        nome=tag["href"].split("_")[-1],
                        colecao="externo",
                        pasta=self._base,
                    ),
                ): tag["href"]
                for tag in soup.find_all("a", {"class": "external-link"})
            }
        return self._inep

    def dicionario_para_baixar(self) -> typing.Dict[Documento, str]:
        """
        Le os conteúdos da pasta de dados e seleciona apenas os arquivos
        a serem baixados como complementares

        :return: dicionário com nome do arquivo e link para a página
        """
        return {doc: link for doc, link in self.inep.items() if not doc.exists()}

    def download_conteudo(self) -> None:
        """
        Realiza o download dos dados INEP para uma pasta local
        """
        for doc, link in self.dicionario_para_baixar().items():
            with tempfile.TemporaryFile() as temp:
                download_dados_web(temp, link)
                cam1 = obtem_objeto_caminho(str(Path(temp.name).parent))
                cam2 = self._ds.gera_caminho(doc)
                cam1.copia_conteudo(temp.name, cam2)

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
