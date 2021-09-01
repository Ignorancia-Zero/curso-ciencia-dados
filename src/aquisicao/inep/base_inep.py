import abc
import os
import typing
import urllib

from bs4 import BeautifulSoup

from src.aquisicao.base_etl import BaseETL
from src.utils.web import download_dados_web


class BaseINEPETL(BaseETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do INEP
    """

    URL: str = (
        "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/"
    )

    _url: str

    def __init__(
        self, entrada: str, saida: str, base: str, criar_caminho: bool = True
    ) -> None:
        """
        Instância o objeto de ETL INEP

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param base: Nome da base que vai na URL do INEP
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(entrada, saida, criar_caminho)

        self._url = f"{self.URL}/{base}"

    def le_pagina_inep(self) -> typing.Dict[str, str]:
        """
        Realiza o web-scraping da página de dados do INEP

        :return: dicionário com nome do arquivo e link para a página
        """
        html = urllib.request.urlopen(self._url).read()
        soup = BeautifulSoup(html, features="html.parser")
        return {
            tag["href"].split("_")[-1]: tag["href"]
            for tag in soup.find_all("a", {"class": "external-link"})
        }

    def dicionario_para_baixar(self) -> typing.Dict[str, str]:
        """
        Le os conteúdos da pasta de dados e seleciona apenas os arquivos
        a serem baixados como complementares

        :return: dicionário com nome do arquivo e link para a página
        """
        para_baixar = self.le_pagina_inep()
        baixados = os.listdir(str(self.caminho_entrada))
        return {arq: link for arq, link in para_baixar.items() if arq not in baixados}

    def download_conteudo(self) -> None:
        """
        Realiza o download dos dados INEP para uma pasta local
        """
        for arq, link in self.dicionario_para_baixar().items():
            caminho_arq = self.caminho_saida / arq
            download_dados_web(caminho_arq, link)

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
