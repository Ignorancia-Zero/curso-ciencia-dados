import abc
import zipfile

import pandas as pd

from src.aquisicao.inep.base_inep import BaseINEPETL


class BaseCensoEscolarETL(BaseINEPETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do CensoEscolar
    """

    _tabela: str

    def __init__(
        self, entrada: str, saida: str, tabela: str, criar_caminho: bool = True
    ) -> None:
        """
        Instância o objeto de ETL Censo Escolar

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param tabela: Tabela do censo escolar a ser processada
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(entrada, saida, "censo-escolar", criar_caminho)

        self._tabela = tabela

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # realiza o download dos dados do censo
        self.download_conteudo()

        # carrega as tabelas de interesse
        for arq in self.le_pagina_inep():
            with zipfile.ZipFile(arq) as arq_zip:
                nome_zip = [arq for arq in arq_zip.namelist() if self._tabela in arq][0]
                self._dados_entrada[arq] = pd.read_csv(
                    arq_zip.open(nome_zip), encoding="latin-1", sep="|"
                )

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        pass
