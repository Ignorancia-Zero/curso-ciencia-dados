import abc
import os
import shutil
import zipfile

import pandas as pd
import pyunpack
from tqdm import tqdm

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
        super().__init__(
            f"{entrada}/censo_escolar", saida, "censo-escolar", criar_caminho
        )

        self._tabela = tabela

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # realiza o download dos dados do censo
        self.download_conteudo()

        # inicializa os dados de entrada como um dicionário vazio
        self._dados_entrada = dict()

        # para cada arquivo do censo demográfico
        for censo in tqdm(os.listdir(self.caminho_entrada)):
            # abre o arquivo zip com o conteúdo do censo
            with zipfile.ZipFile(self.caminho_entrada / f"{censo}") as z:
                # lista os conteúdos dos arquivos zip que contém o nome tabela
                arqs = [f for f in z.namelist() if f"{self._tabela}." in f.lower()]

                # se houver algum arquivo deste tipo dentro do zip
                if len(arqs) == 1:
                    arq = arqs[0]

                    # e este arquivo for um CSV
                    if ".csv" in arq.lower():
                        # le os conteúdos do arquivo por meio do buffer do zip
                        self._dados_entrada[censo] = pd.read_csv(
                            z.open(arq), encoding="latin-1", sep="|"
                        )

                    # caso seja outro arquivo zip
                    elif ".zip" in arq.lower():
                        # cria um novo zipfile e le o arquivo deste novo zip
                        with zipfile.ZipFile(z.open(arq)) as z2:
                            arq = z2.namelist()[0]
                            self._dados_entrada[censo] = pd.read_csv(
                                z2.open(arq), encoding="latin-1", sep="|"
                            )

                    # caso seja um arquivo winrar
                    elif ".rar" in arq.lower():
                        # extraí o conteúdo do arquivo
                        z.extract(arq, path=self.caminho_entrada)
                        (
                            pyunpack.Archive(
                                self.caminho_entrada / f"{arq}"
                            ).extractall(self.caminho_entrada)
                        )

                        # lê os dados do disco
                        csv = [
                            f
                            for f in os.listdir(self.caminho_entrada)
                            if f"{self._tabela}." in f.lower()
                        ][0]
                        self._dados_entrada[censo] = pd.read_csv(
                            self.caminho_entrada / f"{csv}", encoding="latin-1", sep="|"
                        )

                        # excluí os conteúdos extraídos
                        shutil.rmtree(self.caminho_entrada / f"{arq.split('/')[0]}")
                        os.remove(self.caminho_entrada / f"{csv}")

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        pass
