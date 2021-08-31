import abc
import typing
from pathlib import Path

import pandas as pd


class BaseETL(abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar
    """

    caminho_entrada: Path
    caminho_saida: Path
    _dados_entrada: typing.Dict[str, pd.DataFrame]
    _dados_saida: typing.Dict[str, pd.DataFrame]

    def __init__(self, entrada: str, saida: str, criar_caminho: bool = True) -> None:
        """
        Instância o objeto de ETL Base

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        self.caminho_entrada = Path(entrada)
        self.caminho_saida = Path(saida)

        if criar_caminho:
            self.caminho_entrada.mkdir(parents=True, exist_ok=True)
            self.caminho_saida.mkdir(parents=True, exist_ok=True)

        self._dados_entrada = None
        self._dados_saida = None

    @property
    def dados_entrada(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de entrada

        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_entrada is None:
            self.extract()
        return self._dados_entrada

    @property
    def dados_saida(self) -> typing.Dict[str, pd.DataFrame]:
        """
        Acessa o dicionário de dados de saída

        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_saida is None:
            self.extract()
        return self._dados_saida

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
        for arq, df in self.dados_saida.items():
            df.to_parquet(self.caminho_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        self.extract()
        self.transform()
        self.load()
