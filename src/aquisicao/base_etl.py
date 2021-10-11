import abc
import logging
import typing

from src.io.data_store import DataStore
from src.io.data_store import Documento


class BaseETL(abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar
    """

    ds: DataStore
    criar_caminho: bool
    _dados_entrada: typing.List[Documento]
    _dados_saida: typing.List[Documento]
    logger: logging.Logger

    def __init__(self, ds: DataStore, criar_caminho: bool = True) -> None:
        """
        Instância o objeto de ETL Base

        :param ds: instância de objeto data store
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        self.criar_caminho = criar_caminho
        self._ds = ds

        self._dados_entrada = None
        self._dados_saida = None

        self.logger = logging.getLogger(__name__)

    def __str__(self) -> str:
        """
        Representação de texto da classe
        """
        return self.__class__.__name__

    @property
    def dados_entrada(self) -> typing.List[Documento]:
        """
        Acessa o dicionário de dados de entrada

        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if self._dados_entrada is None:
            self.extract()
        return self._dados_entrada

    @property
    def dados_saida(self) -> typing.List[Documento]:
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
        for doc in self.dados_saida:
            self._ds.insere_dados(doc)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        self.logger.info(f"EXTRAINDO DADOS {self}")
        self.extract()

        self.logger.info(f"TRANSFORMANDO DADOS {self}")
        self.transform()

        self.logger.info(f"CARREGANDO DADOS {self}")
        self.load()
