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

    _ds: DataStore
    _reprocessar: bool
    _criar_caminho: bool
    _dados_entrada: typing.List[Documento]
    _dados_saida: typing.List[Documento]
    _logger: logging.Logger

    def __init__(
        self,
        ds: DataStore,
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL Base

        :param ds: instância de objeto data store
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        self._criar_caminho = criar_caminho
        self._reprocessar = reprocessar
        self._ds = ds
        self._logger = logging.getLogger(__name__)

    def __str__(self) -> str:
        """
        Representação de texto da classe
        """
        return self.__class__.__name__

    @property
    @abc.abstractmethod
    def documentos_entrada(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de entrada

        :return: lista de documentos de entrada
        """
        raise NotImplementedError("É preciso implementar o método")

    @property
    @abc.abstractmethod
    def documentos_saida(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de saída

        :return: lista de documentos de saída
        """
        raise NotImplementedError("É preciso implementar o método")

    @property
    def dados_entrada(self) -> typing.List[Documento]:
        """
        Acessa o dicionário de dados de entrada

        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if not hasattr(self, "_dados_entrada"):
            self.extract()
        return self._dados_entrada

    @property
    def dados_saida(self) -> typing.List[Documento]:
        """
        Acessa o dicionário de dados de saída

        :return: dicionário com o nome do arquivo e um dataframe com os dados
        """
        if not hasattr(self, "_dados_saida"):
            self.extract()
        return self._dados_saida

    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        raise NotImplementedError("É preciso implementar o método")

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for doc in self.dados_saida:
            self._ds.salva_documento(doc)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados
        """
        if all([doc.exists() for doc in self.documentos_saida]) and not self._reprocessar:
            self._logger.info(f"DADOS DE {self} JÁ FORAM PROCESSADOS")
            return

        self._logger.info(f"EXTRAINDO DADOS {self}")
        self.extract()

        self._logger.info(f"TRANSFORMANDO DADOS {self}")
        self.transform()

        self._logger.info(f"CARREGANDO DADOS {self}")
        self.load()
