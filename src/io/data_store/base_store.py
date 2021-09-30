import abc

from src.io.data_store.data import Data


class BaseDataStore(abc.ABC):

    def __init__(self):
        self.pasta_dados = "dados"

    @abc.abstractmethod
    def get_data(self, data: Data):
        pass

    @abc.abstractmethod
    def put_data(self):
        pass

    @abc.abstractmethod
    def list_content(self):
        pass

    @abc.abstractmethod
    def cache(self):
        pass
