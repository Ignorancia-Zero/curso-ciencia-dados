from __future__ import annotations

import abc
import typing


class _CaminhoBase(abc.ABC):
    """
    Um objeto caminho contém um conjunto de funções que permite
    gerenciar arquivos e diretórios contidos dentro deste repositório
    de dados, que pode ser local ou remoto
    """

    _caminho: str

    def __init__(self, caminho: str, criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho

        :param caminho: string com o caminho desejado
        :param criar_caminho: flag se o caminho deve ser criado
        """
        self._caminho = caminho
        if criar_caminho:
            self.cria_caminho()

    @abc.abstractmethod
    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto
        """
        raise NotImplementedError("É preciso implementar o método")

    def apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto

        :param apaga_conteudo: flag se devemos apagar o diretório mesmo que
        ele tenha algum conteúdo
        """
        if len(self.lista_conteudo()) > 0 and not apaga_conteudo:
            raise PermissionError("O conteúdo do diretório não está vazio")
        else:
            self._apaga_caminho()

    @abc.abstractmethod
    def obtem_caminho(self, destino: typing.Union[str, typing.List[str]]) -> str:
        """
        Obtém uma string com o caminho completo para o destino passado
        que pode ser uma string com um nome de arquivo ou outra pasta,
        ou uma lista de sub-diretórios e um arquivo final

        :param destino: lista ou string de pastas ao destino final
        :return: string com caminho completo para destino
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        raise NotImplementedError("É preciso implementar o método")

    def renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        if nome_origem not in self.lista_conteudo():
            raise FileNotFoundError(
                f"{nome_origem} não está contido em {self._caminho}"
            )
        self._renomeia_conteudo(nome_origem, nome_destino)

    @abc.abstractmethod
    def _copia_conteudo(
        self, nome_conteudo: str, caminho_destino: _CaminhoBase
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        raise NotImplementedError("É preciso implementar o método")

    def copia_conteudo(self, nome_conteudo: str, caminho_destino: _CaminhoBase) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        if nome_conteudo not in self.lista_conteudo():
            raise FileNotFoundError(
                f"{nome_conteudo} não está contido em {self._caminho}"
            )
        self._copia_conteudo(nome_conteudo, caminho_destino)

    @abc.abstractmethod
    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        raise NotImplementedError("É preciso implementar o método")

    def apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        if nome_conteudo not in self.lista_conteudo():
            raise FileNotFoundError(
                f"{nome_conteudo} não está contido em {self._caminho}"
            )
        self._apaga_conteudo(nome_arquivo)

    @abc.abstractmethod
    def carrega_arquivo(self, nome_arquivo: str, **kwargs: typing.Any) -> typing.Any:
        """
        Carrega o arquivo contido no caminho

        :param nome_arquivo: nome do arquivo a ser carregado
        :param kwargs: argumentos específicos para a função de carregamento
        """
        raise NotImplementedError("É preciso implementar o método")

    @abc.abstractmethod
    def salva_arquivo(
        self, dados: typing.Any, nome_arquivo: str, **kwargs: typing.Any
    ) -> None:
        """
        Faz o upload de um determinado conteúdo para o caminho

        :param dados: bytes, data frame, string, etc. a ser salvo
        :param nome_arquivo: nome do arquivo a ser salvo
        :param kwargs: argumentos específicos para a função de salvamento
        """
        raise NotImplementedError("É preciso implementar o método")
