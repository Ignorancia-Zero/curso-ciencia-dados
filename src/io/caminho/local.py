from __future__ import annotations

import os
import shutil
import typing
from pathlib import Path

from ._base import _CaminhoBase


class CaminhoLocal(_CaminhoBase):
    """
    Objeto caminho que gerencia arquivo contidos no próprio disco
    rígido da máquina executando o código
    """

    caminho: Path

    def __init__(self, caminho: str, criar_caminho: bool = False) -> None:
        """
        Inicializa o objeto caminho local

        :param caminho: string com o caminho desejado
        :param criar_caminho: flag se o caminho deve ser criado
        """
        self.caminho = Path(caminho)
        super().__init__(caminho, criar_caminho)

    def cria_caminho(self) -> None:
        """
        Cria a pasta para a string deste objeto
        """
        self.caminho.mkdir(parents=True, exist_ok=True)

    def _apaga_caminho(self, apaga_conteudo: bool = False) -> None:
        """
        Apaga a pasta para a string deste objeto
        """
        if apaga_conteudo:
            shutil.rmtree(self.caminho)
        else:
            os.remove(str(self.caminho))

    def obtem_caminho(self, destino: typing.Union[str, typing.List[str]]) -> str:
        """
        Obtém uma string com o caminho completo para o destino passado
        que pode ser uma string com um nome de arquivo ou outra pasta,
        ou uma lista de sub-diretórios e um arquivo final

        :param destino: lista ou string de pastas ao destino final
        :return: string com caminho completo para destino
        """
        if isinstance(destino, str):
            destino = [destino]
        return os.path.join(str(self.caminho), *destino)

    def lista_conteudo(self) -> typing.List[str]:
        """
        Lista as pastas e arquivos dentro do caminho selecionado

        :return: lista de pastas e arquivos
        """
        return os.listdir(str(self.caminho))

    def verifica_se_arquivo(self, nome_conteudo: str) -> bool:
        """
        Verifica se um determinado arquivo contido dentro
        do caminho é um arquivo

        :param nome_conteudo: nome do conteúdo a ser verificado
        :return: True se for um arquivo
        """
        return os.path.isfile(self.obtem_caminho(nome_conteudo))

    def _renomeia_conteudo(self, nome_origem: str, nome_destino: str) -> None:
        """
        Renomeia o conteúdo dentro do caminho

        :param nome_origem: nome do conteúdo contido no caminho
        :param nome_destino: nome do conteúdo de destino
        """
        os.rename(str(self.caminho / nome_origem), str(self.caminho / nome_destino))

    def _copia_conteudo_mesmo_caminho(
        self, nome_conteudo: str, caminho_destino: CaminhoLocal
    ) -> None:
        """
        Copia um conteúdo contido no caminho para o caminho de destino

        :param nome_conteudo: nome do conteúdo a ser copiado
        :param caminho_destino: objeto caminho de destino
        """
        if self.verifica_se_arquivo(nome_conteudo):
            shutil.copyfile(
                self.obtem_caminho(nome_conteudo),
                caminho_destino.obtem_caminho(nome_conteudo),
            )
        else:
            shutil.copytree(
                self.obtem_caminho(nome_conteudo),
                caminho_destino.obtem_caminho(nome_conteudo),
            )

    def _apaga_conteudo(self, nome_conteudo: str) -> None:
        """
        Apaga um conteúdo contido no caminho

        :param nome_conteudo: nome do conteúdo a ser apagado
        """
        if self.verifica_se_arquivo(nome_conteudo):
            os.remove(self.obtem_caminho(nome_conteudo))
        else:
            shutil.rmtree(self.obtem_caminho(nome_conteudo))

    def _gera_buffer_carregar(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Carrega o arquivo contido no caminho

        :param nome_arquivo: nome do arquivo a ser carregado
        :param kwargs: argumentos específicos para a função de carregamento
        """
        return open(self.obtem_caminho(nome_arquivo), "rb")

    def _gera_buffer_salvar(
        self, nome_arquivo: str, **kwargs: typing.Any
    ) -> typing.BinaryIO:
        """
        Gera um buffer para os dados a serem salvos em algum local que serão
        usados como parte do método de salvar

        :param nome_arquivo: nome do arquivo a ser salvo
        :param kwargs: argumentos específicos para a função de salvamento
        """
        return open(self.obtem_caminho(nome_arquivo), "wb")
