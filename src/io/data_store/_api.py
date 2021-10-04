from __future__ import annotations

import logging
import os
import typing
from collections.abc import Collection
from collections.abc import Hashable

from src.utils.interno import obtem_extencao
from ..caminho import obtem_objeto_caminho
from ..caminho._base import _CaminhoBase
from ..configs import DS_ENVS


class Documento(Hashable):
    """
    Representa a interface com os dados que podem ser acessados
    """

    colecao: str
    nome: str
    tipo: str
    pasta: str
    colecao: Colecao
    _data: typing.Any

    def __init__(
        self, ds: DataStore, referencia: dict, data: typing.Any = None
    ) -> None:
        """
        Instancia um novo objeto documento

        :param ds: instância de data store
        :param referencia: dicionário com o nome, coleção e pasta
        :param data: dados do documento
        """
        self.ds = ds
        self.nome = os.path.splitext(os.path.basename(referencia["nome"]))[0]
        self.tipo = referencia.get("tipo")
        if self.tipo is None:
            self.tipo = obtem_extencao(referencia["nome"])
        self.pasta = referencia.get("pasta")
        self.colecao = Colecao(ds=ds, nome=referencia.get("colecao"), pasta=self.pasta)
        self._data = data

    def obtem_dados(self, **kwargs) -> typing.Any:
        """
        Carrega os dados do documento em um novo objeto

        :param kwargs: argumentos de carregamento dos dados
        :return: objeto carregado python
        """
        if self._data is None:
            self._data = self.ds.obtem_dados(self, **kwargs)
        return self._data

    @property
    def data(self, **kwargs) -> typing.Any:
        """
        Obtém os dados do documento

        :return: dados do documento
        """
        if self._data is None:
            self.obtem_dados(**kwargs)
        return self._data

    def __eq__(self, documento: Documento) -> bool:
        """
        Checa se dois documentos são iguais

        :param documento: documento para comparar
        :return: True se os documentos forem iguais
        """
        if not isinstance(documento, Documento):
            return False
        return (
            self.ds._env == documento.ds._env
            and self.colecao.nome == documento.colecao.nome
            and self.colecao.pasta == documento.colecao.pasta
            and self.nome == documento.nome
            and self.tipo == documento.tipo
        )

    def __str__(self) -> str:
        """
        Representação textual do documento

        :return: string com documento
        """
        return (
            f"Documento: nome={self.nome}, tipo={self.tipo}, "
            f"colecao={self.colecao.nome}"
        )

    def __hash__(self) -> hash:
        return hash(
            (
                self.ds._env,
                self.colecao.nome,
                self.colecao.pasta,
                self.nome,
                self.tipo,
            )
        )

    def __repr__(self) -> str:
        return str(self)


class Colecao(Collection):
    """
    Coleção de documentos contido em um determinado caminho
    gerenciado pelo Data Store
    """

    _dados: typing.List[Documento]
    ds: DataStore
    nome: str
    pasta: str

    def __init__(self, ds: DataStore, nome: str, pasta: str = None) -> None:
        """
        Instância um novo objeto Coleção

        :param ds: instância do data store
        :param nome: nome da coleção
        :param pasta: sub-pastas da coleção
        """
        self.ds = ds
        self.nome = nome
        self._dados = None
        self.pasta = pasta

    @property
    def dados(self) -> typing.List[Documento]:
        """
        Lista de dados contido numa coleção

        :return: lista de dados da coleção
        """
        if self._dados is None:
            self._dados = self.ds.lista_documentos(self)
        return self._dados

    def __contains__(self, item) -> bool:
        """
        Verifica se um documento está contido na coleção

        :param item: documento a ser verificado
        :return: True se o documento está na coleção
        """
        for contained_item in self.dados:
            if item == contained_item:
                return True
        return False

    def __iter__(self):
        """
        Itera sobre os documentos da coleção

        :return: gerador de documentos
        """
        for doc in self.dados:
            yield doc

    def __len__(self) -> int:
        """
        Obtém o tamanho da coleção

        :return: número de documentos na coleção
        """
        return len(self.dados)

    def __str__(self):
        """
        Representação textual da coleção

        :return: texto com caminho para coleção
        """
        return f"Coleção: {self.ds._obtem_caminho(colecao=self)}"


class DataStore:
    """
    O objeto data store será a interface responsável por guardar dados
    que a ferramenta produzir que pode ser compartilhado com o resto
    do ambiente

    Dado isso, ele deve ser capaz de carregar, salvar e listar dados
    que estão contidos no ambiente do mesmo, além de ser capaz de gerar
    caches entre ambientes distintos
    """

    _env: str
    caminho: _CaminhoBase
    logger: logging.Logger

    def __init__(self, env: str) -> None:
        """
        Gera uma instância do data store

        :param env: ambiente do objeto data store
        """
        self._env = env
        self.logger = logging.getLogger(__name__)
        self.caminho = obtem_objeto_caminho(DS_ENVS[env])

    def _obtem_caminho(self, data: Documento = None, colecao: Colecao = None) -> str:
        """
        Gera uma string com o caminho de destino à coleção de dados
        que pode ser passada pelo objeto dados ou pelo objeto coleção

        :param data: objeto com informação de um dado
        :param colecao: objeto coleção de dados
        :return: string caminho para a coleção
        """
        # levanta um erro caso nenhum dos dois seja fornecido
        if data is None and colecao is None:
            raise ValueError("É preciso fornecer um objeto dados ou coleção")

        # extraí o objeto coleção do objeto data
        if data is not None:
            colecao = data.colecao

        # gera lista de caminhos
        lista_cam = [colecao.nome]
        if colecao.pasta is not None:
            lista_cam += colecao.pasta.split("/")

        return self.caminho.obtem_caminho(lista_cam)

    def gera_caminho(
        self, documento: Documento = None, colecao: Colecao = None
    ) -> _CaminhoBase:
        """
        Gera um novo objeto caminho com destino a coleção de dados
        que pode ser passada pelo objeto dados ou pelo objeto coleção

        :param documento: objeto com informação de um documento
        :param colecao: objeto coleção de dados
        :return: caminho para a coleção
        """
        return self.caminho.__class__(self._obtem_caminho(documento, colecao))

    def obtem_dados(self, documento: Documento, **kwargs) -> typing.Any:
        """
        Carrega os dados de um determinado documento em
        um objeto python

        :param documento: documento a ser carregado
        :param kwargs: parâmetros de carregamento
        :return: objeto python carregado
        """
        self.logger.debug(f"Carregando documento {documento}")
        return self.gera_caminho(documento=documento).carrega_arquivo(
            nome_arquivo=f"{documento.nome}.{documento.tipo}",
            ext=documento.tipo,
            **kwargs,
        )

    def insere_dados(self, documento: Documento, **kwargs) -> None:
        """
        Insere os dados de um documento para o data store

        :param documento: documento a ser salvo
        :param kwargs: parâmetros para salvar
        """
        self.logger.debug(f"Salvando documento {documento}")
        self.gera_caminho(documento=documento).salva_arquivo(
            dados=documento.data,
            nome_arquivo=f"{documento.nome}.{documento.tipo}",
            **kwargs,
        )

    def lista_documentos(self, colecao: Colecao) -> typing.List[Documento]:
        """
        Obtém a lista de todos os documentos contidos numa coleção

        :param colecao: objeto coleção
        :return: lista de documentos da coleção
        """
        return [
            Documento(
                self, {"nome": arq, "colecao": colecao.nome, "pasta": colecao.pasta}
            )
            for arq in self.gera_caminho(colecao=colecao).lista_conteudo()
        ]
