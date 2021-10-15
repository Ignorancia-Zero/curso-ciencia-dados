from __future__ import annotations

import logging
import os
import typing
from collections.abc import Collection
from collections.abc import Hashable

import geopandas as gpd
import pandas as pd

from src.io.caminho import obtem_objeto_caminho
from src.io.caminho._base import _CaminhoBase
from src.io.configs import DS_ENVS, EXTENSOES_TEXTO
from src.io.le_dados import le_dados_comprimidos
from src.utils.interno import obtem_extencao


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
        self.nome = os.path.basename(referencia["nome"])
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
            self._data = self.ds.carrega_como_objeto(self, **kwargs)
        return self._data

    def exists(self) -> bool:
        """
        Checa se o documento existe

        :return: True se o documento existir
        """
        cam = self.ds.gera_caminho(self)
        return cam.verifica_se_arquivo(f"{self.nome}.{self.tipo}")

    @property
    def data(self, **kwargs) -> typing.Any:
        """
        Obtém os dados do documento

        :return: dados do documento
        """
        if self._data is None:
            self.obtem_dados(**kwargs)
        return self._data

    @data.setter
    def data(self, dados: typing.Any) -> None:
        """
        Muda o valor dos dados do documento

        :param dados: objeto python para substituir dados
        """
        self._data = dados

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
    caminho_base: _CaminhoBase
    _logger: logging.Logger

    def __init__(self, env: str = "local_completo") -> None:
        """
        Gera uma instância do data store

        :param env: ambiente do objeto data store
        """
        self._env = env
        self._logger = logging.getLogger(__name__)
        self.caminho_base = obtem_objeto_caminho(DS_ENVS[env])

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

        return self.caminho_base.obtem_caminho(lista_cam)

    def gera_caminho(
        self,
        documento: Documento = None,
        colecao: Colecao = None,
        criar_caminho: bool = False,
    ) -> _CaminhoBase:
        """
        Gera um novo objeto caminho com destino a coleção de dados
        que pode ser passada pelo objeto dados ou pelo objeto coleção

        :param documento: objeto com informação de um documento
        :param colecao: objeto coleção de dados
        :param criar_caminho: flag se o caminho deve ser criado
        :return: caminho para a coleção
        """
        return self.caminho_base.__class__(
            self._obtem_caminho(documento, colecao), criar_caminho=criar_caminho
        )

    def carrega_como_objeto(self, documento: Documento, **kwargs) -> typing.Any:
        """
        Carrega os dados de um determinado documento em
        um objeto python

        :param documento: documento a ser carregado
        :param kwargs: parâmetros de carregamento
        :return: objeto python carregado
        """
        self._logger.debug(f"Carregando documento {documento}")

        # obtém o objeto caminho para o documento
        cam = self.gera_caminho(documento=documento)

        # obtém a extenção do arquivo
        if "ext" not in kwargs:
            kwargs["ext"] = documento.tipo
        ext = kwargs["ext"]

        # se a extenção do arquivo for zip
        if ext == "zip":
            # nós vamos ler os dados comprimidos pelo buffer gerado
            return le_dados_comprimidos(
                cam.buffer_para_arquivo(documento.nome), ext, **kwargs
            )

        # checa se como_df está ativado
        elif kwargs.get("como_df"):
            # se estiver carrega os dados com o pandas
            if ext == "parquet":
                return cam.read_parquet(nome_arq=documento.nome, **kwargs)
            elif ext == "hdf" or ext == "h5":
                return cam.read_hdf(nome_arq=documento.nome, **kwargs)
            elif ext == "pkl":
                return cam.read_pickle(nome_arq=documento.nome, **kwargs)
            elif ext == "feather":
                return cam.read_feather(nome_arq=documento.nome, **kwargs)
            elif ext == "csv" or ext == "txt" or ext == "tsv":
                return cam.read_csv(nome_arq=documento.nome, **kwargs)
            elif ext == "xlsx" or ext == "xls":
                return cam.read_excel(nome_arq=documento.nome, **kwargs)
            elif ext == "ods":
                kwargs["engine"] = "odf"
                return cam.read_excel(nome_arq=documento.nome, **kwargs)
            elif ext == "html":
                return cam.read_html(nome_arq=documento.nome, **kwargs)
            elif ext == "xml":
                return cam.read_xml(nome_arq=documento.nome, **kwargs)
            elif ext == "json":
                return cam.read_json(nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para carregar como data frame "
                    f"arquivos do tipo {ext}"
                )

        # checa se como_gdf está ativado
        elif kwargs.get("como_gdf"):
            # se estiver carrega os dados com o geopandas
            if ext == "parquet":
                return cam.gpd_read_parquet(nome_arq=documento.nome, **kwargs)
            elif ext == "feather":
                return cam.gpd_read_feather(nome_arq=documento.nome, **kwargs)
            elif ext in ["shp", "json", "geojson", "topojson"]:
                return cam.gpd_read_file(nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para carregar como geo data frame "
                    f"arquivos do tipo {ext}"
                )

        # caso contrário
        else:
            # tenta alguma das extenções restantes
            if ext == "json":
                return cam.load_json(nome_arq=documento.nome, **kwargs)
            elif ext == "yml":
                return cam.load_yaml(nome_arq=documento.nome, **kwargs)
            elif ext == "pkl":
                return cam.load_pickle(nome_arq=documento.nome, **kwargs)
            elif ext in EXTENSOES_TEXTO:
                return cam.load_txt(nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para carregar como objeto python "
                    f"arquivos do tipo {ext}"
                )

    def salva_documento(self, documento: Documento, **kwargs) -> None:
        """
        Insere os dados de um documento para o data store

        :param documento: documento a ser salvo
        :param kwargs: parâmetros para salvar
        """
        self._logger.debug(f"Salvando documento {documento}")

        # obtém o objeto caminho para o documento
        cam = self.gera_caminho(documento=documento)

        # obtém a extenção do arquivo
        if "ext" not in kwargs:
            kwargs["ext"] = documento.tipo
        ext = kwargs["ext"]

        # se a extenção do arquivo for zip
        if ext == "zip":
            raise NotImplementedError("Nós não configuramos a escrita de arquivos zip")

        # escolhe a função de exportação para um data frame
        if isinstance(documento.data, pd.DataFrame):
            if ext == "parquet":
                cam.to_parquet(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "hdf" or ext == "h5":
                cam.to_hdf(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "pkl":
                cam.to_pickle(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "feather":
                cam.to_feather(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "csv" or ext == "txt" or ext == "tsv":
                cam.to_csv(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "xlsx" or ext == "xls":
                cam.to_excel(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "ods":
                kwargs["engine"] = "odf"
                cam.to_excel(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "html":
                cam.to_html(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "xml":
                cam.to_xml(nome_arq=documento.nome, dados=documento.data, **kwargs)
            elif ext == "json":
                cam.to_json(nome_arq=documento.nome, dados=documento.data, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para escrever um data frame "
                    f"na extenção {ext}"
                )

        # escolhe a função de exportação para um geo data frame
        elif isinstance(documento.data, gpd.GeoDataFrame):
            if ext == "parquet":
                cam.gpd_to_parquet(nome_arq=documento.nome, **kwargs)
            elif ext == "feather":
                cam.gpd_to_feather(nome_arq=documento.nome, **kwargs)
            elif ext in ["shp", "json", "geojson", "topojson"]:
                cam.gpd_to_file(nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para escrever um geo data frame "
                    f"na extenção {ext}"
                )

        # escolhe alguma das funções restantes
        else:
            if ext == "json":
                return cam.load_json(nome_arq=documento.nome, **kwargs)
            elif ext == "yml":
                return cam.load_yaml(nome_arq=documento.nome, **kwargs)
            elif ext == "pkl":
                return cam.load_pickle(nome_arq=documento.nome, **kwargs)
            elif ext in EXTENSOES_TEXTO:
                return cam.load_txt(nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para carregar como objeto python "
                    f"arquivos do tipo {ext}"
                )
        self._logger.debug(f"Salvando documento {documento}")
        self.gera_caminho(documento=documento).salva_arquivo(
            dados=documento.data,
            nome_arquivo=documento.nome,
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
