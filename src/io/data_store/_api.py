from __future__ import annotations

import logging
import os
import typing
from collections.abc import Collection
from collections.abc import Hashable

import geopandas as gpd
import pandas as pd

from src.io.caminho import CaminhoLocal
from src.io.caminho import CaminhoSQLite
from src.io.caminho import obtem_objeto_caminho
from src.io.caminho._base import _CaminhoBase
from src.io.configs import DS_ENVS, EXTENSOES_TEXTO
from src.io.le_dados import le_dados_comprimidos
from src.utils.info import CAMINHO_INFO
from src.utils.interno import obtem_extencao
from ._catalogo import CatalogoInfo


class Documento(Hashable):
    """
    Representa a interface com os dados que podem ser acessados
    """

    nome: str
    tipo: str
    _pasta: str
    colecao: Colecao
    _data: typing.Any

    def __init__(
        self, ds: DataStore, referencia: typing.Dict[str, str], data: typing.Any = None
    ) -> None:
        """
        Instancia um novo objeto documento

        :param ds: instância de data store
        :param referencia: dicionário com o nome, coleção e pasta
        :param data: dados do documento
        """
        self.ds = ds
        self.nome = os.path.basename(referencia["nome"])

        if not referencia.get("tipo"):
            self.tipo = obtem_extencao(referencia["nome"])
        else:
            self.tipo = referencia["tipo"]

        self._pasta = "" if not referencia.get("pasta") else referencia["pasta"]
        self.colecao = Colecao(ds=ds, nome=referencia["colecao"], pasta=self._pasta)
        self._data = data

    @property
    def pasta(self) -> typing.Union[str, None]:
        return self._pasta

    @pasta.setter
    def pasta(self, v: str) -> None:
        self._pasta = v
        self.colecao.pasta = v

    @property
    def data(self) -> typing.Any:
        """
        Obtém os dados do documento

        :return: dados do documento
        """
        if self._data is None:
            self.obtem_dados()
        return self._data

    @data.setter
    def data(self, dados: typing.Any) -> None:
        """
        Muda o valor dos dados do documento

        :param dados: objeto python para substituir dados
        """
        self._data = dados

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
        return cam.verifica_se_arquivo(self.nome)

    def __eq__(self, documento: typing.Union[object, Documento]) -> bool:
        """
        Checa se dois documentos são iguais

        :param documento: documento para comparar
        :return: True se os documentos forem iguais
        """
        if not isinstance(documento, Documento):
            return False
        else:
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
            f"colecao={self.colecao.nome}, pasta={self._pasta}"
        )

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash(
            (
                self.ds._env,
                self.colecao.nome,
                self.colecao.pasta,
                self.nome,
                self.tipo,
            )
        )


class Colecao(Collection):
    """
    Coleção de documentos contido em um determinado caminho
    gerenciado pelo Data Store
    """

    _dados: typing.Union[None, typing.List[Documento]]
    ds: DataStore
    nome: str
    pasta: str

    def __init__(self, ds: DataStore, nome: str, pasta: str = "") -> None:
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
        return f"Coleção: {self.nome} | {self.pasta}"


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

    _df_ee: pd.DataFrame
    _df_cr: pd.DataFrame
    _df_ep: pd.DataFrame
    _df_cp: pd.DataFrame

    def __init__(self, env: str = "local_completo") -> None:
        """
        Gera uma instância do data store

        :param env: ambiente do objeto data store
        """
        self._env = env
        self._logger = logging.getLogger(__name__)
        self.caminho_base = obtem_objeto_caminho(DS_ENVS[env])

    @property
    def df_ee(self) -> pd.DataFrame:
        """
        Dados de de-para entre o código de ETAPA de ENSINO e
        suas características

        :return: data frame com dados
        """
        if not hasattr(self, "_df_ee"):
            self._df_ee = self.carrega_como_objeto(
                Documento(self, referencia=dict(CatalogoInfo.ETAPA_ENSINO)),
                como_df=True
            )
        return self._df_ee

    @property
    def df_cp(self) -> pd.DataFrame:
        """
        Dados de de-para entre o código de complementação pedagógica
        e a área e nome do curso

        :return: data frame com dados
        """
        if not hasattr(self, "_df_cp"):
            self._df_cp = self.carrega_como_objeto(
                Documento(self, referencia=dict(CatalogoInfo.COMPL_PEDAGOGICA)),
                como_df=True
            )
        return self._df_cp

    @property
    def df_ep(self) -> pd.DataFrame:
        """
        Dados de de-para entre o código de educação profissional
        e o nome e área

        :return: data frame com dados
        """
        if not hasattr(self, "_df_ep"):
            self._df_ep = self.carrega_como_objeto(
                Documento(self, referencia=dict(CatalogoInfo.EDUC_PROF)),
                como_df=True
            )
        return self._df_ep

    @property
    def df_cr(self) -> pd.DataFrame:
        """
        Dados de de-para entre o código de curso de educação
        superior e a área e nome do curso

        :return: data frame com dados
        """
        if not hasattr(self, "_df_cr"):
            self._df_cr = self.carrega_como_objeto(
                Documento(self, referencia=dict(CatalogoInfo.CURSOS)),
                como_df=True
            ).assign(
                NO_CURSO=lambda f: f["NO_CURSO"].astype("category"),
                TP_GRAU_ACADEMICO=lambda f: f["TP_GRAU_ACADEMICO"].astype("category"),
                TP_AREA_CURSO=lambda f: f["TP_AREA_CURSO"].astype("category"),
            )
        return self._df_cr

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
        colecao = data.colecao if data is not None else colecao
        if colecao is None:
            raise ValueError("A coleção obtida é vazia")

        # gera lista de caminhos
        lista_cam = [colecao.nome]
        if colecao.pasta != "":
            lista_cam += colecao.pasta.split("/")

        # verifica se a coleção é uma pasta interna da ferramenta
        if colecao.nome.startswith("__") and colecao.nome.endswith("__"):
            # obtém o nome interno da pasta
            nome_interno = colecao.nome[2:-2]

            # corrige a pasta dentro da coleção
            if colecao.pasta != "":
                lista_cam = colecao.pasta.split("/")
            else:
                lista_cam = []

            # obtém o caminho adequado
            if nome_interno == "info":
                return CaminhoLocal(str(CAMINHO_INFO)).obtem_caminho(lista_cam)
            else:
                raise NotImplementedError(f"Não conhecemos a pasta {nome_interno}")

        return self.caminho_base.obtem_caminho(lista_cam)

    def gera_caminho(
        self,
        documento: typing.Union[Documento, None] = None,
        colecao: typing.Union[Colecao, None] = None,
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
        return obtem_objeto_caminho(
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
            ext = documento.tipo
        else:
            ext = kwargs["ext"]
            del kwargs["ext"]

        # se o caminho for de SQL, nós lemos o dataframe diretamente
        if isinstance(cam, CaminhoSQLite) or ext == "sql":
            return cam.carrega_arquivo(documento.nome, **kwargs)

        # se a extenção do arquivo for zip
        elif ext == "zip":
            # nós vamos processar o zip lendo diversos arquivos
            return le_dados_comprimidos(
                cam.buffer_para_arquivo(documento.nome), ext, **kwargs
            )

        # checa se como_df está ativado
        elif kwargs.get("como_df"):
            del kwargs["como_df"]

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
            del kwargs["como_gdf"]

            # se estiver carrega os dados com o geopandas
            if ext == "parquet":
                return cam.gpd_read_parquet(nome_arq=documento.nome, **kwargs)
            elif ext == "feather":
                return cam.gpd_read_feather(nome_arq=documento.nome, **kwargs)
            elif ext == "shp":
                return cam.gpd_read_shape(nome_arq=documento.nome, **kwargs)
            elif ext in ["json", "geojson", "topojson"]:
                return cam.gpd_read_file(nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para carregar como geo data frame "
                    f"arquivos do tipo {ext}"
                )

        # caso contrário
        else:
            kwargs.pop("como_df", None)
            kwargs.pop("como_gdf", None)

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
        cam = self.gera_caminho(documento=documento, criar_caminho=True)

        # obtém a extenção do arquivo
        if "ext" not in kwargs:
            ext = documento.tipo
        else:
            ext = kwargs["ext"]
            del kwargs["ext"]

        # se o caminho for de SQL
        if isinstance(cam, CaminhoSQLite) or ext == "sql":
            cam.salva_arquivo(documento.data, documento.nome, **kwargs)

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
                cam.save_json(dados=documento.data, nome_arq=documento.nome, **kwargs)
            elif ext == "yml":
                cam.save_yaml(dados=documento.data, nome_arq=documento.nome, **kwargs)
            elif ext == "pkl":
                cam.save_pickle(dados=documento.data, nome_arq=documento.nome, **kwargs)
            elif ext in EXTENSOES_TEXTO:
                cam.save_txt(dados=documento.data, nome_arq=documento.nome, **kwargs)
            else:
                raise NotImplementedError(
                    f"Não criamos um método para carregar como objeto python "
                    f"arquivos do tipo {ext}"
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
