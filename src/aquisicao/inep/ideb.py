import abc
import os
import typing

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao._etl import BaseETL
from src.configs import COLECAO_DADOS_WEB
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.web import obtem_pagina


class IDEBETL(BaseETL, abc.ABC):
    """
    Faz o processamento de dados do IDEB
    """

    URL: str = "https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb/resultados"
    _links: typing.Dict[Documento, str]
    _doc_saida: Documento

    def __init__(
        self,
        ds: DataStore,
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL INEP

        :param ds: instância de objeto data store
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(ds, criar_caminho, reprocessar)
        self._doc_saida = Documento(
            self._ds,
            referencia=CatalogoAquisicao.IDEB
        )

    @property
    def links(self) -> typing.Dict[Documento, str]:
        """
        Realiza o web-scraping da página de dados do INEP

        :return: dicionário com nome do arquivo e link para a página
        """
        if not hasattr(self, "_links"):
            # lê a página web
            soup = obtem_pagina(self.URL)

            # extraí a lista de links
            self._links = {
                Documento(
                    self._ds,
                    referencia=dict(
                        nome=a["href"].split("/")[-1],
                        colecao=COLECAO_DADOS_WEB,
                        pasta="ideb",
                    ),
                ): a["href"]
                for a in soup.find_all("a", attrs={"class": "external-link"})
                if "escola" in a.attrs["href"] and "download" in a.attrs["href"]
            }
        return self._links

    def dicionario_para_baixar(self) -> typing.Dict[Documento, str]:
        """
        Le os conteúdos da pasta de dados e seleciona apenas os arquivos
        a serem baixados como complementares

        :return: dicionário com nome do arquivo e link para a página
        """
        return {doc: link for doc, link in self.links.items() if not doc.exists()}

    @property
    def documentos_entrada(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de entrada

        :return: lista de documentos de entrada
        """
        return list(self.links)

    @property
    def documentos_saida(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de saída

        :return: lista de documentos de saída
        """
        return [self._doc_saida]

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # realiza o download dos dados do censo
        self.download_conteudo()

        # inicializa os dados de entrada como um dicionário vazio
        self._dados_entrada = list()

        # para cada arquivo do censo demográfico
        for ideb in tqdm(self.documentos_entrada):
            ideb.obtem_dados(
                como_df=True,
                padrao_comp=(
                    f"({os.path.basename(ideb.nome)})" f"[.](xlsx|XLSX|xls|XLS)"
                ),
                skiprows=9,
            )
            self._dados_entrada.append(ideb)

    @staticmethod
    def extrai_turma(doc: Documento) -> str:
        """
        Extraí o ano do IDEB sendo processado como uma sigla
        AI = Anos iniciais
        AF = Anos finais
        EM = Ensino Médio

        :param doc: documento com dados do IDEB
        :return: string com sigla dos anos
        """
        if "anos_finais" in doc.nome:
            return "AF"
        elif "anos_iniciais" in doc.nome:
            return "AI"
        else:
            return "EM"

    @staticmethod
    def seleciona_dados(doc: Documento) -> pd.DataFrame:
        """
        Ajusta os dados do documento para extrair um data frame
        com apenas as colunas e linhas de interesse

        :param doc: dados de IDEB
        :return: data frame com dados de interesse
        """
        return (
            doc.data.iloc[:-3, :]
            .drop(
                columns=[
                    "SG_UF",
                    "CO_MUNICIPIO",
                    "NO_MUNICIPIO",
                    "NO_ESCOLA",
                    "REDE",
                ]
            )
            .assign(ID_ESCOLA=lambda f: f["ID_ESCOLA"].astype("uint32"))
        )

    @staticmethod
    def obtem_metricas(df: pd.DataFrame, turma: str) -> pd.DataFrame:
        """
        Processa o data frame com os dados do IDEB extraíndo o de-para
        entre o nome da coluna na tabela original, o nome da métrica e
        o ano dos dados

        :param df: dados de IDEB
        :param turma: anos sendo processado (AI, AF, EM)
        :return: data frame com de-para
        """
        proc = dict(COLUNA=list(), METRICA=list(), ANO=list())
        for c in df.columns[1:]:
            i = c.find("_20")

            # extraí o ano
            a = int(c[i + 1 : i + 5])

            # obtém a métrica registrada
            m = c[:i]

            # adiciona a informação da turma a métrica
            t = c.replace(f"{m}_{a}", "")
            if t.startswith("_"):
                if "SI" in t:
                    m = f"{m}_AF"
                else:
                    m = f"{m}_AF{t}"
            else:
                m = f"{m}_AF"

            # ajustado os dados do campo
            m = m.replace("VL_", "").replace("INDICADOR_", "")
            if m == f"OBSERVADO_{turma}":
                m = f"IDEB_{turma}"
            elif m == f"PROJECAO_{turma}":
                m = f"IDEB_META_{turma}"

            # adiciona a base
            proc["COLUNA"].append(c)
            proc["METRICA"].append(m)
            proc["ANO"].append(a)

        # transforma o de-para em um data frame
        return pd.DataFrame(proc)

    @staticmethod
    def formata_resultados(df: pd.DataFrame, dados: pd.DataFrame) -> pd.DataFrame:
        """
        Formata o dataframe para as saídas esperadas da base de IDEB

        :param df: data frame com os dados processados
        :param dados: de-para de coluna e métrica/ano
        :return: base de saída formatada
        """
        return (
            df.melt(id_vars="ID_ESCOLA", var_name="COLUNA", value_name="VALOR")
            .merge(dados)
            .assign(
                VALOR=lambda f: f["VALOR"]
                .astype(str)
                .str.replace("[*]", "")
                .str.replace("[,]", ".")
                .replace({"-": np.nan, "ND": np.nan})
                .astype("float32")
            )
            .pivot_table(
                index=["ID_ESCOLA", "ANO"],
                columns=["METRICA"],
                values="VALOR",
                aggfunc="first",
            )
            .reset_index()
        )

    @staticmethod
    def concatena_saidas(saidas: typing.List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concatena as bases de dados de IDEB

        :param saidas: lista com bases de IDEB
        :return: base concatenada única
        """
        df = None
        for s in saidas:
            if df is None:
                df = s
            else:
                df = df.merge(s, on=["ID_ESCOLA", "ANO"], how="outer")
        return df

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        saidas = list()
        for doc in self.dados_entrada:
            self._logger.info(f"Processando dados {doc}")

            # obtém o tipo de ano
            turma = self.extrai_turma(doc)

            # remove as colunas não utilizadas e converte os dados de código de escola
            df = self.seleciona_dados(doc)

            # extraí as métricas reportadas na base
            dados = self.obtem_metricas(df, turma)

            # realiza o melt dos dados e obtém os nomes de campo ajustados
            df = self.formata_resultados(df, dados)
            saidas.append(df)

        self._logger.info("Concatenando saídas")
        self._doc_saida.data = self.concatena_saidas(saidas)