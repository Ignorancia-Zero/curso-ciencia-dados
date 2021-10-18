import typing

import pandas as pd
from tqdm import tqdm

from src.aquisicao.inep.base_censo import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.info import carrega_yaml


class DocenteETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de docente do
    censo escolar
    """

    _configs: typing.Dict[str, typing.Any]
    _documentos_saida: typing.List[Documento]

    def __init__(
        self,
        ds: DataStore,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Docente

        :param ds: instância de objeto data store
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(ds, "docentes", ano=ano, criar_caminho=criar_caminho)
        self._configs = carrega_yaml("aquis_censo_docente.yml")

    @property
    def documentos_saida(self) -> typing.List[Documento]:
        """
        Gera a lista de documentos de saída

        :return: lista de documentos de saída
        """
        if not hasattr(self, "_documentos_saida"):
            self._documentos_saida = [
                Documento(
                    ds=self._ds,
                    referencia=dict(CatalogoAquisicao.CENSO_DOCENTE),
                ),
            ]
        return self._documentos_saida

    def renomeia_colunas(self, base: Documento) -> None:
        """
        Renomea as colunas da base de entrada

        :param base: documento com os dados a serem tratados
        """
        base.data.rename(columns=self._configs["RENOMEIA_COLUNAS"], inplace=True)

    def gera_dt_nascimento(self, base: Documento) -> None:
        """
        Cria a coluna de data de nascimento do gestor

        :param base: documento com os dados a serem tratados
        """
        base.data["DT_NASCIMENTO"] = pd.to_datetime(
            base.data["NU_ANO"] * 100 + base.data["NU_MES"], format="%Y%m"
        )

    def dropa_colunas(self, base: Documento) -> None:
        """
        Remove colunas que são redundantes com outras bases

        :param base: documento com os dados a serem tratados
        """
        base.data.drop(
            columns=self._configs["DROPAR_COLUNAS"], inplace=True, errors="ignore"
        )

    def processa_tp(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: documento com os dados a serem tratados
        """
        # converte a coluna para tipo categórico
        for c, d in self._configs["DEPARA_TP"].items():
            if c in base.data:
                # lista os valores a serem categorizados
                vals = list(d.values())

                # obtém os valores
                unicos = set(base.data[c].dropna().replace(d))
                esperado = set(vals)

                # verifica que não há nenhum erro com os dados a serem preenchidos
                if not esperado.issuperset(unicos):
                    raise ValueError(
                        f"A coluna {c} da base {base.nome} possuí os valores "
                        f"{unicos - esperado} a mais"
                    )

                # cria o tipo categórico
                cat = pd.Categorical(vals).dtype

                # realiza a conversão da coluna
                base.data[c] = base.data[c].replace(d).astype(cat)

    def concatena_bases(self) -> None:
        """
        Concatena as bases de dados em uma saída única
        """
        self._dados_saida = list()
        self.documentos_saida[0].data = pd.concat(
            [base.data for base in self.dados_entrada]
        )
        self._dados_saida += self.documentos_saida

    def ajustes_finais(self) -> None:
        """
        Realiza os ajuste finais a base de dados gerada
        """
        docente = self._dados_saida[0].data

        # faz o sorting e reset index
        docente.sort_values(by=["ID_DOCENTE", "ANO"], inplace=True)
        docente.reset_index(drop=True, inplace=True)

        # garante que todas as colunas existam
        rm = set(docente) - set(self._configs["DS_SCHEMA"])
        ad = set(self._configs["DS_SCHEMA"]) - set(docente)
        if len(rm) > 0:
            self._logger.warning(f"As colunas {rm} serão removidas do data set")
        if len(ad) > 0:
            self._logger.warning(f"As colunas {ad} serão adicionadas do data set")
        docente = docente.reindex(columns=self._configs["DS_SCHEMA"])

        # preenche nulos com valores fixos
        for c, p in self._configs["PREENCHER_NULOS"].items():
            if c in docente:
                docente[c] = docente[c].fillna(p)

        # ajusta o schema
        for c, dtype in self._configs["DS_SCHEMA"].items():
            if dtype.startswith("pd."):
                docente[c] = docente[c].astype(eval(dtype))
            else:
                docente[c] = docente[c].astype(dtype)

        self._dados_saida[0].data = docente

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        self._logger.info("Processando bases de entrada")
        for base in tqdm(self.dados_entrada):
            self.renomeia_colunas(base)
            self.gera_dt_nascimento(base)
            self.dropa_colunas(base)
            self.processa_tp(base)

        self._logger.info("Concatenando bases de dados")
        self.concatena_bases()

        self._logger.info("Realizando ajustes finais na base")
        self.ajustes_finais()
