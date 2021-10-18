import typing

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao.inep.base_censo import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.info import carrega_yaml


class TurmaETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de turma do
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
        Instância o objeto de ETL de dados de Turma

        :param ds: instância de objeto data store
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(ds, "turmas", ano=ano, criar_caminho=criar_caminho)
        self._configs = carrega_yaml("aquis_censo_turma.yml")

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
                    referencia=dict(CatalogoAquisicao.CENSO_TURMA),
                ),
            ]
        return self._documentos_saida

    def renomeia_colunas(self, base: Documento) -> None:
        """
        Renomea as colunas da base de entrada

        :param base: documento com os dados a serem tratados
        """
        base.data.rename(columns=self._configs["RENOMEIA_COLUNAS"], inplace=True)

    def dropa_colunas(self, base: Documento) -> None:
        """
        Remove colunas que são redundantes com outras bases

        :param base: documento com os dados a serem tratados
        """
        base.data.drop(
            columns=self._configs["DROPAR_COLUNAS"], inplace=True, errors="ignore"
        )

    def processa_in(self, base: Documento) -> None:
        """
        Gera colunas IN_ que não existiam em bases mais antigas

        :param base: documento a ser processado
        """
        if "IN_DISC_EST_SOCIAIS_SOCIOLOGIA" not in base.data:
            base.data["IN_DISC_EST_SOCIAIS_SOCIOLOGIA"] = (
                base.data.reindex(
                    columns=["IN_DISC_SOCIOLOGIA", "IN_DISC_ESTUDOS_SOCIAIS"]
                ).sum(axis=1)
                > 0
            ).astype("int")

        if "IN_ESPECIAL_EXCLUSIVA" not in base.data:
            if "TP_MOD_ENSINO" in base.data:
                base.data["IN_ESPECIAL_EXCLUSIVA"] = (
                    base.data["TP_MOD_ENSINO"] == 2
                ).astype("int")

    def processa_tp(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: documento com os dados a serem tratados
        """
        # gera a coluna de TP_MEDIACAO_DIDATICO_PEDAGO
        if "TP_MEDIACAO_DIDATICO_PEDAGO" not in base.data:
            base.data["TP_MEDIACAO_DIDATICO_PEDAGO"] = np.where(
                base.data["CO_ETAPA_ENSINO"].isin([46, 47, 48, 53, 54, 55, 58, 61, 63]),
                2,
                np.where(base.data["CO_ETAPA_ENSINO"].notnull(), 1, np.nan),
            )

        # gera a coluna de TP_TIPO_ATENDIMENTO_TURMA
        if "TP_TIPO_ATENDIMENTO_TURMA" not in base.data:
            if "TP_MOD_ENSINO" in base.data:
                regular = base.data["TP_MOD_ENSINO"] == 1
            elif "IN_REGULAR" in base.data:
                regular = base.data["IN_REGULAR"] == 1
            else:
                regular = None

            if "NU_DIAS_ATIVIDADE" in base.data:
                ae = base.data["NU_DIAS_ATIVIDADE"].isin([1, 2, 3, 4, 5, 6, 7])
            elif "CO_TIPO_ATIVIDADE_1" in base.data:
                ae = base.data["CO_TIPO_ATIVIDADE_1"].notnull()
            elif "TP_TIPO_TURMA" in base.data:
                ae = base.data["TP_TIPO_TURMA"] == 4
            else:
                ae = None

            if "TP_MOD_ENSINO" in base.data:
                especial = base.data["TP_MOD_ENSINO"] == 2
            elif "IN_ESPECIAL_EXCLUSIVA" in base.data:
                especial = base.data["IN_ESPECIAL_EXCLUSIVA"] == 1
            elif "IN_DISC_ATENDIMENTO_ESPECIAIS" in base.data:
                especial = base.data["IN_DISC_ATENDIMENTO_ESPECIAIS"] == 1
            elif "TP_TIPO_TURMA" in base.data and base.data["TP_TIPO_TURMA"].max() >= 5:
                especial = base.data["TP_TIPO_TURMA"] == 5
            else:
                especial = None

            if (regular is not None) and (ae is not None) and (especial is not None):
                base.data["TP_TIPO_ATENDIMENTO_TURMA"] = np.where(
                    especial,
                    4,
                    np.where(
                        regular & ae, 2, np.where(ae, 3, np.where(regular, 1, np.nan))
                    ),
                )

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
        turma = self._dados_saida[0].data

        # faz o sorting e reset index
        turma.sort_values(by=["ID_TURMA", "ANO"], inplace=True)
        turma.reset_index(drop=True, inplace=True)

        # garante que todas as colunas existam
        rm = set(turma) - set(self._configs["DS_SCHEMA"])
        ad = set(self._configs["DS_SCHEMA"]) - set(turma)
        if len(rm) > 0:
            self._logger.warning(f"As colunas {rm} serão removidas do data set")
        if len(ad) > 0:
            self._logger.warning(f"As colunas {ad} serão adicionadas do data set")
        turma = turma.reindex(columns=self._configs["DS_SCHEMA"])

        # preenche nulos com valores fixos
        for c, p in self._configs["PREENCHER_NULOS"].items():
            if c in turma:
                turma[c] = turma[c].fillna(p)

        # ajusta o schema
        for c, dtype in self._configs["DS_SCHEMA"].items():
            if dtype.startswith("pd."):
                turma[c] = turma[c].astype(eval(dtype))
            else:
                turma[c] = turma[c].astype(dtype)

        self._dados_saida[0].data = turma

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        self._logger.info("Processando bases de entrada")
        for base in tqdm(self.dados_entrada):
            self.renomeia_colunas(base)
            self.dropa_colunas(base)
            self.processa_in(base)
            self.processa_tp(base)

        self._logger.info("Concatenando bases de dados")
        self.concatena_bases()

        self._logger.info("Realizando ajustes finais na base")
        self.ajustes_finais()
