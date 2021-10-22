import typing

import numpy as np

from src.aquisicao.inep._censo_escolar import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento


class DocenteETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de docente do
    censo escolar
    """

    _documentos_saida: typing.List[Documento]

    def __init__(
        self,
        ds: DataStore,
        criar_caminho: bool = True,
        reprocessar: bool = False,
        ano: typing.Union[int, str] = "ultimo",
    ) -> None:
        """
        Instância o objeto de ETL de dados de Docente

        :param ds: instância de objeto data store
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        """
        super().__init__(
            ds,
            "docentes",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )

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
                    referencia=dict(CatalogoAquisicao.DOCENTE),
                ),
                Documento(
                    ds=self._ds,
                    referencia=dict(CatalogoAquisicao.DOCENTE_TURMA),
                ),
            ]
        return self._documentos_saida

    def processa_in(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: documento com os dados a serem tratados
        """
        super(DocenteETL, self).processa_in(base)

        if (
            "IN_INTERCULTURAL_OUTROS" in base.data
            and "IN_ESPECIFICO_OUTROS" not in base.data
        ):
            base.data.rename(
                columns={"IN_INTERCULTURAL_OUTROS": "IN_ESPECIFICO_OUTROS"},
                inplace=True,
            )

    def processa_tp(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: documento com os dados a serem tratados
        """
        if "TP_ESCOLARIDADE" not in base.data and "TP_ESCOLARIDADE_0" in base.data:
            base.data["TP_ESCOLARIDADE"] = np.where(
                base.data["TP_ESCOLARIDADE_0"] <= 2,
                base.data["TP_ESCOLARIDADE_0"],
                np.where(
                    base.data["TP_ESCOLARIDADE_0"].isin([3, 4, 5]),
                    3,
                    np.where(base.data["TP_ESCOLARIDADE_0"] == 6, 4, np.nan),
                ),
            )

        if "TP_ENSINO_MEDIO" not in base.data and "TP_ESCOLARIDADE_0" in base.data:
            base.data["TP_ENSINO_MEDIO"] = np.where(
                base.data["TP_ESCOLARIDADE_0"] == 5,
                1,
                np.where(
                    base.data["TP_ESCOLARIDADE_0"] == 3,
                    2,
                    np.where(base.data["TP_ESCOLARIDADE_0"] == 4, 4, 9),
                ),
            )

        if "TP_TIPO_DOCENTE" in base.data:
            if base.data["TP_TIPO_DOCENTE"].min() == 0:
                base.data["TP_TIPO_DOCENTE"] += 1

        super(DocenteETL, self).processa_tp(base)
