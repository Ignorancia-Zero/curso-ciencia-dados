import typing

import numpy as np

from src.aquisicao.inep.base_censo import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento


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
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Turma

        :param ds: instância de objeto data store
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            ds, "turmas", ano=ano, criar_caminho=criar_caminho, reprocessar=reprocessar
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
                    referencia=dict(CatalogoAquisicao.TURMA),
                ),
            ]
        return self._documentos_saida

    def processa_in(self, base: Documento) -> None:
        """
        Gera colunas IN_ que não existiam em bases mais antigas

        :param base: documento a ser processado
        """
        super(TurmaETL, self).processa_in(base)

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
        super(TurmaETL, self).processa_tp(base)
