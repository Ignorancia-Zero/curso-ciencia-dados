import typing

import numpy as np

from src.aquisicao.inep.base_censo import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento


class EscolaETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de escola
    do censo escolar
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
        Instância o objeto de ETL de dados de Escola

        :param ds: instância de objeto data store
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            ds, "escolas", ano=ano, criar_caminho=criar_caminho, reprocessar=reprocessar
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
                    referencia=dict(CatalogoAquisicao.ESCOLA),
                ),
            ]
        return self._documentos_saida

    def processa_in(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: documento com os dados a serem tratados
        """
        super(EscolaETL, self).processa_in(base)

        # cria a coluna IN_ENERGIA_OUTROS
        if (
            "IN_ENERGIA_OUTROS" not in base.data
            and "IN_ENERGIA_INEXISTENTE" in base.data
        ):
            base.data["IN_ENERGIA_OUTROS"] = (
                (
                    base.data[
                        [
                            c
                            for c in base.data
                            if "IN_ENERGIA" in c and c != "IN_ENERGIA_INEXISTENTE"
                        ]
                    ].sum(axis=1)
                    == 0
                )
                & (base.data["IN_ENERGIA_INEXISTENTE"] == 0)
            ).astype("int")

        # cria a coluna IN_LOCAL_FUNC_GALPAO
        if (
            "IN_LOCAL_FUNC_GALPAO" not in base.data
            and "TP_OCUPACAO_GALPAO" in base.data
        ):
            base.data["IN_LOCAL_FUNC_GALPAO"] = np.where(
                (base.data["TP_OCUPACAO_GALPAO"] > 0)
                & (base.data["TP_OCUPACAO_GALPAO"] <= 3),
                1,
                np.where(base.data["TP_OCUPACAO_GALPAO"] == 0, 0, np.nan),
            )

        # cria a coluna IN_LINGUA_INDIGENA e IN_LINGUA_PORTUGUESA
        if "IN_LINGUA_INDIGENA" not in base.data and "TP_INDIGENA_LINGUA" in base.data:
            base.data["IN_LINGUA_INDIGENA"] = (
                base.data["TP_INDIGENA_LINGUA"].isin([1, 3])
            ).astype("int")

        if (
            "IN_LINGUA_PORTUGUESA" not in base.data
            and "TP_INDIGENA_LINGUA" in base.data
        ):
            base.data["IN_LINGUA_PORTUGUESA"] = (
                base.data["TP_INDIGENA_LINGUA"].isin([2, 3])
            ).astype("int")

        # corrige a coluna IN_BIBLIOTECA
        if "IN_SALA_LEITURA" not in base.data and "IN_BIBLIOTECA" in base.data:
            if "IN_BIBLIOTECA_SALA_LEITURA" in base.data:
                base.data.drop(columns=["IN_BIBLIOTECA_SALA_LEITURA"], inplace=True)
            base.data.rename(
                columns={"IN_BIBLIOTECA": "IN_BIBLIOTECA_SALA_LEITURA"},
                inplace=True,
            )

        # cria a coluna IN_AGUA_POTAVEL
        if "IN_AGUA_POTAVEL" not in base.data and "IN_AGUA_FILTRADA" in base.data:
            base.data["IN_AGUA_POTAVEL"] = (
                (base.data["IN_AGUA_FILTRADA"] == 2)
            ).astype("int")

        # substítui o valor 9 pelo valor nulo
        for c in base.data:
            if c.startswith("IN_"):
                base.data[c] = base.data[c].replace({9: np.nan})

    def processa_tp(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: documento com os dados a serem tratados
        """
        # cria a coluna TP_INDIGENA_LINGUA
        if (
            "TP_INDIGENA_LINGUA" not in base.data
            and "IN_LINGUA_INDIGENA" in base.data
            and "IN_LINGUA_PORTUGUESA" in base.data
        ):
            base.data["TP_INDIGENA_LINGUA"] = np.where(
                (base.data["IN_LINGUA_INDIGENA"] == 1)
                & (base.data["IN_LINGUA_PORTUGUESA"] == 0),
                1,
                np.where(
                    (base.data["IN_LINGUA_INDIGENA"] == 0)
                    & (base.data["IN_LINGUA_PORTUGUESA"] == 1),
                    2,
                    np.where(
                        (base.data["IN_LINGUA_INDIGENA"] == 1)
                        & (base.data["IN_LINGUA_PORTUGUESA"] == 1),
                        3,
                        np.where(
                            (
                                base.data["TP_SITUACAO_FUNCIONAMENTO"]
                                .astype("str")
                                .isin(["1", "1.0", "EM ATIVIDADE"])
                            )
                            & (base.data["IN_EDUCACAO_INDIGENA"] == 0),
                            0,
                            np.nan,
                        ),
                    ),
                ),
            )

        # Corrige o campo de TP_OCUPACAO_GALPAO
        if "TP_OCUPACAO_GALPAO" in base.data:
            if base.data["TP_OCUPACAO_GALPAO"].max() == 1:
                base.data.drop(columns={"TP_OCUPACAO_GALPAO"}, inplace=True)

        # Corrige o campo de TP_OCUPACAO_PREDIO_ESCOLAR
        if "TP_OCUPACAO_PREDIO_ESCOLAR" in base.data:
            if base.data["TP_OCUPACAO_PREDIO_ESCOLAR"].max() == 1:
                base.data.drop(columns={"TP_OCUPACAO_PREDIO_ESCOLAR"}, inplace=True)

        # converte a coluna para tipo categórico
        super(EscolaETL, self).processa_tp(base)

        # vamos converter as variáveis indicadores de escolas particulares
        # em variáveis TP, uma vez que teremos uma terceira opção informando
        # que a escola é pública
        for c in self._configs["COLS_PARTICULAR"]:
            if c in base.data:
                base.data.rename(columns={c: f"TP{c[2:]}"}, inplace=True)
                base.data[f"TP{c[2:]}"] = base.data[f"TP{c[2:]}"].replace(
                    {0: "NÃO", 1: "SIM"}
                )
                base.data[f"TP{c[2:]}"] = np.where(
                    (base.data["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                    & (base.data["TP_DEPENDENCIA"] == "PRIVADA"),
                    base.data[f"TP{c[2:]}"],
                    np.where(
                        (base.data["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                        & ~(base.data["TP_DEPENDENCIA"] == "PRIVADA"),
                        base.data[f"TP{c[2:]}"].fillna("PÚBLICA"),
                        np.nan,
                    ),
                )
                base.data[f"TP{c[2:]}"] = base.data[f"TP{c[2:]}"].astype("category")

    def ajusta_schema(
        self,
        base: Documento,
        fill: typing.Dict[str, typing.Any],
        schema: typing.Dict[str, str],
    ) -> None:
        """
        Modifica o schema de uma base para bater com as configurações

        :param base: documento com os dados a serem modificados
        :param fill: dicionário de preenchimento por coluna
        :param schema: dicionário de tipo de dados por coluna
        """
        # corrige variáveis de escolas privadas
        for c in self._configs["COLS_PARTICULAR"]:
            c = c if c.startswith("TP") else f"TP{c[2:]}"
            if c in base.data:
                base.data[c] = np.where(
                    (base.data[c] == "PÚBLICA")
                    & (base.data["TP_DEPENDENCIA"] == "PRIVADA"),
                    np.nan,
                    base.data[c],
                )

        # corrige a ocupação de galpão
        if "TP_OCUPACAO_GALPAO" in base.data:
            # corrige a ocupação de galpão
            base.data["TP_OCUPACAO_GALPAO"] = np.where(
                (
                    (base.data["TP_OCUPACAO_GALPAO"] == "NÃO")
                    & (base.data["IN_LOCAL_FUNC_GALPAO"] == 1)
                )
                | (
                    (base.data["TP_OCUPACAO_GALPAO"] != "NÃO")
                    & (base.data["IN_LOCAL_FUNC_GALPAO"] == 0)
                ),
                np.nan,
                base.data["TP_OCUPACAO_GALPAO"],
            )

        # garante que todas as colunas existam
        super(EscolaETL, self).ajusta_schema(base, fill, schema)
