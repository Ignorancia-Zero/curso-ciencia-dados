import typing

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao.inep.base_censo import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.info import carrega_yaml


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
    ) -> None:
        """
        Instância o objeto de ETL de dados de Escola

        :param ds: instância de objeto data store
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(ds, "escolas", ano=ano, criar_caminho=criar_caminho)
        self._configs = carrega_yaml("aquis_censo_escola.yml")

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
                    referencia=CatalogoAquisicao.CENSO_ESCOLA,
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

    def processa_dt(self, base: Documento) -> None:
        """
        Realiza a conversão das colunas de datas de texto para datetime

        :param base: documento com os dados a serem tratados
        """
        colunas_data = [c for c in base.data.columns if c.startswith("DT_")]
        for c in colunas_data:
            try:
                base.data[c] = pd.to_datetime(base.data[c], format="%d/%m/%Y")
            except ValueError:
                base.data[c] = pd.to_datetime(base.data[c], format="%d%b%Y:00:00:00")

    def processa_qt(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas de quantidades

        :param base: documento com os dados a serem tratados
        """
        if base.nome >= "2019.zip":
            for c in self._configs["COLS_88888"]:
                if c in base.data:
                    base.data[c] = base.data[c].replace({88888: np.nan})

    def processa_in(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas indicadoras

        :param base: documento com os dados a serem tratados
        """
        # gera a lista de todas as colunas existentes
        in_atual = [c for c in base.data if c.startswith("IN_")]
        cols = set(self._configs["IN_COLS"] + in_atual)
        dif = set(in_atual) - set(self._configs["IN_COLS"])
        if len(dif) > 0:
            self._logger.warning(
                f"Há novas colunas IN que foram adicionadas -> {dif}"
                f"\nConsidere adiciona-las ao info/aquis_censo_escola.yml"
            )

        # preenche bases com colunas IN quando há uma coluna QT
        for col in base.data:
            if (
                col[:2] == "QT"
                and f"IN{col[2:]}" in cols
                and f"IN{col[2:]}" not in base.data
            ):
                base.data[f"IN{col[2:]}"] = (base.data[col] > 0).astype("int")

        # realiza o tratamento das colunas IN_ a partir das configurações
        self.gera_coluna_por_comparacao(base.data, self._configs["TRATAMENTO_IN"])

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
                if c in self._configs["PREENCHER_TP"]:
                    vals += [self._configs["PREENCHER_TP"][c]]
                cat = pd.Categorical(vals).dtype

                # realiza a conversão da coluna
                base.data[c] = base.data[c].replace(d).astype(cat)

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
        escola = self._dados_saida[0].data

        # faz o sorting e reset index
        escola.sort_values(by=["CO_ENTIDADE", "ANO"], inplace=True)
        escola.reset_index(drop=True, inplace=True)

        # preenchimento com valores históricos
        for c in self._configs["COLS_FBFILL"]:
            if c in escola:
                escola[c] = escola.groupby(["CO_ENTIDADE"])[c].ffill().values
                escola[c] = escola.groupby(["CO_ENTIDADE"])[c].bfill().values

        # remove colunas que são redundantes
        escola.drop(
            columns=self._configs["REMOVER_COLS"], inplace=True, errors="ignore"
        )

        # corrige variáveis de escolas privadas
        for c in self._configs["COLS_PARTICULAR"]:
            c = c if c.startswith("TP") else f"TP{c[2:]}"
            if c in escola:
                escola[c] = np.where(
                    (escola[c] == "PÚBLICA") & (escola["TP_DEPENDENCIA"] == "PRIVADA"),
                    np.nan,
                    escola[c],
                )

        # corrige a ocupação de galpão
        if "TP_OCUPACAO_GALPAO" in escola:
            # corrige a ocupação de galpão
            escola["TP_OCUPACAO_GALPAO"] = np.where(
                (
                    (escola["TP_OCUPACAO_GALPAO"] == "NÃO")
                    & (escola["IN_LOCAL_FUNC_GALPAO"] == 1)
                )
                | (
                    (escola["TP_OCUPACAO_GALPAO"] != "NÃO")
                    & (escola["IN_LOCAL_FUNC_GALPAO"] == 0)
                ),
                np.nan,
                escola["TP_OCUPACAO_GALPAO"],
            )

        # garante que todas as colunas existam
        escola = escola.reindex(columns=self._configs["DS_SCHEMA"])

        # preenche nulos com valores fixos
        for c, p in self._configs["PREENCHER_NULOS"].items():
            if c in escola:
                escola[c] = escola[c].fillna(p)

        # ajusta o schema
        for c, dtype in self._configs["DS_SCHEMA"].items():
            if dtype.startswith("pd."):
                escola[c] = escola[c].astype(eval(dtype))
            else:
                escola[c] = escola[c].astype(dtype)

        self._dados_saida[0].data = escola

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        self._logger.info("Processando bases de entrada")
        for base in tqdm(self.dados_entrada):
            self.renomeia_colunas(base)
            self.dropa_colunas(base)
            self.processa_dt(base)
            self.processa_qt(base)
            self.processa_in(base)
            self.processa_tp(base)

        self._logger.info("Concatenando bases de dados")
        self.concatena_bases()

        self._logger.info("Realizando ajustes finais na base")
        self.ajustes_finais()
