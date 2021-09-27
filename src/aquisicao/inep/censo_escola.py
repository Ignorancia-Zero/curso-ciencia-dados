import typing

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao.inep.base_censo import BaseCensoEscolarETL
from src.utils.info import carrega_yaml


class EscolaETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de escola
    do censo escolar
    """

    _configs: typing.Dict[str, typing.Any]

    def __init__(self, entrada: str, saida: str, criar_caminho: bool = True) -> None:
        """
        Instância o objeto de ETL de dados de Escola

        :param entrada: string com caminho para pasta de entrada
        :param saida: string com caminho para pasta de saída
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(entrada, saida, "escolas", criar_caminho)
        self._configs = carrega_yaml("aquis_censo_escola.yml")

    def renomeia_colunas(self) -> None:
        """
        Renomea as colunas da base de entrada
        """
        for base in tqdm(self.dados_entrada.values()):
            base.rename(columns=self._configs["RENOMEIA_COLUNAS"], inplace=True)

    def dropa_colunas(self) -> None:
        """
        Remove colunas que são redundantes com outras bases
        """
        for base in tqdm(self.dados_entrada.values()):
            base.drop(
                columns=self._configs["DROPAR_COLUNAS"], inplace=True, errors="ignore"
            )

    def processa_dt(self) -> None:
        """
        Realiza a conversão das colunas de datas de texto para datetime
        """
        for base in tqdm(self.dados_entrada.values()):
            colunas_data = [c for c in base.columns if c.startswith("DT_")]
            for c in colunas_data:
                try:
                    base[c] = pd.to_datetime(base[c], format="%d/%m/%Y")
                except ValueError:
                    base[c] = pd.to_datetime(base[c], format="%d%b%Y:00:00:00")

    def processa_qt(self) -> None:
        """
        Realiza o processamento das colunas de quantidades
        """
        for k, base in tqdm(self.dados_entrada.items()):
            if k >= "2019.zip":
                for c in self._configs["COLS_88888"]:
                    if c in base:
                        base[c] = base[c].replace({88888: np.nan})

    def processa_in(self) -> None:
        """
        Realiza o processamento das colunas indicadoras
        """
        # gera a lista de todas as colunas existentes
        cols = set([c for d in self.dados_entrada.values() for c in d])

        for base in tqdm(self.dados_entrada.values()):
            # preenche bases com colunas IN quando há uma coluna QT
            for col in base:
                if (
                    col[:2] == "QT"
                    and f"IN{col[2:]}" in cols
                    and f"IN{col[2:]}" not in base
                ):
                    base[f"IN{col[2:]}"] = (base[col] > 0).astype("int")

            # realiza o tratamento das colunas IN_ a partir das configurações
            self.gera_coluna_por_comparacao(base, self._configs["TRATAMENTO_IN"])

            # cria a coluna IN_ENERGIA_OUTROS
            if "IN_ENERGIA_OUTROS" not in base and "IN_ENERGIA_INEXISTENTE" in base:
                base["IN_ENERGIA_OUTROS"] = (
                    (
                        base[
                            [
                                c
                                for c in base
                                if "IN_ENERGIA" in c and c != "IN_ENERGIA_INEXISTENTE"
                            ]
                        ].sum(axis=1)
                        == 0
                    )
                    & (base["IN_ENERGIA_INEXISTENTE"] == 0)
                ).astype("int")

            # cria a coluna IN_LOCAL_FUNC_GALPAO
            if "IN_LOCAL_FUNC_GALPAO" not in base and "TP_OCUPACAO_GALPAO" in base:
                base["IN_LOCAL_FUNC_GALPAO"] = np.where(
                    (base["TP_OCUPACAO_GALPAO"] > 0)
                    & (base["TP_OCUPACAO_GALPAO"] <= 3),
                    1,
                    np.where(base["TP_OCUPACAO_GALPAO"] == 0, 0, np.nan),
                )

            # cria a coluna IN_LINGUA_INDIGENA e IN_LINGUA_PORTUGUESA
            if "IN_LINGUA_INDIGENA" not in base and "TP_INDIGENA_LINGUA" in base:
                base["IN_LINGUA_INDIGENA"] = (
                    base["TP_INDIGENA_LINGUA"].isin([1, 3])
                ).astype("int")

            if "IN_LINGUA_PORTUGUESA" not in base and "TP_INDIGENA_LINGUA" in base:
                base["IN_LINGUA_PORTUGUESA"] = (
                    base["TP_INDIGENA_LINGUA"].isin([2, 3])
                ).astype("int")

            # corrige a coluna IN_BIBLIOTECA
            if "IN_SALA_LEITURA" not in base and "IN_BIBLIOTECA" in base:
                if "IN_BIBLIOTECA_SALA_LEITURA" in base:
                    base.drop(columns=["IN_BIBLIOTECA_SALA_LEITURA"], inplace=True)
                base.rename(
                    columns={"IN_BIBLIOTECA": "IN_BIBLIOTECA_SALA_LEITURA"},
                    inplace=True,
                )

            # cria a coluna IN_AGUA_POTAVEL
            if "IN_AGUA_POTAVEL" not in base and "IN_AGUA_FILTRADA" in base:
                base["IN_AGUA_POTAVEL"] = ((base["IN_AGUA_FILTRADA"] == 2)).astype(
                    "int"
                )

            # substítui o valor 9 pelo valor nulo
            for c in base:
                if c.startswith("IN_"):
                    base[c] = base[c].replace({9: np.nan})

    def processa_tp(self) -> None:
        """
        Realiza o processamento das colunas de tipo
        """
        for k, base in tqdm(self.dados_entrada.items()):
            # cria a coluna TP_INDIGENA_LINGUA
            if (
                "TP_INDIGENA_LINGUA" not in base
                and "IN_LINGUA_INDIGENA" in base
                and "IN_LINGUA_PORTUGUESA" in base
            ):
                base["TP_INDIGENA_LINGUA"] = np.where(
                    (base["IN_LINGUA_INDIGENA"] == 1)
                    & (base["IN_LINGUA_PORTUGUESA"] == 0),
                    1,
                    np.where(
                        (base["IN_LINGUA_INDIGENA"] == 0)
                        & (base["IN_LINGUA_PORTUGUESA"] == 1),
                        2,
                        np.where(
                            (base["IN_LINGUA_INDIGENA"] == 1)
                            & (base["IN_LINGUA_PORTUGUESA"] == 1),
                            3,
                            np.where(
                                (
                                    base["TP_SITUACAO_FUNCIONAMENTO"]
                                    .astype("str")
                                    .isin(["1", "1.0", "EM ATIVIDADE"])
                                )
                                & (base["IN_EDUCACAO_INDIGENA"] == 0),
                                0,
                                np.nan,
                            ),
                        ),
                    ),
                )

            # Corrige o campo de TP_OCUPACAO_GALPAO
            if "TP_OCUPACAO_GALPAO" in base:
                if base["TP_OCUPACAO_GALPAO"].max() == 1:
                    base.drop(columns={"TP_OCUPACAO_GALPAO"}, inplace=True)

            # Corrige o campo de TP_OCUPACAO_PREDIO_ESCOLAR
            if "TP_OCUPACAO_PREDIO_ESCOLAR" in base:
                if base["TP_OCUPACAO_PREDIO_ESCOLAR"].max() == 1:
                    base.drop(columns={"TP_OCUPACAO_PREDIO_ESCOLAR"}, inplace=True)

            # converte a coluna para tipo categórico
            for c, d in self._configs["DEPARA_TP"].items():
                if c in base:
                    # lista os valores a serem categorizados
                    vals = list(d.values())

                    # obtém os valores
                    unicos = set(base[c].dropna().replace(d))
                    esperado = set(vals)

                    # verifica que não há nenhum erro com os dados a serem preenchidos
                    if not esperado.issuperset(unicos):
                        raise ValueError(
                            f"A coluna {c} da base {k} possuí os valores "
                            f"{unicos - esperado} a mais"
                        )

                    # cria o tipo categórico
                    if c in self._configs["PREENCHER_TP"]:
                        vals += [self._configs["PREENCHER_TP"][c]]
                    cat = pd.Categorical(vals).dtype

                    # realiza a conversão da coluna
                    base[c] = base[c].replace(d).astype(cat)

            # vamos converter as variáveis indicadores de escolas particulares
            # em variáveis TP, uma vez que teremos uma terceira opção informando
            # que a escola é pública
            for c in self._configs["COLS_PARTICULAR"]:
                if c in base:
                    base.rename(columns={c: f"TP{c[2:]}"}, inplace=True)
                    base[f"TP{c[2:]}"] = base[f"TP{c[2:]}"].replace(
                        {0: "NÃO", 1: "SIM"}
                    )
                    base[f"TP{c[2:]}"] = np.where(
                        (base["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                        & (base["TP_DEPENDENCIA"] == "PRIVADA"),
                        base[f"TP{c[2:]}"],
                        np.where(
                            (base["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE")
                            & ~(base["TP_DEPENDENCIA"] == "PRIVADA"),
                            base[f"TP{c[2:]}"].fillna("PÚBLICA"),
                            np.nan,
                        ),
                    )
                    base[f"TP{c[2:]}"] = base[f"TP{c[2:]}"].astype("category")

    def concatena_bases(self) -> None:
        """
        Concatena as bases de dados nas saídas temporal e atemporal
        """
        self._dados_saida = dict()

        # cria a base de dados temporal
        self._dados_saida["escola_temp"] = pd.concat(
            [
                base.loc[
                    lambda f: f["TP_SITUACAO_FUNCIONAMENTO"] == "EM ATIVIDADE"
                ].drop(
                    columns=self._configs["COLS_ATEMPORAL"]
                    + ["TP_SITUACAO_FUNCIONAMENTO"],
                    errors="ignore",
                )
                for base in self.dados_entrada.values()
            ]
        )

        # cria a base de dados atemporal
        self._dados_saida["escola_atemp"] = pd.concat(
            [
                base.reindex(columns=["CO_ENTIDADE"] + self._configs["COLS_ATEMPORAL"])
                for base in self.dados_entrada.values()
            ]
        ).drop_duplicates(subset=["CO_ENTIDADE"], keep="last")

    def preenche_nulos(self) -> None:
        """
        Realiza o preenchimento de valores nulos dentro da base temporal
        """
        # faz o sorting e reset index
        self._dados_saida["escola_temp"].sort_values(
            by=["CO_ENTIDADE", "NU_ANO_CENSO"], inplace=True
        )
        self._dados_saida["escola_temp"].reset_index(drop=True, inplace=True)

        # preenchimento com valores históricos
        for c in self._configs["COLS_FBFILL"]:
            self._dados_saida["escola_temp"][c] = (
                self._dados_saida["escola_temp"]
                .groupby(["CO_ENTIDADE"])[c]
                .ffill()
                .values
            )
            self._dados_saida["escola_temp"][c] = (
                self._dados_saida["escola_temp"]
                .groupby(["CO_ENTIDADE"])[c]
                .bfill()
                .values
            )

        # remove colunas que são redundantes
        self._dados_saida["escola_temp"].drop(
            columns=self._configs["REMOVER_COLS"], inplace=True
        )

        # corrige variáveis de escolas privadas
        for c in self._configs["COLS_PARTICULAR"]:
            c = c if c.startswith("TP") else f"TP{c[2:]}"
            self._dados_saida["escola_temp"][c] = np.where(
                (self._dados_saida["escola_temp"][c] == "PÚBLICA")
                & (self._dados_saida["escola_temp"]["TP_DEPENDENCIA"] == "PRIVADA"),
                np.nan,
                self._dados_saida["escola_temp"][c],
            )

        # corrige a ocupação de galpão
        self._dados_saida["escola_temp"]["TP_OCUPACAO_GALPAO"] = np.where(
            ~(self._dados_saida["escola_temp"]["TP_OCUPACAO_GALPAO"] == "NÃO")
            & ~(self._dados_saida["escola_temp"]["IN_LOCAL_FUNC_GALPAO"] == 0),
            np.nan,
            self._dados_saida["escola_temp"]["TP_OCUPACAO_GALPAO"],
        )

        # preenche nulos com valores fixos
        for c, p in self._configs["PREENCHER_NULOS"].items():
            self._dados_saida["escola_temp"][c] = self._dados_saida["escola_temp"][
                c
            ].fillna(p)

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        self.logger.info("Renomeando colunas das bases de escola")
        self.renomeia_colunas()

        self.logger.info("Dropando colunas redundantes com outras bases")
        self.dropa_colunas()

        self.logger.info("Processando colunas com datas")
        self.processa_dt()

        self.logger.info("Processando colunas QT_")
        self.processa_qt()

        self.logger.info("Processando colunas IN_")
        self.processa_in()

        self.logger.info("Processando colunas TP_")
        self.processa_tp()

        self.logger.info("Concatenando bases em saídas únicas")
        self.concatena_bases()

        self.logger.info("Ajustando valores nulos")
        self.preenche_nulos()
