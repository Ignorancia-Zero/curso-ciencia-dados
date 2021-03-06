import abc
import re
import typing

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.aquisicao.inep._micro_inep import BaseINEPETL
from src.io.data_store import DataStore
from src.io.data_store import Documento
from src.utils.info import carrega_excel
from src.utils.info import carrega_yaml


class BaseCensoEscolarETL(BaseINEPETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do CensoEscolar
    """

    _ano: typing.Union[str, int]
    _tabela: str
    _configs: typing.Dict[str, typing.Any]
    _regioes: str
    _carrega_cols: typing.List[str]
    _dtype: typing.Dict[str, str]
    _rename: typing.Dict[str, str]
    _cols_in: typing.List[str]

    def __init__(
        self,
        ds: DataStore,
        tabela: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
        regioes: typing.Sequence[str] = ("CO", "NORDESTE", "NORTE", "SUDESTE", "SUL"),
    ) -> None:
        """
        Instância o objeto de ETL Censo Escolar

        :param ds: instância de objeto data store
        :param tabela: Tabela do censo escolar a ser processada
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        :param regioes: lista de regiões que devem ser processadas
        """
        super().__init__(
            ds,
            "censo-escolar",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )
        self._tabela = tabela

        # carrega o arquivo YAML de configurações
        self._configs = carrega_yaml(f"aquis_censo_{tabela}.yml")

        # gera um regex de seleção de regiões
        self._regioes = "|".join([f"_{r.lower()}|_{r.upper()}" for r in regioes])

        # obtém a lista de colunas que devem ser extraídas da base
        ano = self.ano
        while ano > 2006:
            try:
                self._carrega_cols = (
                    carrega_excel(
                        f"aquis_censo_{tabela}_cols.xlsx", sheet_name=str(ano)
                    )
                    .loc[lambda f: f["USAR"] == 1, "COLUNA"]
                    .to_list()
                )
            except NameError:
                self._logger.warning(
                    f"Ano {ano} não encontrado na configuração de colunas, utilizando ano anteiror"
                )
                ano -= 1
            else:
                break

        # obtém o de-para de tipo e de nome
        dtype = carrega_excel(f"aquis_censo_{tabela}_cols.xlsx", sheet_name="dtype")
        self._dtype = dict(zip(dtype["COLUNA"].to_list(), dtype["DTYPE"].to_list()))
        self._rename = dict(zip(dtype["COLUNA"].to_list(), dtype["RENAME"].to_list()))

        # gera uma lista com todas as colunas do tipo IN_
        self._cols_in = (
            dtype.loc[lambda f: f["RENAME"].str.startswith("IN_"), "RENAME"]
            .drop_duplicates()
            .to_list()
        )

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # realiza o download dos dados do censo
        self.download_conteudo()

        # inicializa os dados de entrada como um dicionário vazio
        self._dados_entrada = list()

        # para cada arquivo do censo demográfico
        for censo in tqdm(self.documentos_entrada):
            conf = dict(
                como_df=True,
                padrao_comp=(
                    f"({self._tabela.lower()}|{self._tabela.upper()}|{self._tabela.lower().title()})"
                    f"({self._regioes})?"
                    f"[.](csv|CSV|rar|RAR|zip|ZIP)"
                ),
                sep="|",
                encoding="latin-1",
            )

            # carrega uma versão dummy dos dados e compara contra os valores reais
            dummy = self._ds.carrega_como_objeto(documento=censo, nrows=10, **conf)
            if isinstance(dummy, dict):
                dummy = pd.concat(list(dummy.values()))
            total_cols = set(dummy.columns)
            if len(total_cols - set(self._dtype)) > 0:
                self._logger.warning(
                    f"As colunas {total_cols - set(self._dtype)} foram adicionadas ao dataset, avalie se não é necessário adiciona-las ao arquivo de configuração"
                )

            conf["usecols"] = self._carrega_cols
            conf["dtype"] = self._dtype
            censo.obtem_dados(**conf)

            if censo._data is not None:
                if isinstance(censo.data, dict):
                    censo.data = pd.concat(list(censo.data.values()))
                censo.data.rename(columns=self._rename, inplace=True)
                self._dados_entrada.append(censo)

        if len(self._dados_entrada) == 0:
            raise ValueError(
                f"As configurações do objeto não geraram qualquer base de dados"
                f"de entrada -> {self._base} / {self._tabela} / {self._ano}"
            )

    @staticmethod
    def obtem_operacao(
        op: str,
    ) -> typing.Callable[[pd.DataFrame, typing.List[str]], pd.Series]:
        """
        Recebe uma string com o tipo de operação de comparação
        a ser realiza e retorna uma função que receberá um dataframe
        e uma lista de colunas e devolverá uma série de booleanos
        comparando a soma desta lista de colunas por linha ao valor 0

        :param op: operação de comparação (=, >, <, >=, <=, !=)
        :return: função que compara soma de colunas ao valor 0
        """
        if op == "=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) == 0
        elif op == ">":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) > 0
        elif op == "<":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) < 0
        elif op == ">=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) >= 0
        elif op == "<=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) <= 0
        elif op == "!=":
            return lambda f, c: f.reindex(columns=c).sum(axis=1) != 0
        else:
            raise ValueError(
                f"O operador {op} não faz parte da lista de operações disponíveis"
            )

    @staticmethod
    def gera_coluna_por_comparacao(
        base: pd.DataFrame,
        colunas_a_tratar: typing.Dict[str, typing.List[str]],
    ) -> None:
        """
        Realiza a criação de novas colunas na base passada a partir
        de um outro conjunto de colunas que são somadas linha a linha
        e comparadas com o valor 0 por meio de algum operador

        :param base: base de dados a ser processada
        :param colunas_a_tratar: dicionário com configurações de tratamento
        """
        # percorre o dicionário de configurações
        for coluna, tratamento in colunas_a_tratar.items():
            # extraí o padrão de colunas de origem e a operação de comparação
            padrao, operacao = tratamento

            # obtém a lista de colunas de origem que devem ser utilizadas
            colunas_origem = [
                c for c in base.columns if re.search(padrao, c) is not None
            ]

            # se a coluna não existir na base e se nós temos colunas de origem
            if (coluna not in base.columns) and len(colunas_origem) > 0:
                # aplica a função de geração de colunas
                func = BaseCensoEscolarETL.obtem_operacao(operacao)
                base[coluna] = func(base, colunas_origem).astype("int")

    def gera_dt_nascimento(self, base: Documento) -> None:
        """
        Cria a coluna de data de nascimento do gestor

        :param base: documento com os dados a serem tratados
        """
        if (
            "NU_ANO" in base.data
            and "NU_MES" in base.data
            and "DT_NASCIMENTO" in self._configs["DADOS_SCHEMA"]
        ):
            if "NU_DIA" in base.data:
                base.data["DT_NASCIMENTO"] = pd.to_datetime(
                    base.data["NU_ANO"].astype(str)
                    + base.data["NU_MES"].astype(str)
                    + base.data["NU_DIA"].astype(str),
                    format="%Y%m%d",
                )
            else:
                base.data["DT_NASCIMENTO"] = pd.to_datetime(
                    base.data["NU_ANO"].astype(str) + base.data["NU_MES"].astype(str),
                    format="%Y%m",
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
        cols = set(self._cols_in + in_atual)
        dif = set(in_atual) - set(self._cols_in)
        if len(dif) > 0:
            self._logger.warning(
                f"Há novas colunas IN que foram adicionadas -> {dif}"
                f"\nConsidere adiciona-las ao info/aquis_censo_{self._tabela}.yml"
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

    def remove_duplicatas(self, base: Documento) -> typing.Union[None, Documento]:
        """
        Remove duplicatas na base devido a uma chave secundária
        que gera um de-para entre o nível de granularidade da base
        e alguma outra entidade do censo

        :param base: base com duplicatas a serem removidas
        :return: documento com base de de-para
        """
        if len(self._configs["COLS_DEPARA"]) == 0:
            return None
        cols = [self._configs["COL_ID"], "ANO"] + self._configs["COLS_DEPARA"]
        base_id = Documento(
            ds=self._ds,
            referencia=dict(
                nome=f"{self._tabela}_depara.parquet",
                colecao=base.colecao.nome,
                pasta=base.colecao.pasta,
            ),
            data=base.data.reindex(columns=cols),
        )
        self._logger.debug(base, base.data.shape)
        base.data.drop(
            columns=self._configs["COLS_DEPARA"], errors="ignore", inplace=True
        )
        base.data.drop_duplicates(inplace=True)
        self._logger.debug(base, base.data.shape)

        return base_id

    def gera_documento_saida(
        self, base: Documento, base_id: typing.Union[None, Documento]
    ) -> None:
        """
        Concatena as bases de dados em uma saída única

        :param base: documento com os dados de entrada
        :param base_id: documento com os dados que duplicam linhas
        """
        self._dados_saida = list()
        self.documentos_saida[0].data = base.data
        if base_id is not None:
            self.documentos_saida[1].data = base_id.data
        self._dados_saida += self.documentos_saida

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
        # garante que todas as colunas existam
        rm = set(base.data) - set(schema)
        ad = set(schema) - set(base.data)
        if len(rm) > 0:
            self._logger.warning(f"As colunas {rm} serão removidas do data set")
        if len(ad) > 0:
            self._logger.warning(f"As colunas {ad} serão adicionadas do data set")
        base.data = base.data.reindex(columns=schema)

        # preenche nulos com valores fixos
        for c, p in fill.items():
            if c in base.data:
                base.data[c] = base.data[c].fillna(p)

        # ajusta o schema
        for c, dtype in schema.items():
            if dtype.startswith("pd."):
                base.data[c] = base.data[c].astype(eval(dtype))
            elif dtype == "str":
                base.data[c] = base.data[c].astype(dtype).replace({"nan": None})
            else:
                base.data[c] = base.data[c].astype(dtype)

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        self._logger.info("Gera DT nascimento")
        self.gera_dt_nascimento(self.dados_entrada[0])

        self._logger.info("Processando colunas DT_")
        self.processa_dt(self.dados_entrada[0])

        self._logger.info("Processando colunas QT_")
        self.processa_qt(self.dados_entrada[0])

        self._logger.info("Processando colunas IN_")
        self.processa_in(self.dados_entrada[0])

        self._logger.info("Processando colunas TP_")
        self.processa_tp(self.dados_entrada[0])

        self._logger.info("Removendo informações duplicadas")
        base_id = self.remove_duplicatas(self.dados_entrada[0])

        self._logger.info("Gera documentos de saída")
        self.gera_documento_saida(self.dados_entrada[0], base_id)

        self._logger.info("Realizando ajustes finais na base")
        self.ajusta_schema(
            base=self.documentos_saida[0],
            fill=self._configs["PREENCHER_NULOS"],
            schema=self._configs["DADOS_SCHEMA"],
        )
        if len(self.documentos_saida) > 1:
            self.ajusta_schema(
                base=self.documentos_saida[1],
                fill=self._configs["PREENCHER_NULOS"],
                schema=self._configs["DEPARA_SCHEMA"],
            )
