import abc
import re
import typing

import pandas as pd
from tqdm import tqdm

from src.aquisicao.inep.base_inep import BaseINEPETL
from src.io.data_store import DataStore


class BaseCensoEscolarETL(BaseINEPETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do CensoEscolar
    """

    _ano: typing.Union[str, int]
    _tabela: str

    def __init__(
        self,
        ds: DataStore,
        tabela: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
    ) -> None:
        """
        Instância o objeto de ETL Censo Escolar

        :param ds: instância de objeto data store
        :param tabela: Tabela do censo escolar a ser processada
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        """
        super().__init__(ds, "censo-escolar", ano=ano, criar_caminho=criar_caminho)
        self._tabela = tabela

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

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        # realiza o download dos dados do censo
        self.download_conteudo()

        # inicializa os dados de entrada como um dicionário vazio
        self._dados_entrada = list()

        # para cada arquivo do censo demográfico
        for censo in tqdm(self.inep):
            self._ds.carrega_como_objeto(
                censo,
                como_df=True,
                padrao_comp=(
                    f"{self._tabela.lower()}."
                    f"|{self._tabela.upper()}."
                    f"|{self._tabela.lower().title()}."
                ),
                sep="|",
                encoding="latin-1",
            )
            if censo._data is not None:
                self._dados_entrada.append(censo)

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse
        """
        pass
