import typing

from src.aquisicao.inep._censo_escolar import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento


class _MatriculaRegiaoETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de matrícula do
    censo escolar para uma região em particular

    Este objeto é construído pelo MatriculaETL e utilizado para
    fazer uma gestão eficiente de memória
    """

    _configs: typing.Dict[str, typing.Any]
    _documentos_saida: typing.List[Documento]
    reg: str

    def __init__(
        self,
        ds: DataStore,
        regiao: str,
        ano: typing.Union[int, str] = "ultimo",
        criar_caminho: bool = True,
        reprocessar: bool = False,
    ) -> None:
        """
        Instância o objeto de ETL de dados de Matrícula

        :param ds: instância de objeto data store
        :param regiao: região a ser processada
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            ds,
            "matricula",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
            regioes=[regiao],
        )
        self.reg = regiao.upper()

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
                    referencia=dict(CatalogoAquisicao.ALUNO),
                ),
                Documento(
                    ds=self._ds,
                    referencia=dict(CatalogoAquisicao.MATRICULA),
                ),
            ]
        return self._documentos_saida

    def processa_tp(self, base: Documento) -> None:
        """
        Realiza o processamento das colunas de tipo

        :param base: documento com os dados a serem tratados
        """
        if "TP_ZONA_RESIDENCIAL" in base.data:
            if base.data["TP_ZONA_RESIDENCIAL"].min() == 0:
                base.data["TP_ZONA_RESIDENCIAL"] += 1

        super(_MatriculaRegiaoETL, self).processa_tp(base)

    def load(self) -> None:
        """
        Exporta os dados transformados
        """
        for doc in self.dados_saida:
            doc.pasta = f"{doc.nome}/ANO={self.ano}/REGIAO={self.reg}"
            doc.nome = f"{self.reg}_{self.ano}.parquet"
            doc.data.drop(columns=["ANO"], inplace=True)
            self._ds.salva_documento(doc)


class MatriculaETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de matrícula do
    censo escolar
    """

    _configs: typing.Dict[str, typing.Any]
    _documentos_saida: typing.List[Documento]
    _etls: typing.List[_MatriculaRegiaoETL]

    def __init__(
        self,
        ds: DataStore,
        criar_caminho: bool = True,
        reprocessar: bool = False,
        ano: typing.Union[int, str] = "ultimo",
    ) -> None:
        """
        Instância o objeto de ETL de dados de Matrícula

        :param ds: instância de objeto data store
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        """
        super().__init__(
            ds,
            "matricula",
            ano=ano,
            criar_caminho=criar_caminho,
            reprocessar=reprocessar,
        )
        self._etls = [
            _MatriculaRegiaoETL(
                ds=self._ds,
                regiao=reg,
                ano=self.ano,
                criar_caminho=self._criar_caminho,
                reprocessar=self._reprocessar,
            )
            for reg in ["CO", "NORDESTE", "NORTE", "SUDESTE", "SUL"]
        ]

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
                    referencia=dict(CatalogoAquisicao.ALUNO),
                ),
                Documento(
                    ds=self._ds,
                    referencia=dict(CatalogoAquisicao.MATRICULA),
                ),
            ]
        return self._documentos_saida

    def extract(self) -> None:
        """
        Extraí os dados do objeto
        """
        self._logger.info(
            "Os dados serão carregados de forma individual para controlar o uso de memória"
        )
        self._dados_entrada = list()

    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse

        Para este objeto nós fazemos o processamento dos dados de
        cada região por meio de ETLs para cada uma
        """
        for etl in self._etls:
            self._logger.info(f"----- PROCESSANDO DADOS PARA REGIÃO {etl.reg} -----")
            etl.extract()
            etl.transform()

    def load(self) -> None:
        """
        Exporta os dados transformados utilizando o _MatriculaRegiaoETL
        """
        for etl in self._etls:
            etl.load()
