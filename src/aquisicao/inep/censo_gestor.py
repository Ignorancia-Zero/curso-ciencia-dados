import typing

from src.aquisicao.inep._censo_escolar import BaseCensoEscolarETL
from src.io.data_store import CatalogoAquisicao
from src.io.data_store import DataStore
from src.io.data_store import Documento


class GestorETL(BaseCensoEscolarETL):
    """
    Classe que realiza o processamento de dados de gestor do
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
        Instância o objeto de ETL de dados de Gestor

        :param ds: instância de objeto data store
        :param ano: ano da pesquisa a ser processado (pode ser um inteiro ou 'ultimo')
        :param criar_caminho: flag indicando se devemos criar os caminhos
        :param reprocessar: flag se devemos reprocessar o conteúdo do ETL
        """
        super().__init__(
            ds, "gestor", ano=ano, criar_caminho=criar_caminho, reprocessar=reprocessar
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
                    referencia=dict(CatalogoAquisicao.GESTOR),
                ),
                Documento(
                    ds=self._ds,
                    referencia=dict(CatalogoAquisicao.GESTOR_ESCOLA),
                ),
            ]
        return self._documentos_saida
