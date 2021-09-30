

class Data:
    """
    Representa a interface com os dados que podem ser acessados
    """

    colecao: str
    nome: str
    tipo: str
    pasta: str
    _data: bytes

    def constroi_por_bytes(self):
        pass

    def constroi_por_df(self):
        pass

    def obtem_dados(self):
        pass

    @property
    def data(self):
        pass
