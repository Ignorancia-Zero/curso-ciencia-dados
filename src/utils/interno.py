import inspect
import os
import types
import typing


def obtem_argumentos_objeto(
    objeto: typing.Union[typing.Callable, object], kwargs: dict
) -> dict:
    """
    Extrai de um dicionário de argumentos chave os argumentos que
    são relacionados a um determinado objeto

    :param objeto: objeto ou função a ter parâmetros inspecionado
    :param kwargs: argumentos chave a serem associados
    :return: dicionário com argumentos chave existentes no objeto
    """
    if isinstance(objeto, types.FunctionType):
        sig = inspect.signature(objeto)
    else:
        sig = inspect.signature(objeto.__init__)
    args_obj = dict()
    for param in sig.parameters:
        if param in kwargs:
            args_obj[param] = kwargs[param]
    return args_obj


def obtem_extencao(arquivo: str) -> str:
    """
    Obtem a extenção de um arquivo

    :param arquivo: nome do arquivo
    :return: string com extenção sem .
    """
    return os.path.splitext(arquivo)[-1][1:].lower()
