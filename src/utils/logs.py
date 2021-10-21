import logging
import sys
import traceback
import typing
from datetime import datetime
from pathlib import Path


def configura_logs(
    formato: str = "{asctime} {levelname} ({module}:{lineno:d}) {message}",
    arquivo: bool = True,
) -> str:
    """
    Inicia os objetos Logger e realiza as configurações de formatação,
    nível e saída do log

    :param formato: formatação dos logs
    :param arquivo: flag se devemos criar um stream para um arquivo
    :return: data e horário da execução do programa
    """
    # obtém o logger raíz
    logger_raiz = logging.getLogger()
    logger_raiz.setLevel(logging.INFO)

    # gera a chave de execução
    chave_de_execucao = datetime.now().strftime("%Y%m%d-%H%M%S")

    # configura um handler para o console
    if len(logger_raiz.handlers) > 0:
        chandler = logger_raiz.handlers[0]
    else:
        chandler = logging.StreamHandler()
        logger_raiz.addHandler(chandler)
    chandler.setLevel(logging.INFO)

    # configura um formato de saída
    formatter = logging.Formatter(formato, style="{", datefmt="%Y-%m-%d %H:%M:%S")
    chandler.setFormatter(formatter)

    # configura o objeto para saída para um arquivo
    if arquivo:
        # cria uma pasta com a saída dos logs
        log_dir = Path(__file__).parent.parent.parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # configura o objeto de saída para o arquivo
        fhandler = logging.FileHandler(
            filename=log_dir / f"{chave_de_execucao}.log", mode="a"
        )
        fhandler.setLevel(logging.INFO)

        # ajusta a formataçõa
        fhandler.setFormatter(formatter)
        logger_raiz.addHandler(fhandler)

    # exporta o primeiro log
    logger = logging.getLogger(__name__)
    logger.info(f"Inicializando execução {chave_de_execucao}")

    return chave_de_execucao


def log_erros(func: typing.Callable) -> typing.Callable:
    """
    Cria um decorador para ser colocado nas funções principais
    do código de forma que permita transferir o traceback caso
    ocorra algum erro ao log

    :param func: função a ser decorada
    :return: função que irá exportar o traceback para o log
    """

    def func_log(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        logger = logging.getLogger(__name__)
        try:
            return func(*args, **kwargs)
        except BaseException:
            logger.error(traceback.format_exc())
            sys.exit(-1)

    return func_log
