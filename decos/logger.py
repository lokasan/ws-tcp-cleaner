import logging
import sys
import decos.logs.config_client_log
import decos.logs.config_server_log
logging.basicConfig(level=logging.INFO)

if sys.argv[0].find('client') == -1:
    logger = logging.getLogger('server')
else:
    logger = logging.getLogger('client')


def loggers(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Была вызвана функция {func.__name__} с параметрами {args}, {kwargs}, вызов из модуля {func.__module__}")
        ret = func(*args, **kwargs)
        return ret
    return wrapper
