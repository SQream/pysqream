from pysqream.globals import dbg
import logging


logger = logging.getLogger("dbapi_logger")
logger.setLevel(logging.DEBUG)
logger.disabled = True


def printdbg(*debug_print):
    if dbg:
        print(*debug_print)


def start_logging(log_path=None):
    log_path = log_path or '/tmp/sqream_dbapi.log'
    # logging.disable(logging.NOTSET)
    logger.disabled = False
    try:
        handler = logging.FileHandler(log_path)
    except Exception as e:
        raise Exception("Bad log path was given, please verify path is valid and no forbidden characters were used")

    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    return logger


def stop_logging():
    # logging.disable(logging.CRITICAL)
    logger.handlers = []
    logger.disabled = True


def log_and_raise(exception_type, error_msg):
    if logger.isEnabledFor(logging.ERROR):
        logger.error(error_msg, exc_info=True)

    raise exception_type(error_msg)
