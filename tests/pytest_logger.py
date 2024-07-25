import logging
import sys

initiated = False


def Logger():
    return __init_logger()


def __init_logger():
    global initiated

    logger = logging.getLogger()
    logging.getLogger('faker').setLevel(logging.ERROR)
    if not initiated:
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler("test_log")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        initiated = True
    return logger
