import logging

try:
    import SysDictionary

    logging.basicConfig(level=SysDictionary.LOGGING_CONF.get("LOG_LEVEL", logging.INFO))
except (ModuleNotFoundError, KeyError) as e:
    logging.basicConfig(level=logging.WARNING)
    logging.warning(f"Failure to set logging configuration: {str(e)}")


def get_logger(name):
    logger = logging.getLogger(name)
    return logger
