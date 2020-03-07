import logging
import configparser
import os

config = configparser.ConfigParser()

# Get the config file path from environmental variable CFG_DIR
CFG_DIR = os.environ.get('CFG_DIR')
if CFG_DIR:
    CFG_FILE = CFG_DIR + "\\config.properties"
else:
    CFG_FILE = "E:\\Spark\\github\\Pyspark\\DataframeOperations\\conf\\config.properties"

# Read the CFG file for the log settings
config.read(CFG_FILE)
if 'LOGS' in config:
    LOG_CFG = config['LOGS']
    LOG_LEVEL = LOG_CFG['LOG_LEVEL'].upper()
    LOG_FILE = LOG_CFG['LOG_FILE']
    ERR_FILE = LOG_CFG['ERR_FILE']
else:
    LOG_LEVEL = "INFO".upper()
    LOG_FILE = "E:\\Spark\\logs\\dataframe_operations.log"
    ERR_FILE = "E:\\Spark\\logs\\dataframe_operations.err"

formatter = logging.Formatter('%(asctime)s: %(name)s: %(levelname)s: %(message)s')


def get_logger(logger_name):
    """Defines the logger and returns the same"""
    logger = logging.getLogger(logger_name)
    logger.setLevel(LOG_LEVEL)

    log_handler = logging.FileHandler(LOG_FILE)
    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    error_handler = logging.FileHandler(ERR_FILE)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    if logger.getEffectiveLevel() == logging.DEBUG:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
