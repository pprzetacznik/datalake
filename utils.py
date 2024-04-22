import sys
import logging
from config import Config


def setup_logger():
    if Config.VERBOSE:
        logger = logging.getLogger("kafka")
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)


def read_file(filename):
    with open(filename) as f:
        return f.read()
