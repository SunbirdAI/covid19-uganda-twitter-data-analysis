import logging
import datetime
import os
from definitions import LOGGING_DIR


def create_logger(logger_name, filename, logging_format, logging_level):
    """
    Creates a logger
    :param logger_name: the name of the python file using making the logs
     (can use __name__ variable in each calling file)
    :param filename: the name of the file to which to write the logs
    :param logging_format: format of the logs
    :param logging_level: DEBUG, INFO, WARNING e.t.c
    :return: the logger object
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging_level)

    formatter = logging.Formatter(logging_format, datefmt="%H:%M:%S")

    directory = "{}/{}".format(LOGGING_DIR, get_current_date_string())
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_handler = logging.FileHandler("{}/{}".format(directory, filename))
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger


def get_current_date_string():
    """
    :return: the current date as a string to be appended to the directories
    """
    return datetime.datetime.now().strftime('%Y-%m-%d')
