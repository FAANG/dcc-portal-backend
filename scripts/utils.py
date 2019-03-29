"""
Different function that could be used in any faang backend script
"""
import logging


def create_logging_instance(name):
    """
    This function will create logger instance that will log information to {name}.log file
    Log example: 29-Mar-19 11:54:33 - DEBUG - This is a debug message
    :param name: name of the logger and file
    :return: logger instance
    """
    # Create a custom logger
    logger = logging.getLogger(name)

    # Create handlers
    f_handler = logging.FileHandler(f'{name}.log')
    f_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(f_handler)
    return logger
