import logging


class InvalidDefaultConfiguration(Exception):
    def __init__(self):
        logging.error('Incorrect type of default value given in the default.ini file. '
                      'For correct types of values for every option check --help')


class PathIsFile(Exception):
    def __init__(self, path):
        logging.error(path + ' is a file, not a directory')


class ValueNegative(Exception):
    def __init__(self, value):
        logging.error(f"{value} can't be negative")


class NonexistentSchemaFile(Exception):
    def __init__(self, path):
        logging.error(f"There is no file {path}")


class IncorrectSchema(Exception):
    def __init__(self, message):
        logging.error("Given data schema is incorrect: " + message)


class WrongTypeRandRange(Exception):
    def __init__(self):
        logging.error("'rand()' can be used only with int")


class WrongListOfChoices(Exception):
    def __init__(self, key):
        logging.error(f"List of values for '{key}' is incorrect")


class IncorrectType(Exception):
    def __init__(self, type):
        logging.error(f"Incorrect type given: {type}")


class IncorrectValue(Exception):
    def __init__(self, value, type):
        logging.error(f"Value {value} is incorrect for a given type - expected {type}")


class ForbiddenCharInFileName(Exception):
    def __init__(self):
        logging.error("'/' character is forbidden in file names in linux systems")
