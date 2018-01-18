##############################################################################################
# Project     : sqlite3 wrapper
# File        : sql_utils.py
# Author      : Remi Malaquin
# Date        : 01/17/2018
# Description : Utils functions for SQL access
##############################################################################################

import logging
import unicodedata
import inspect

from sql_exception import *


class Logger:
    def __init__(self, name, severity=logging.DEBUG):
        # create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(severity)

        # create console handler and set level to debug
        self.logger.handlers.clear()
        self.ch = self.define_handler()
        self.set_severity_level(self.ch)

        # create formatter
        self.functionName = ""
        self.formatter = logging.Formatter('%(asctime)s [%(name)s.' + self.functionName
                                           + '] %(levelname)s - %(message)s')

        # add formatter to ch
        self.ch.setFormatter(self.formatter)

        # add ch to logger
        self.logger.addHandler(self.ch)

    def define_handler(self):
        return logging.StreamHandler()

    def set_severity_level(self, handler, severityLevel=logging.DEBUG):
        handler.setLevel(severityLevel)

    def change_formatter(self, functionName=""):
        if self.functionName != functionName:
            self.formatter = logging.Formatter('%(asctime)s [%(name)s.' + functionName
                                               + '] %(levelname)s - %(message)s')
            self.ch.setFormatter(self.formatter)

    def debug(self, functionName, message: str):
        self.change_formatter(functionName)
        self.logger.debug(message)

    def info(self, functionName, message: str):
        self.change_formatter(functionName)
        self.logger.info(message)

    def warning(self, functionName, message: str):
        self.change_formatter(functionName)
        self.logger.warning(message)

    def error(self, functionName, message: str):
        self.change_formatter(functionName)
        self.logger.error(message)

    def critical(self, functionName, message: str):
        self.change_formatter(functionName)
        self.logger.critical(message)


def sql_type(typename, value):
    """
    To be completed (Only INTEGER and TEXT are defined)

    :param typename:
    :param value:
    :return: type
    """
    log = Logger('SQL_type')
    if 'INTEGER'.lower() in typename.lower():
        return int(value)
    elif 'FLOAT'.lower() in typename.lower():
        return float(value)
    elif 'TEXT'.lower() in typename.lower():
        return str(value)
    elif 'TIMESTAMP'.lower() in typename.lower():
        return str(value)
    elif 'TIME'.lower() in typename.lower():
        return int(value)
    else:
        log.critical(functionName=str(inspect.stack()[-5][3]), message="Type Unknown")
        raise TypeError(str(typename) + " is an Unknown Type: Check sql_type function in SQL/sql_utils.py")


def check_param_char(ref_param, test_param, test='111'):
    log = Logger(name='Check_param_char')

    #####################################################################################################
    # check the length of parameter
    if test[0] == '1':
        if len(ref_param) != len(test_param):
            log.error(functionName='check_param_length',
                      message="Length of parameter defined at the creation of the table "
                              "differ from the one used to insert data in the table")
            # raise Exception
            raise SqlLengthParameterError("ref_param = " + str(len(ref_param)) + " | test_param = " + str(len(test_param)))
        else:
            pass

    #####################################################################################################
    # Check parameter name
    if test[1] == '1':
        for key in test_param:
            if key not in ref_param.keys():
                log.error(functionName='check_param_name',
                          message="The following parameter does not exist in the reference: " + str(key))
                # raise Exception
                raise SqlNameParameterError("This parameter does not exist in the SQL table " + str(key))
            else:
                pass

    #####################################################################################################
    # Check the type of each parameter
    if test[2] == '1':
        for key, val in test_param.items():
            try:
                test_param[key] = sql_type(ref_param[key], val)
            except:
                # raise Exception
                raise SqlTypeParameterError("parameterName = " + str(key) +
                                            " | exp = " + str(ref_param[key]) +
                                            " | get : " + str(type(val).__name__))


def check_for_double_items(param, table, query_info, auth):
    double_list = []
    indice = []
    # Get every column number of the filter key to check on the table
    for paramInfo in query_info:
        if paramInfo[1] in param.keys():
            indice.append(paramInfo[0])

    # Check and return True if double occurs
    for t in table:
        for idx, idx2 in zip(indice, range(len(list(param.values())))):
            if t[idx].lower() == list(param.values())[idx2].lower():
                double_list.append(True)
            else:
                double_list.append(False)
        # Double didn't occurs so we can clean the list because we are looking for double
        if auth:
            if len(set(double_list)) > 1:
                double_list = []
        else:
            if True in double_list:
                double_list = [True]

    if not double_list:  # Empty list
        double = False
    else:
        double = list(set(double_list))[0]

    if table and double:
        raise SqlDoubleItemsOccurs("Item already exist in the database")


def translate_no_accent_nocase_sensitive(string_exemple):
    return str(unicodedata.normalize('NFKD', str(string_exemple)).encode('ASCII', 'ignore'), 'utf-8').lower()
