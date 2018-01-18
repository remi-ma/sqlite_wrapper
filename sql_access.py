##############################################################################################
# Project     : sqlite3 wrapper
# File        : sql_access.py
# Author      : Remi Malaquin
# Date        : 01/17/2018
# Description : Routines to access and modify SQL database and table.
##############################################################################################

import sqlite3

from sql_utils import *


class SQLDatabase:
    """
    Manage Database
    """

    def __init__(self, databaseName="/default/directory/DBName"):
        """
        Connect to the database & create cursor.

        :param databaseName: DB file to create/read
        """
        self.base = sqlite3.connect(databaseName)
        self.base.create_function("noaccent", 1, translate_no_accent_nocase_sensitive)
        self.cursor = self.base.cursor()
        self.SQLdblog = Logger(name='SQLDatabase', severity=logging.INFO)

    def commit(self):
        """
        Commit changes into the database

        :return: None
        """
        self.SQLdblog.debug(functionName="commit",
                            message="Commit into Database")
        self.base.commit()

    def close(self):
        """
        Close Database

        :return: None
        """
        self.SQLdblog.debug(functionName="close",
                            message="Close Database")
        self.base.close()

    def list_table(self):
        """
        List all the table inside the database

        :return: list of table name
        """
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [idx[0] for idx in self.cursor.fetchall()]

    def drop(self, table: str):
        try:
            if table not in self.list_table():
                raise SqlTableUnknown("Table does not exist in the database")
            self.cursor.execute("DROP TABLE " + table)
            self.SQLdblog.debug(functionName="drop",
                                message="Table '" + table + "' has been removed from database.")
        except SqlTableUnknown:
            self.SQLdblog.error(functionName="drop",
                                message="Table '" + table + "' doesn't exist in the database...")
            raise

        except sqlite3.OperationalError:
            self.SQLdblog.error(functionName="drop",
                                message="Can't remove Table '" + table + "'")


class SQLTable(object):
    """
    Manage Table in a database.

       - Create Table
       - Insert/modify/delete data in table
    """

    def __init__(self, SQLdbObj, tableName, **kwargs):
        """
        Define table related to a database, with its name and its parameters
            - Create new table if does not exists
            OR
            - extract info from existing table (param_name & param_type)

        :param SQLdbObj: database name object
        :param tableName: table name
        :param kwargs: <param_name1>='<param_type1>', <param_name2>='<param_type2>', ...
        """
        self.db           = SQLdbObj                                     # related database object
        self.tableName    = tableName                                    # Name of the table
        self.dataType     = []                                           #
        self.filterKey    = []

        self.SQLtablelog = Logger(name='SQLTable', severity=logging.INFO)

        # Dynamically create the defined table if not ALREADY defined.
        try:
            if self.tableName == '':
                raise SqlTableNameEmptyError("Table cannot be created because the name is empty")
            self.__create_table(kwargs)                                  # Use Implicit function to create table

            self.tableVar = kwargs                                       # Extract parameters of the table
            self.tablePrimVar = {k: v for k, v in kwargs.items()
                                 if 'primary key'.lower() in v.lower()}  # Extract Primary parameter of the table
            self.tableLen = len(self.tableVar)                           # Number of parameter in the table

            self.SQLtablelog.info(functionName="__init__",  # str(inspect.stack()[-5][3]),
                                  message="Table '" + self.tableName + "' created")

        # Exception if name is empty !!!
        except SqlTableNameEmptyError as e:
            self.SQLtablelog.error(functionName="__init__", message=e.args[0])
            raise

        # Extract info from existing table in the database (parameters, Primary parameter, length, ...)
        except sqlite3.OperationalError:
            query_result = self.query_info()                             # Query the database about the table

            self.tableVar = {idx[1]: idx[2] for idx in query_result}     # Update parameters of the table
            self.tablePrimVar = {idx[1]: idx[2] for idx in query_result
                                 if idx[-1] == 1}                        # Update Primary parameter of the table
            self.tableLen = len(self.tableVar)                           # Update Number of parameter in the table

            self.SQLtablelog.debug(functionName="__init__",
                                   message="Table '" + self.tableName + "' already exists : Info extracted.")

    def __create_table(self, tableVar):
        """
        Implicit function to create table => called in the __init__ function

        :param tableVar: dictionary of {param_name : param_type}
        :return: None
        """
        # Merge every parameters of the Table in a list: ["name1 type1", "name2 type2", ...]
        for key, val in tableVar.items():
            self.dataType.append(key + " " + val)
        # Create Table
        self.db.cursor.execute("CREATE TABLE " + self.tableName + '(' + ", ".join(self.dataType) + ")")

    def query_info(self):
        """
        Query the database to analyse if table already exists.

        :return: Table info (parameters, ...)
        """
        self.db.cursor.execute("PRAGMA table_info(" + self.tableName + ")")
        return self.db.cursor.fetchall()

    def define_filter_for_insertion(self, filterKeys: list):
        """
        Define a list of parameter that will be used during insertion to check if data already exists in table
        !!!! Must be called before insert, otherwise, no filter will be applied !!!!

        :param filterKeys: List of parameter to filter
        :return: None
        """
        # check if all filter are in the parameter of the table
        try:
            for key in filterKeys:
                if key not in self.tableVar.keys():
                    raise InsertionKeyNotFoundError(str(filterKeys) + "' parameters not found in " + str(list(self.tableVar.keys())))
            self.SQLtablelog.debug(functionName="define_filter_for_insertion",
                                   message="'" + str(filterKeys) + "' parameters will be used for filtering")
        except InsertionKeyNotFoundError:
            self.SQLtablelog.error(functionName="define_filter_for_insertion",
                                   message="'" + str(filterKeys) + "' parameters not found in "
                                           + str(list(self.tableVar.keys())))
        finally:
            self.filterKey = filterKeys

    def insert(self, auth=False, **kwargs):
        """
        Insert element into the associated table

        :param auth: hidden parameter :)
        :param kwargs: Dictionary of parameter => defined by SQLTable object
        :return: None
        """

        try:
            # Check & Compare the characteristics of the parameters
            ''' # -> Table parameters (number, type) have already been defined during table creation
                #    This function checks that SQLTable.insert 'kwargs param' are inline with the table param
                #       - ref_param = parameters @ creation of the table
                #       - test_param = parameters @ call of insert function
            '''
            check_param_char(ref_param=self.tableVar, test_param=kwargs)

            # Select the filter parameters (thanks to SQLTable.define_filter_for_insertion function)
            param = {k: v for k, v in kwargs.items() if k in self.filterKey}

            # Filter key MUST NOT be empty for insertion ...
            for k, v in param.items():
                if v == '':
                    raise SqlFilterKeyEmptyError("Parameter '" + str(k) + "' is empty")

            # Check filtered parameter does NOT exist in table
            table = self.select_one(**param)

            check_for_double_items(param=param, table=table, query_info=self.query_info(), auth=auth)

            self.SQLtablelog.info(functionName="insert",
                                  message="INSERT in " + str(self.tableName) + ": " +
                                          " | ".join([str(k) + "=" + str(kwargs[k]) for k in list(kwargs.keys())]))

            # 'id' parameter should be automatically incremented, So don't need to insert it if present
            if 'id' in kwargs.keys():
                kwargs.pop('id')
                table_len_final = self.tableLen - 1
            else:
                table_len_final = self.tableLen

            self.db.cursor.execute("INSERT INTO " + self.tableName + '(' + ",".join(kwargs.keys()) + ") " +
                                   "VALUES(" + ','.join(["?"] * table_len_final) + ")", tuple(kwargs.values()))

        except (SqlFilterKeyEmptyError, SqlLengthParameterError, SqlNameParameterError, SqlTypeParameterError, SqlDoubleItemsOccurs) as e:
            self.SQLtablelog.error(functionName="insert", message=e.args[0])
            raise

        except sqlite3.OperationalError:
            self.SQLtablelog.error(functionName="insert", message="Please, check the definition of the table you try to access, " +
                                                                  "parameter definition does not reach expectations")

    def modify(self, **kwargs):
        """
        Parameter already exists in table but you want to modify it anyway

        :param kwargs: Primary key is mandatory to modify the table
        :return: None
        """

        #####################################################################################################
        # Check & Compare the characteristics of the parameters
        ''''# -> Table parameters (type, ...) have already been defined during table creation
            #    This function checks that SQLTable.insert 'kwargs param' are inline with the table param
            #       - ref_param = parameters @ creation of the table
            #       - test_param = parameters @ call of insert function
        '''
        try:
            check_param_char(ref_param=self.tableVar, test_param=kwargs, test='011')

            # Extract Primary Key from Table info
            search_param = next(iter(self.tablePrimVar.keys()))  # Primary parameter to search in the table

            #####################################################################################################
            # Check if Primary Parameter has been defined in entry.
            if search_param in kwargs.keys():
                search_var = {k: v for k, v in kwargs.items()
                              if search_param.lower() == k.lower()}  # Variable to search for primary key
                kwargs.__delitem__(search_param)  # Delete primary key from parameters
                list_other_param = [key + "=?" for key in kwargs]
                list_search_param = search_param + "=?"
                tuple_param = tuple([kwargs[key] for key in kwargs]) + tuple([search_var[search_param]])

                #################################################################################################
                # Check first if it already exist in the table and if there are no double.
                presence = self.select_one(**search_var)
                presence_name = ", ".join([presence[i][0] for i in range(len(presence))])
                if (len(presence) == 1) or (search_var[search_param] in [presence[i][0] for i in range(len(presence))]):
                    self.db.cursor.execute("UPDATE " + str(self.tableName) +
                                           " SET " + ','.join(list_other_param) +
                                           " WHERE " + list_search_param, tuple_param)
                    self.SQLtablelog.info(functionName="modify",
                                          message="Modify '" + str(search_var[search_param]) +
                                                  "' item from table '" + str(self.tableName) +
                                                  "' with " + str(kwargs))

                #################################################################################################
                # Double has been detected during modify process
                elif len(presence) > 1:
                    raise SqlSeveralElementItemsSelected("Several elements contains the same name: [" +
                                                            str(presence_name) + "] Choose the right one")

                #################################################################################################
                # No element detected with this name => use insert function instead
                else:
                    raise SqlNoElementFound("No element found in that table => Use SQLTable.insert function instead")

            #####################################################################################################
            # Primary key is missing @ call
            else:
                raise SqlMissingPrimaryKey("Missing primary key : " + str(search_param))

        except (SqlLengthParameterError, SqlNameParameterError, SqlTypeParameterError, SqlDoubleItemsOccurs,
                SqlMissingPrimaryKey, SqlNoElementFound, SqlSeveralElementItemsSelected) as e:
            self.SQLtablelog.error(functionName="modify", message=e.args[0])
            raise

    def delete(self, inclusion=" AND ", **kwargs):
        """
        Delete a data from table

        :param inclusion: define combinational logic between filter
        :param kwargs: pattern research
        :return: None
        """

        filterkey = []
        filterval = []
        for key, value in kwargs.items():
            for i in str(value).split(' '):
                filterkey.append("instr(noaccent(" + key + "), ?)>0")
                filterval.append(translate_no_accent_nocase_sensitive(i))
        filtertotal = inclusion.join(filterkey)

        try:
            self.db.cursor.execute("DELETE FROM " + str(self.tableName) +
                                   " WHERE " + filtertotal,
                                   tuple(filterval))
            self.SQLtablelog.info(functionName="delete",
                                  message="All data filtered with" + str(filterval) +
                                          " have been deleted from table '" + str(self.tableName) + "'")
        except sqlite3.OperationalError:
            self.SQLtablelog.error(functionName="delete",
                                   message="Are you sure your parameter are correct")

    def select_all(self):
        """
        Query the database to extract all information from the selected table

        :return: Tuple of information
        """
        self.db.cursor.execute("SELECT " + ", ".join(self.tableVar.keys()) +
                               " FROM " + str(self.tableName))
        return self.db.cursor.fetchall()

    def select_one(self, inclusion=" AND ", **kwargs):
        """
        Query the database to extract the information of a predefined name of the Table

        :param inclusion: Choose the logic for filtering between multiple parameters
        :param kwargs: Parameter to look for
        :return: result of research
        """
        filterkey = []
        filterval = []
        for key, value in kwargs.items():
            for i in str(value).split(' '):
                filterkey.append("instr(noaccent(" + key + "), ?)>0")
                filterval.append(translate_no_accent_nocase_sensitive(i))
        filtertotal = inclusion.join(filterkey)

        try:
            self.db.cursor.execute("SELECT " + ", ".join(self.tableVar.keys()) +
                                   " FROM " + str(self.tableName)  +
                                   " WHERE " + filtertotal,
                                   tuple(filterval))
        except sqlite3.OperationalError:
            self.SQLtablelog.error(functionName="select_one",
                                   message="Did you define a filter before insertion?")

        return self.db.cursor.fetchall()

if __name__ == "__main__":
    # define SQL Database
    database = SQLDatabase(databaseName="./DB")

    # Define Table "Cars"
    param_default = {'name': 'TEXT PRIMARY KEY', 'brand': 'TEXT', 'color': 'TEXT', 'price': 'FLOAT', 'horsepower': 'INTEGER'}
    table = SQLTable(SQLdbObj=database, tableName="Cars", **param_default)

    # define filter before insertion: These paramters must be UNIQUE in the table...
    table.define_filter_for_insertion(['name'])

    # Insert into table
    try:
        table.insert(name='i8', brand='BMW', color='Blue', price=143400, horsepower=357)
        table.insert(name='Model S', brand='Tesla', color='Blue', price=68000, horsepower=382)
    except SqlDoubleItemsOccurs as e:
        print(e.args[0])

    # Query the info of the table...
    print("-----------------------")
    print("Query the table")
    print(table.query_info())

    # Now, Select all elements in the table.
    print("-----------------------")
    print("All elements in the DB")
    print(table.select_all())

    # Now, Select one element in the table.
    print("-----------------------")
    print("ONE element selected in the DB")
    print(table.select_one(name='S'))

    # Now, Let's modify the color of the Tesla Model S.
    print("-----------------------")
    print("ONE element selected in the DB")
    print(table.modify(name='Model S', color="Red"))


    # Don't forget to commit Change in the database !!!
    database.commit()

    # Close Database
    database.close()

    # Reopen Database
    dbobj = SQLDatabase(databaseName="./DB")


    # Define Table "Ingredient"
    param_default = {'name': 'TEXT PRIMARY KEY', 'calory': 'INTEGER', 'portion': 'FLOAT', 'unit': 'TEXT'}
    table_ingredient = SQLTable(SQLdbObj=dbobj, tableName="Ingredient", **param_default)

    # define filter before insertion: These paramters must be UNIQUE in the table...
    table_ingredient.define_filter_for_insertion(['name'])

    # Insert into table Ingredient
    try:
        table_ingredient.insert(name='Tiramisu', calory=10000, portion=3.2, unit='kg') ##  ¯\_(ツ)_/¯
        table_ingredient.insert(**{'name':'Tomato', 'calory':10, 'portion':123, 'unit':'g'})
    except SqlDoubleItemsOccurs as e:
        print(e.args[0])

    # Don't forget to commit Change in the database !!!
    dbobj.commit()

    # Define Table "Cars"
    param_default = {'name': 'TEXT PRIMARY KEY', 'tata': 'FLOAT', 'titi': 'INTEGER', 'babar': 'TEXT'}
    table_car = SQLTable(SQLdbObj=dbobj, tableName="Cars", **param_default)

    # On query, we can see that table cars already exist because it retrieve information from pre-existing table instead of yours...
    print("-----------------------")
    print("Query the table")
    print(table_car.query_info())

    # Now, Select one element in the table.
    print("-----------------------")
    print("All elements in the DB")
    print(table_car.select_all())

    # Now, list all table in the database.
    print("-----------------------")
    print("All tables in the database")
    print(dbobj.list_table())

    # Close Database
    dbobj.close()
