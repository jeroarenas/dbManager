"""
This class provides functionality for managing a generic sqlite
or mysql database:

Created on May 11 2018

@authors: Jerónimo Arenas García (jeronimo.arenas@uc3m.es)
          Saúl Blanco Fortes (sblanco@tsc.uc3m.es)
          Jesús Cid Sueiro (jcid@ing.uc3m.es)

Exports class BaseDMsql that can be used to derive new classes
for specific projects that may include table creation, data import,
etc for a particular project

The base clase provided in this file implements the following methods:
* __init__        : The constructor of the class. It creates connection
                    to a particular database
* __del__         : Cleanly closes the database
* deleteDBtables  : Deletes table(s) from database
* addTableColumn  : Adds a column at the end of table of the database
* dropTableColumn : Removes column from table (only MySQL)
* readDBtable     : Reads rows from table and returns a pandas dataframe
                    with the retrieved data
* readDBchunks    : RProvides an iterator to read chunks of rows in table.
                    Each iteration returns a chunk of predefined max number of rows
                    to avoid stalling the mysql server
* getTableNames   : Gets the names of the tables in the database
* getColumnNames  : Gets the names of the columns in a particular table
* getTableInfo    : Gets the number of rows and the names of columns in table
* insertInTable   : Insert new records in Table. Input data comes as a list of tuples.
* deleteFromTable : Delete records from Table. Conditions are given on columname and values
* setField        : Updates table records. Input data comes as a list of tuples.
* upsert          : Update or insert records in a table. Input data comes as panda df
                    If the record exists (according to primary key) data will be updated
                    If the record does not exist, new records will be created
* exportTable     : Export a table from database either as pickle or excel file
* DBdump          : Creates dump of full database, or dump of selected tables
* execute         : Execute SQL command received as parameter

"""

import os
import pandas as pd
import MySQLdb
import sqlite3
import numpy as np
import copy
from progress.bar import Bar

def chunks(l, n):
    """Yields successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

class BaseDMsql(object):
    """
    Data manager base class.
    """

    def __init__(self, db_name, db_connector, path2db=None,
                 db_server=None, db_user=None, db_password=None,
                 db_port=None, unix_socket=None, charset='utf8mb4'):
        """
        Initializes a DataManager object

        Args:
            db_name      :Name of the DB
            db_connector :Connector. Available options are mysql or sqlite
            path2db      :Path to the project folder (sqlite only)
            db_server    :Server (mysql only)
            db_user      :User (mysql only)
            db_password  :Password (mysql only)
            db_port      :port(mysql via TCP only) Necessary if not 3306
            unix_socket  :socket for local connectivity. If available, connection
                          is slightly faster than through TCP. 
            charset      :Codificación a utilizar por defecto en la conexión
        """

        # Store paths to the main project folders and files
        self._path2db = copy.copy(path2db)
        self.dbname = db_name
        self.connector = db_connector
        self.server = db_server
        self.user = db_user
        self.password = db_password
        self.port = db_port
        self.unix_socket = unix_socket

        # Other class variables
        self.dbON = False    # Will switch to True when the db is connected.
        # Connector to database
        self._conn = None
        # Cursor of the database
        self._c = None

        # Try connection
        try:
            if self.connector == 'mysql':
                if self.unix_socket:
                    self._conn = MySQLdb.connect(host=self.server,
                                    user=self.user, passwd=self.password,
                                    db=self.dbname,
                                    unix_socket=unix_socket, charset=charset)
                elif self.port:
                    self._conn = MySQLdb.connect(host=self.server,
                                    user=self.user, passwd=self.password,
                                    db=self.dbname, port=self.port,
                                    charset=charset)
                else:
                    self._conn = MySQLdb.connect(host=self.server,
                                    user=self.user, passwd=self.password,
                                    db=self.dbname, charset=charset)
                self._c = self._conn.cursor()
                print('MySQL database connection successful. Default database:', self.dbname)
                self.dbON = True
                #self._conn.set_character_set('utf8')
            elif self.connector == 'sqlite3':
                # sqlite3
                # sqlite file will be in the root of the project, we read the
                # name from the config file and establish the connection
                db_fname = os.path.join(self._path2db,
                                        self.dbname + '.db')
                print("---- Connecting to {}".format(db_fname))
                self._conn = sqlite3.connect(db_fname)
                self._c = self._conn.cursor()
                self.dbON = True
            else:
                print("---- Unknown DB connector {}".format(self.connector))
        except:
            print("---- Error connecting to the database")

        return


    def __del__(self):
        """
        When destroying the object, it is necessary to commit changes
        in the database and close the connection
        """

        try:
            self._conn.commit()
            self._conn.close()
        except:
            print("---- Error closing database")

        return

    """def setConnCharset(self, charsetcode):
        self._conn.set_character_set(charsetcode)
        return
    """

    def deleteDBtables(self, tables=None):
        """
        Delete tables from database

        Args:
            tables: If string, name of the table to reset.
                    If list, list of tables to reset
                    If None (default), all tables are deleted
        """

        # If tables is None, all tables are deleted and re-generated
        if tables is None:
            # Delete all existing tables
            self._c.execute('SET FOREIGN_KEY_CHECKS = 0')
            for table in self.getTableNames():
                self._c.execute("DROP TABLE " + table)
            self._c.execute('SET FOREIGN_KEY_CHECKS = 1')

        else:
            # It tables is not a list, make the appropriate list
            if type(tables) is str:
                tables = [tables]

            # Remove all selected tables (if exist in the database).
            for table in set(tables) & set(self.getTableNames()):
                self._c.execute("DROP TABLE " + table)

        self._conn.commit()

        return

    def addTableColumn(self, tablename, columnname, columntype):
        """
        Add a new column to the specified table.

        Args:
            tablename  : Table to which the column will be added
            columnname : Name of new column
            columntype : Type of new column.

        """

        # Check if the table exists
        if tablename in self.getTableNames():

            # Check that the column does not already exist
            if columnname not in self.getColumnNames(tablename):

                #Allow columnames with spaces
                columnname = '`'+columnname+'`'

                """# Fit characters to the allowed format if necessary
                fmt = ''
                if (self.connector == 'mysql' and
                    ('TEXT' in columntype or 'VARCHAR' in columntype) and
                    not ('CHARACTER SET' in columntype or
                         'utf8mb4' in columntype)):

                    # We enforze utf8mb4 for mysql
                    fmt = ' CHARACTER SET utf8mb4'


                sqlcmd = ('ALTER TABLE ' + tablename + ' ADD COLUMN ' +
                          columnname + ' ' + columntype + fmt)"""
                sqlcmd = ('ALTER TABLE ' + tablename + ' ADD COLUMN ' +
                          columnname + ' ' + columntype)          
                self._c.execute(sqlcmd)

                # Commit changes
                self._conn.commit()

            else:
                print(("WARNING: Column {0} already exists in table {1}."
                       ).format(columnname, tablename))

        else:
            print('Error adding column to table. Please, select a valid ' +
                  'table name from the list')
            print(self.getTableNames())

        return

    def dropTableColumn(self, tablename, columnname):
        """
        Remove column from the specified table

        Args:
            tablename    :Table from which the column will be removed
            columnname   :Name of column to be removed

        """

        # Check if the table exists
        if tablename in self.getTableNames():

            # Check that the column exists
            if columnname in self.getColumnNames(tablename):

                #Allow columnames with spaces
                columname = '`'+columnname+'`'

                # ALTER TABLE DROP COLUMN IS ONLY SUPPORTED IN MYSQL
                if self.connector == 'mysql':

                    sqlcmd = ('ALTER TABLE ' + tablename + ' DROP COLUMN ' +
                              columnname)
                    self._c.execute(sqlcmd)

                    # Commit changes
                    self._conn.commit()

                else:
                    print('Error deleting column. Column drop not supported for SQLITE')

            else:
                print('Error deleting column. The column does not exist')
                print(tablename, columnname)

        else:
            print('Error deleting column. Please, select a valid table name' +
                  ' from the list')
            print(self.getTableNames())

        return

    def readDBtable(self, tablename, limit=None, selectOptions=None,
                    filterOptions=None, orderOptions=None):
        """
        Read data from a table in the database can choose to read only some
        specific fields

        Args:
            tablename    :  Table to read from
            selectOptions:  string with fields that will be retrieved
                            (e.g. 'REFERENCIA, Resumen')
            filterOptions:  string with filtering options for the SQL query
                            (e.g., 'UNESCO_cd=23')
            orderOptions:   string with field that will be used for sorting the
                            results of the query
                            (e.g, 'Cconv')
            limit:          The maximum number of records to retrieve

        """

        try:

            sqlQuery = 'SELECT '
            if selectOptions:
                sqlQuery = sqlQuery + selectOptions
            else:
                sqlQuery = sqlQuery + '*'

            sqlQuery = sqlQuery + ' FROM ' + tablename + ' '

            if filterOptions:
                sqlQuery = sqlQuery + ' WHERE ' + filterOptions

            if orderOptions:
                sqlQuery = sqlQuery + ' ORDER BY ' + orderOptions

            if limit:
                sqlQuery = sqlQuery + ' LIMIT ' + str(limit)

            # This is to update the connection to changes by other
            # processes.
            self._conn.commit()

            # Return the pandas dataframe. Note that numbers in text format
            # are not converted to
            return pd.read_sql(sqlQuery, con=self._conn,
                               coerce_float=False)

        except Exception as E:
            print(str(E))
            print('Error in query:', sqlQuery)
            return

    def readDBchunks(self, tablename, orderField, chunksize=50000,
                        selectOptions=None, limit=None, filterOptions=None, verbose=False):
        """
        Read data from a table in the database using chunks. 
        Can choose to read only some specific fields
        Rather than returning a dataframe, it returns an iterator that builds 
        dataframes of desired number of rows (chunksize)

        Args:
            tablename    :  Table to read from
            orderField   :  name of field that will be used for sorting the
                            results of the query (e.g, 'ID'). This is the column
                            that will be used for iteration, so this variable is mandatory
            chunksize    :  Length of chunks for reading the table. Default value: 50000
            selectOptions:  string with fields that will be retrieved
                            (e.g. 'REFERENCIA, Resumen')
                            If None, all columns will be retrieved
            limit:          The total maximum number of records to retrieve.
            filterOptions:  string with filtering options for the SQL query
                            (e.g., 'UNESCO_cd=23')
            verbose      :  If True, information on the number of rows read so far will be
                            displayed
        """

        if limit:
            remaining = limit
            next_chunk = min(remaining, chunksize)
        else:
            next_chunk = chunksize

        cont = 0
        
        selectOptions = selectOptions + ', ' + orderField

        df = self.readDBtable(tablename, limit=next_chunk, selectOptions=selectOptions,
                filterOptions = filterOptions, orderOptions=orderField)

        while (len(df)):
            cont = cont+len(df)
            if verbose:
                print('[DBManager (readDBchunks)] Number of rows read so far:', cont)
            if limit:
                remaining = limit - cont
                next_chunk = min(remaining, chunksize)
            else:
                next_chunk = chunksize
            yield df.iloc[:,:-1]

            #Next we need to start from last retrieved element
            filtercondition = orderField + '>' + str(df.iloc[:,-1][len(df)-1])
            if filterOptions:
                filtercondition = filtercondition + ' AND ' + filterOptions
            
            if next_chunk>0:
                df = self.readDBtable(tablename, limit=next_chunk, selectOptions=selectOptions,
                        filterOptions = filtercondition, orderOptions=orderField)
            else:
                #If maximum number of records has been reached, set df to empty list to exit
                df = []


    def getTableNames(self):
        """
        Returns a list with the names of all tables in the database
        """

        # The specific command depends on whether we are using mysql or sqlite
        if self.connector == 'mysql':
            sqlcmd = ("SELECT table_name FROM INFORMATION_SCHEMA.TABLES " +
                      "WHERE table_schema='" + self.dbname + "'")
        else:
            sqlcmd = "SELECT name FROM sqlite_master WHERE type='table'"

        self._c.execute(sqlcmd)
        tbnames = [el[0] for el in self._c.fetchall()]

        return tbnames

    def getColumnNames(self, tablename):
        """
        Returns a list with the names of all columns in the indicated table

        Args:
            tablename: the name of the table to retrieve column names
        """

        # Check if tablename exists in database
        if tablename in self.getTableNames():
            # The specific command depends on whether we are using mysql or
            #  sqlite
            if self.connector == 'mysql':
                sqlcmd = "SHOW COLUMNS FROM " + tablename
                self._c.execute(sqlcmd)
                columnnames = [el[0] for el in self._c.fetchall()]
            else:
                sqlcmd = "PRAGMA table_info(" + tablename + ")"
                self._c.execute(sqlcmd)
                columnnames = [el[1] for el in self._c.fetchall()]

            return columnnames

        else:
            print('Error retrieving column names: Table does not exist on ' +
                  'database')
            return []

    def getTableInfo(self, tablename):

        # Get columns
        cols = self.getColumnNames(tablename)

        # Get number of rows
        sqlcmd = "SELECT COUNT(*) FROM " + tablename
        self._c.execute(sqlcmd)
        n_rows = self._c.fetchall()[0][0]

        return cols, n_rows

    def insertInTable(self, tablename, columns, arguments, chunksize=None, verbose=False):
        """
        Insert new records into table

        Args:
            tablename:  Name of table in which the data will be inserted
            columns:    Name of columns for which data are provided
            arguments:  A list of lists or tuples, each element associated
                        to one new entry for the table
            chunksize: If chunksize is not None, Data will be inserted in chunks
                       of the specified size
            verbose: True or False. If chunksize is active and verbose is True, 
                     the progress of the chunks insertion is displayed on screen
        """

        # Make sure columns is a list, and not a single string
        if not isinstance(columns, (list,)):
            columns = [columns]

        # To allow for column names that have spaces
        columns = list(map(lambda x: '`'+x+'`', columns))

        ncol = len(columns)

        if len(arguments[0]) == ncol:
            # Make sure the tablename is valid
            if tablename in self.getTableNames():
                # Make sure we have a list of tuples; necessary for mysql
                arguments = list(map(tuple, arguments))

                sqlcmd = ('INSERT INTO ' + tablename +
                              '(' + ','.join(columns) + ') VALUES (')
                if self.connector == 'mysql':
                    sqlcmd += '%s' + (ncol-1)*',%s' + ')'
                else:    
                    sqlcmd += '?' + (ncol-1)*',?' + ')'

                if chunksize:

                    n_chunks = np.ceil(len(arguments)/chunksize)
                    if verbose:
                        print('\n')
                        bar = Bar('Inserting chunks of data in database', max=n_chunks)
                    for chk in chunks(arguments, chunksize):
                        if verbose:
                            bar.next()
                        self._c.executemany(sqlcmd, chk)
                        self._conn.commit()

                    if verbose:
                        bar.finish()

                else:

                    self._c.executemany(sqlcmd, arguments)
                    # Commit changes
                    self._conn.commit()
            else:
                print('Error inserting data in table: The table does not exist')
        else:
            print('Error inserting data in table: number of columns mismatch')

        return

    def deleteFromTable(self, tablename, columns, arguments, chunksize=None, verbose=False):
        """
        Delete rows from table

        Args:
            tablename:  Name of table from which data will be removed
            columns:    Name of columns for which data are provided
            arguments:  A list of lists or tuples, conditions for data to be removed
            chunksize: If chunksize is not None, Data will be deleted in chunks
                       of the specified size
            verbose: True or False. If chunksize is active and verbose is True, 
                     the progress of the chunks deletion is displayed on screen

        E.g., if columns is ['userID','productID'] and arguments is
        [['234', '227'],['234', '228']] this function will delete from the
        table all rows where userID='234' AND productID='227', and all rows
        where userID='234' and productID='228'
        """

        # Make sure columns is a list, and not a single string
        if not isinstance(columns, (list,)):
            columns = [columns]

        # To allow for column names that have spaces
        columns = list(map(lambda x: '`'+x+'`', columns))

        ncol = len(columns)

        if len(arguments[0]) == ncol:
            # Make sure the tablename is valid
            if tablename in self.getTableNames():
                # Make sure we have a list of tuples; necessary for mysql
                arguments = list(map(tuple, arguments))

                sqlcmd = 'DELETE FROM ' + tablename + ' WHERE '
                if self.connector == 'mysql':
                    sqlcmd += ' AND '.join([el + '=%s' for el in columns])
                else:    
                    sqlcmd += ' AND '.join([el + '=?' for el in columns])

                if chunksize:

                    n_chunks = np.ceil(len(arguments)/chunksize)
                    if verbose:
                        print('\n')
                        bar = Bar('Deleting data from database', max=n_chunks)
                    for chk in chunks(arguments, chunksize):
                        if verbose:
                            bar.next()
                        self._c.executemany(sqlcmd, chk)
                        self._conn.commit()

                    if verbose:
                        bar.finish()

                else:

                    self._c.executemany(sqlcmd, arguments)
                        # Commit changes
                    self._conn.commit()

            else:
                print('Error deleting data from table: The table does not exist')
        
        else:
            print('Error deleting data from table table: number of columns mismatch')

        return

    def setField(self, tablename, keyfld, valueflds, values):
        """
        Update records of a DB table

        Args:
            tablename:  Table that will be modified
            keyfld:     string with the column name that will be used as key
                        (e.g. 'REFERENCIA')
            valueflds:  list with the names of the columns that will be updated
                        (e.g., 'Lemas')
            values:     A list of tuples in the format
                            (keyfldvalue, valuefldvalue)
                        (e.g., [('Ref1', 'gen celula'),
                                ('Ref2', 'big_data, algorithm')])

        """

        # Auxiliary function to circularly shift a tuple one position to the
        # left
        def circ_left_shift(tup):
            ls = list(tup[1:]) + [tup[0]]
            return tuple(ls)

        # Make sure valueflds is a list, and not a single string
        if not isinstance(valueflds, (list,)):
            valueflds = [valueflds]

        # To allow for column names that have spaces
        valueflds = list(map(lambda x: '`'+x+'`', valueflds))

        ncol = len(valueflds)

        if len(values[0]) == (ncol+1):
            # Make sure the tablename is valid
            if tablename in self.getTableNames():

                # # Update DB entries one by one.
                # # WARNING: THIS VERSION MAY NOT WORK PROPERLY IF v
                # #          HAS A STRING CONTAINING "".
                # for v in values:
                #     sqlcmd = ('UPDATE ' + tablename + ' SET ' +
                #               ', '.join(['{0} ="{1}"'.format(f, v[i + 1])
                #                          for i, f in enumerate(valueflds)]) +
                #               ' WHERE {0}="{1}"'.format(keyfld, v[0]))
                #     self._c.execute(sqlcmd)

                # This is the old version: it might not have the problem of
                # the above version, but did not work properly with sqlite.
                # Make sure we have a list of tuples; necessary for mysql
                # Put key value last in the tuples
                values = list(map(circ_left_shift, values))

                sqlcmd = 'UPDATE ' + tablename + ' SET '
                if self.connector == 'mysql':
                    sqlcmd += ', '.join([el+'=%s' for el in valueflds])
                    sqlcmd += ' WHERE ' + keyfld + '=%s'
                else:
                    sqlcmd += ', '.join([el+'=?' for el in valueflds])
                    sqlcmd += ' WHERE ' + keyfld + '=?'

                self._c.executemany(sqlcmd, values)

                # Commit changes
                self._conn.commit()

            else:
                print('Error udpating table values: The table does not exist')
        else:
            print('Error updating table values: number of columns mismatch')

        return

    def upsert(self, tablename, keyfld, df, robust=True):

        """
        Update records of a DB table with the values in the df
        This function implements the following additional functionality:
        * If there are columns in df that are not in the SQL table,
          columns will be added
        * New records will be created in the table if there are rows
          in the dataframe without an entry already in the table. For this,
          keyfld indicates which is the column that will be used as an
          index

        Args:
            tablename:  Table that will be modified
            keyfld:     string with the column name that will be used as key
                        (e.g. 'REFERENCIA')
            df:         Dataframe that we wish to save in table tablename
            robust:     If False, verifications are skipped
                        (for a faster execution)

        """

        # Check that table exists and keyfld exists both in the Table and the
        # Dataframe
        if robust:
            if tablename in self.getTableNames():
                if not ((keyfld in df.columns) and
                   (keyfld in self.getColumnNames(tablename))):
                    print("Upsert function failed: Key field does not exist",
                          "in the selected table and/or dataframe")
                    return
            else:
                print('Upsert function failed: Table does not exist')
                return

        # Reorder dataframe to make sure that the key field goes first
        flds = [keyfld] + [x for x in df.columns if x != keyfld]
        df = df[flds]

        if robust:
            # Create new columns if necessary
            for clname in df.columns:
                if clname not in self.getColumnNames(tablename):
                    if df[clname].dtypes == np.float64:
                        self.addTableColumn(tablename, clname, 'DOUBLE')
                    else:
                        if df[clname].dtypes == np.int64:
                            self.addTableColumn(tablename, clname, 'INTEGER')
                        else:
                            self.addTableColumn(tablename, clname, 'TEXT')

        # Check which values are already in the table, and split
        # the dataframe into records that need to be updated, and
        # records that need to be inserted
        keyintable = self.readDBtable(tablename, limit=None,
                                      selectOptions=keyfld)
        keyintable = keyintable[keyfld].tolist()
        values = [tuple(x) for x in df.values]
        values_insert = list(filter(lambda x: x[0] not in keyintable, values))
        values_update = list(filter(lambda x: x[0] in keyintable, values))

        if len(values_update):
            self.setField(tablename, keyfld, df.columns[1:].tolist(),
                          values_update)
        if len(values_insert):
            self.insertInTable(tablename, df.columns.tolist(), values_insert)

        return

    def exportTable(self, tablename, fileformat, path, filename, cols=None):
        """
        Export columns from a table to a file.

        Args:
            :tablename:  Name of the table
            :fileformat: Type of output file. Available options are
                            - 'xlsx'
                            - 'pkl'
            :filepath:   Route to the output folder
            :filename:   Name of the output file
            :columnames: Columns to save. It can be a list or a string
                         of comma-separated columns.
                         If None, all columns saved.
        """

        # Path to the output file
        fpath = os.path.join(path, filename)

        # Read data:
        if cols is list:
            options = ','.join(cols)
        else:
            options = cols

        df = self.readDBtable(tablename, selectOptions=options)

        # ######################
        # Export results to file
        if fileformat == 'pkl':
            df.to_pickle(fpath)

        else:
            df.to_excel(fpath)

        return

    def DBdump(self, filename, tables=None):
        """
        Creates dump of database

        Args:
            :filename:   Name of the output file
            :tables:     List of tables to include in the dump
                         If None, all columns saved.
        """

        if self.connector == 'mysql':

            # If tables is None, all tables are included in dump
            if tables is None:
                table_list = ''

            else:

                # It tables is not a list, make the appropriate list
                if type(tables) is str:
                    tables = [tables]

                table_list = ' ' + ' '.join(tables)

            try:
                dumpcmd = 'mysqldump -h ' + self.server + ' -u ' + self.user + \
                          ' -p' + self.password + ' ' + self.dbname + table_list + \
                          ' > ' + filename
                os.system(dumpcmd)
            except: 
                print('Error when creating dump. Check route to filename')

        else:

            print('Database dump only supported for MySQL databases')

        return

    def execute(self, sqlcmd):
        """
        Execute SQL command received as parameter

        Args:
            :
        """
        self._c.execute(sqlcmd)
        return
