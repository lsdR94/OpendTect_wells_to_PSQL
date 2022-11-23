import sys
import traceback as tb
import psycopg2 as p
import time
import pandas as pd
import numpy as np

REPLACE_DICT = {
    "[": "",
    "'": "",
    ", ": ",",
    ",  ": ",",
    " ": "_",
    "-": "_",
    "/": "_",
    "`": "_",
    "]": ""
}

def execute_psql_command(command, connection):
    """
    Executes PSQL commands.
    
    ARGUMENTS
    ---------
        command : str
            PSQL commands.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
    
    RETURN
    ------
        str
            Prints whether the command could be executed successfully
            or not. Also includes the command execution time on psql 
            server.
    """
    try:
        init = time.time()
        # PSQL cursor
        cursor = connection.cursor() 
        cursor.execute(command)
        connection.commit()
        end = time.time()
        return(f"Query has been executed successfully in {end - init}s"
        )
    except Exception as e:
        # Terminate connection
        connection.commit()
        end = time.time()
        print("Traceback details: ")
        details = tb.format_tb(e.__traceback__)
        print("\n".join(details))
        print(e.__class__.__name__, ":", e)
        print(f"Query can not be processed. Execution time = {end - init}s")
        
def wells_table_creation(table_name, connection, column_list=[]):
    """
    Creates psql tables based on OPENDTECT wells info.
    
    PARANETERS
    ----------
        table_name : str
            PostgreSQL table to create.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
            
        column_list : list (optional)
            List of PSQL statements for a more flexible table creation.
    
    RETURN
    ------
        PSQL table. The default table is created using the 
        following columns:
            - well_id SERIAL NOT NULL UNIQUE PRIMARY KEY
            - opendtect_id NUMERIC(10,1) NOT NULL
            - well_name VARCHAR(30) NOT NULL UNIQUE
            - x_coordinate NUMERIC(10,2) NOT NULL
            - y_coordinate NUMERIC(10,2) NOT NULL
            - status VARCHAR(30) NOT NULL
    """
    
    if len(column_list) != 0:
        table_creation_query = f"CREATE TABLE {table_name}("
        for column in column_list:
            table_creation_query += column
        table_creation_query += ")"   
    else:
        table_creation_query = f"""
            CREATE TABLE {table_name}(
                well_id SERIAL NOT NULL UNIQUE PRIMARY KEY,
                opendtect_id NUMERIC(10,1) NOT NULL,
                well_name VARCHAR(30) NOT NULL UNIQUE,
                x_coordinate NUMERIC(10,2) NOT NULL,
                y_coordinate NUMERIC(10,2) NOT NULL,
                status VARCHAR(30) NOT NULL
            )
            """
    return(execute_psql_command(table_creation_query, connection))

def fetch_psql_command(command, connection):
    """
    Make queries and return server's output.
    
    ARGUMENTS
    ---------
        command : str
            PSQL commands.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
    
    RETURN
    ------
        Tuple (column_names, query_result)
            Psycopg2 doesn't returns columns with its fetchall 
            method; therefore, the tuple contains column_names plus 
            query results.
    """
    try:
        init = time.time()
        # PSQL cursor
        cursor = connection.cursor() 
        cursor.execute(command)
        query_result = cursor.fetchall()
        column_names = [col_name[0] for col_name in cursor.description]
        connection.commit()
        end = time.time()
        # print(f"Fetch command has been executed successfully. Execution time = {end - init}s")
        return (column_names, query_result)
    except Exception as e:
        # Terminate connection
        connection.commit()
        end = time.time()
        print("Traceback details: ")
        details = tb.format_tb(e.__traceback__)
        print("\n".join(details))
        print(e.__class__.__name__, ":", e)
        print(f"Command can not be processed. Execution time = {end - init}s")

def fetch_column_names(table_name, connection, limit=0):
    """
    Fetches only column names of the given table.
    
    ARGUMENTS
    ---------
        table_name : str
            PSQL table object.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
            
        limit : int
            0 by default. Adds a query limit of 0 to return only the 
            column names.
    
    RETURN
    ------
        fetch_psql_command
            fetch_psql_command method that returns only the table's 
            column names.
    """
    column_names_query = f"SELECT * FROM {table_name} LIMIT {limit}"
    return(fetch_psql_command(column_names_query, connection)[0])

def csv_to_df(file_path, sep=",", feet=True, columns=None, encoding="latin-1"):
    """
    Creates a Pandas Dataframe from csv file.
    
    If provided, can convert feet to meter columns, fillna and
    round numeric columns.
    
    ARGUMENTS
    ---------
        file_path : str
            Location of CSV file.
        
        sep : str
            CSV's column separator. ',' by default.
            
        encoding : str
            Encoding format for Pandas.
            
        columns : list
            List of columns:
            [0] Dataframe columns
            [1] String columns
            [2] Numeric columns
            [3] Feet columns
    
    RETURN
    ------
        Pandas.DataFrame
    """
    df = pd.read_csv(file_path, sep, encoding=encoding)
    #Reformating columns
    if columns != None:
        df.columns = columns[0]
        #Replace nan for 0.0 in numeric columns to ease psql imports
        df[columns[2]] = df[columns[2]].fillna(float(0))
        df[columns[2]] = np.round(df[columns[2]], 2)
    #Convert feet into meters
    if feet:
        df[columns[3]] *= 0.3048

    # df_nulls = df.replace(to_replace=["*", np.NaN], value=["", ""])
    return df

def string_replacement(string, replace_dict=REPLACE_DICT):
    """
    Replaces characters for others.
    
    Allows to ease string formatting for queries.
    
    ARGUMENTS
    ---------
        string : str
            Unformatted string.
        
        replace_dict : dict
            Pair of targets and replacements. REPLACE_DICT by default.
            
    RETURN
    ------
        str
            Formatted string.
    
    """
    for key,value in replace_dict.items():
        string = string.replace(key, value)
    return string

def df_to_psql_bc(df, table_name, well_name, connection, on_conflict_do="NOTHING"):
    """
    Inserts data from dataframes to PSQL tables BY COLUMNS.
    
    Aimed at storing litoestratigraphy of all wells vectorially in a single 
    table. Stores a single log into PSQL arrays.
        
    ARGUMENTS
    ---------
        df : Pandas.DataFrame
            Structured data. Pandas DataFrame.
        
        table_name : str
            PSQL table object.
            
        well_name : str
            Well's database name.
            
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
            
        on_conflict_do : str
            PSQL statements for data updates. (DO) NOTHING by default.
                
    RETURN
    ------
        str
            Insertion Query.
    """
    #Recover table columns (same as df)
    table_df_columns = fetch_column_names(table_name, connection)
    #PSQL statements
    insert_statement = f"INSERT INTO {table_name}({string_replacement(str(table_df_columns))}) "
    values_statement = f"VALUES('{well_name}', "
    conflict_statement = f"ON CONFLICT ({table_df_columns[0]}) DO "
    conflict_statement += f"{on_conflict_do};"
    for column in df.columns:
        if column != df.columns[-1]:
            values_statement += f"array{df[column].to_list()}, ".replace("nan", "NULL")
        if column == df.columns[-1]:
            values_statement += f"array{df[column].to_list()}) ".replace("nan", "NULL")
    insert_query = insert_statement + values_statement + conflict_statement
    return (insert_query)

def df_to_psql_br(df, table_name, connection, on_conflict_do="NOTHING"):
    """
    Inserts data from dataframes to PSQL tables BY ROWS.
    
    Aimed at storing single data into a table.
        
    ARGUMENTS
    ---------
        df : Pandas.DataFrame
            Structured data. Pandas DataFrame.
        
        table_name : str
            PSQL table object.
                      
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
            
        on_conflict_do : str
            PSQL statements for data updates. (DO) NOTHING by default.
                
    RETURN
    ------
        str
            Insertion Query.
    """
    #Recover table columns (same as df)
    table_df_columns = fetch_column_names(table_name, connection)
    #PSQL statements
    insert_statement = f"INSERT INTO {table_name}({string_replacement(str(table_df_columns))}) "
    values_statement = f"VALUES"
    conflict_statement = f"ON CONFLICT ({table_df_columns[0]}) DO "
    conflict_statement += f"{on_conflict_do};"
    for values in df.values:
        if list(values) != list(df.values[-1]):
            values_statement += f"({str(list(values))}),".replace("nan", "NULL").replace("[", "").replace("]", "")
        if list(values) == list(df.values[-1]):
            values_statement += f"({str(list(values))})".replace("nan", "NULL").replace("[", "").replace("]", "")
    insert_query = insert_statement + values_statement + conflict_statement
    return (insert_query)