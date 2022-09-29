import sys
import traceback as tb
import psycopg2 as p
import time

def execute_psql_command(command, connection):
    """
    Executes PSQL commands.
    
    ARGUMENTS
    ---------
        command : str
            PSQL commands.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user
            and PSQL server.
    
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
        return(f"Command has been executed successfully. Execution time = {end - init}s")
    except Exception as e:
        # Terminate connection
        connection.commit()
        end = time.time()
        print("Traceback details: ")
        details = tb.format_tb(e.__traceback__)
        print("\n".join(details))
        print(e.__class__.__name__, ":", e)
        print(f"Command can not be processed. Execution time = {end - init}s")
        
def wells_table_creation(table_name, connection, column_list=[]):
    """
    Creates psql tables based on OPENDTECT wells info.
    
    PARANETERS
    ----------
        table_name : str
            PostgreSQL table to create.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user
            and PSQL server.
            
        column_list : list (optional)
            List of PSQL statements for a more flexible table
            creation.
    
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
            Parameters to create a connection between end user
            and PSQL server.
    
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
        print(f"Command has been executed successfully. Execution time = {end - init}s")
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
        table_name : psql table
            PSQL table object.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user
            and PSQL server.
            
        limit : int
            0 by default. Adds a query limit of 0 to return
            only the column names.
    
    RETURN
    ------
        fetch_psql_command
            fetch_psql_command method that returns only the
            table's column names.
    """
    column_names_query = f"SELECT * FROM {table_name} LIMIT {limit}"
    return(fetch_psql_command(column_names_query, connection)[0]) 