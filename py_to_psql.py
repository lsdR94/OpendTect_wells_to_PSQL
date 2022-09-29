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