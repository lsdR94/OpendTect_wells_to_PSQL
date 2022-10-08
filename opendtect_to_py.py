import time
import py_to_psql as pp
import odpy.wellman as wm

def fetch_opendtect_wells_info():
    """
   Fetches OPENDTECT wells info. 
   
   Maps OPENDTECT well names into wellman's getInfo method.
   
   RETURNS
   -------
       Generator of dictionaries with well information. 
   """

    well_names = wm.getNames(reload=True)
    # wellman lambda function
    def lambdaf(well_name): return (wm.getInfo(well_name)) 
    return map(lambdaf, well_names)

def insert_wells(
    table_name, 
    name_column_name, 
    connection, 
    values_statement=None
):
    """
    Insert OPENDTECT well data into given table.
    
    The defaults OPENDTECT keys used are as follows:
        - ID
        - Name
        - X
        - Y
        - Status
    These default keys are related to the ones used in the
    wells_table_creation method
    
    ARGUMENTS
    ---------
        table_name : str
            PSQL table object.
            
        name_column_name : str
            Name of the PSQL table column name.
            
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user
            and PSQL server.
            
        values_statement : list (optional)
            List of PSQL statements for a more flexible table
            insertion.
       
    RETURN
    ------
           str
               Finalization message + execution time.
     
     FOOT NOTES
     ----------
         This method is.. a bit of hardcoded. I'm very sorry.
         values_statement argument is provided in order to allow
         a more flexible insertion if you are using other columns.
    
    """
    init = time.time()
    # column names & fetch wells
    column_names = pp.fetch_column_names(table_name, connection)
    opendtect_wells_info = fetch_opendtect_wells_info()
    # PSQL statement: conflict & insert
    conflict_statement = f"ON CONFLICT ({name_column_name}) DO NOTHING"
    insert_statement = f"INSERT INTO {table_name}("
    for col_name in column_names[1:]:
        if col_name != column_names[-1]:
            insert_statement += f"{col_name}, "
        else:
            insert_statement += f"{col_name}) "
    # PSQL statement: values
    for well_dict in opendtect_wells_info:
        init2 = time.time()
        if values_statement == None:
            values_statement = f"""
                VALUES(
                    {well_dict['ID']}, 
                    '{well_dict['Name']}', 
                    {well_dict['X']}, 
                    {well_dict['Y']}, 
                    '{well_dict['Status']}'
                )
            """
        upsertion_query = insert_statement + values_statement + conflict_statement
        pp.execute_psql_command(upsertion_query, connection)
        end2 = time.time()
        print(f"Well {well_dict['Name']} insertion completed in {end2 - init2}s")
    end = time.time()
    return f"Wells insertion completed in {end - init}s"

def fetch_opendtect_well_log(well_name, log_name):
    """
    Fetches a well log.
    
    ARGUMENTS
    ---------
        well_name : str
            Well database name.
            
        log_name : str
            Log name as reported by wellman.
    
    RETURN
    ------
        Tuple
            Two list [arrays for psycopg2] with depths (MD) and 
            log values.
        
        Empty list
            If there isn't any log by log_name related to well_name.
    
    """
    try:
        # log n array
        if log_name == "track":
            log = wm.getTrack(well_name)
        else:
            log = wm.getLog(well_name, log_name)
        return (log)
    except Exception:
        print(f"Log {log_name} not found for Well {well_name}.")
        return([])
    
def insert_log(
    table_name, 
    wells_table, 
    well_name, 
    log_name, 
    connection,
    on_conflict_do="NOTHING"
):
    """
    Insert a single log (of given well) into a table.
    
    If there is no log, null values will be inserted in
    the table.
    
    ARGUMENTS
    ---------
        table_name : str
            PSQL table object.
        
        wells_table : str
            PSQL wells table, where the basic well info
            is stored.
        
        well_name : str
            Well database name.    
        
        log_name : str
            Log name as reported by wellman.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user
            and PSQL server.
    
    RETURNS
    -------
        str
            Prints the finalization of the inserting process. Includes
            execution time.
            
    FOOT NOTES
    ----------
         The construction of the PSQL can be improved.
            
    """
    def array_check(array): return(str([None if sample == 1e+30 else sample for sample in array]).replace("None", "Null"))
    init = time.time()
    # column names & fetch log
    column_names = pp.fetch_column_names(table_name, connection)
    log = fetch_opendtect_well_log(well_name, log_name)
    # fetch well id
    well_id_query = f"SELECT {column_names[0]} FROM {wells_table} WHERE well_name = '{well_name}'"
    well_id = pp.fetch_psql_command(well_id_query, connection)
    # PSQL statements
    conflict_statement = f"ON CONFLICT ({column_names[0]}) DO "
    conflict_statement += f"{on_conflict_do};"
    insert_statement = f"INSERT INTO {table_name}("
    for col_name in column_names:
        if col_name != column_names[-1]:
            insert_statement += f"{col_name}, "
        else:
            insert_statement += f"{col_name}) "
    # Values
    values_statement = f"VALUES ({well_id[1][0][0]}, "
    # If log is not []
    if log:
        for array in log:
            values_statement += f"array{array_check(array)}, "
        values_statement += f"'{log_name}') "           
    # If log [], fill the psql array with nulls
    else:
        for col_name in column_names:
            if (col_name != column_names[-1]) and (col_name != column_names[0]):
                values_statement += f"NULL, "
            elif col_name == column_names[-1]:
                values_statement += f"NULL)"

    # Execute insert statement
    insert_query = insert_statement + values_statement + conflict_statement
    pp.execute_psql_command(insert_query, connection)
    end = time.time()
    print(f"Done inserting well {well_name} '{log_name}' log. Execution time = {end - init}s\n")
    return (insert_query)

def insert_logs(table_name, wells_table, well_names, log_name, connection):
    """
    Feeds insert_log method with wells.
    
    For more details, see insert_log docstring.
    """
    init = time.time()
    for well_name in well_names:
        insert_log(table_name, wells_table, well_name, log_name, connection)
    end = time.time()
    return (f"Log '{log_name}' insertion completed in {end - init}s")

def check_null_wells(
    log_name,
    log_table, 
    wells_table, 
    connection, 
    name_column_name="well_name", 
    id_column_name="well_id"
):
    
    # Fetch Nulls from 
    check_nulls_query = f"SELECT {name_column_name} FROM {log_table} "
    check_nulls_query += f"INNER JOIN {wells_table} "
    check_nulls_query += f"USING({id_column_name}) WHERE {log_name} is NULL"
    null_wells_result = pp.fetch_psql_command(check_nulls_query, connection)
    # null & empty wells lists
    null_wells = []
    empty_wells = []
    for well in null_wells_result[1]:
        # list filling
        null_wells += [well[0]]
        well_check = wm.getLogNames(well[0])
        if well_check:
            print(f"\nWell '{well[0]}' logs:")
            print("****************************************")
            for log_names in well_check:
                 print(log_names)
        else:
            empty_wells += [well[0]]
    return(null_wells, empty_wells)