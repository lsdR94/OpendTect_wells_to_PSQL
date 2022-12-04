from email import message
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

def fetch_opendtect_well_log(well_name, log_name):
    """
    Fetches a well log.
    
    ARGUMENTS
    ---------
        well_name : str
            Well's database name.
            
        log_name : str
            Log name as reported by wellman.
    
    RETURN
    ------
        Tuple
            Two list [arrays for psycopg2] with depths (MD) and log 
            values.
        
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
    
def insert_log_as_arrays_query(
    well_name, 
    log_name,
    table_name, 
    wells_table, 
    connection,
    on_conflict_do="NOTHING"
):
    """
    Creates a query to insert a single log into a table as a PSQL array.
    
    If there is no log, null values will be inserted in
    the table.
    
    ARGUMENTS
    ---------
        well_name : str
            Well's database name.
        
        log_name : str
            Log name as reported by wellman.
            
        table_name : str
            PSQL table target.
        
        wells_table : str
            PSQL wells table, where the basic well info is stored.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
            
        on_conflict_do : str
            PSQL statements for data updates. (DO) NOTHING by default.
    
    RETURNS
    -------
        str
            Log insertion Query.
            
    FOOT NOTES
    ----------
         The construction of the PSQL can be improved.
            
    """
    def array_check(array): return(
        str(
            [None if sample == 1e+30 else sample for sample in array]
        ).replace("None", "Null")
    )
    # column names & fetch log
    column_names = pp.fetch_column_names(table_name, connection)
    try:
        log = fetch_opendtect_well_log(well_name, log_name)
    except:
        print(f"Can not find well's {well_name} '{log_name}' log in Opendtect internal database")
        
    # PSQL statements
    insert_statement = f"INSERT INTO {table_name}({pp.string_replacement(column_names)})"
    values_statement = f"VALUES ('{well_name}', "
    conflict_statement = f"ON CONFLICT ({column_names[0]}) DO "
    conflict_statement += f"{on_conflict_do};"
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
    # well insertion message
    return (insert_query)

def insert_logs(
    well_names, 
    log_name,
    table_name, 
    wells_table, 
    connection,
    mode="array",
    on_conflict_do="NOTHING"

):
    """
    Inserts logs into PSQL tables using loops compounded by well names.
      
    For more details, see insert_log_as_arrays_query and insert_log_by_samples docstring.
    
    RETURN
    ------
        str
            Finalization of the insertion process.
    
    NOTES
    -----
        insert_log_by_samples does not exist (yet)
    """
    init = time.time()
    print(f"Proccessing insertion query. Concept: well log '{log_name}' insertion in {mode} mode")
    if mode == "array":
        for well_name in well_names:
            print(f"\nWell {well_name}")
            insert_query = insert_log_as_arrays_query(well_name, log_name, table_name, wells_table, connection, on_conflict_do)
            pp.execute_psql_command(insert_query, connection)
    if mode == "sample":
         for well_name in well_names:
            print(f"\nWell {well_name}")
            insert_query = insert_log_by_samples(well_name, log_name, table_name, wells_table, connection, on_conflict_do)
            pp.execute_psql_command(insert_query, connection)
    end = time.time()
    return (f"\nLog '{log_name}' insertion completed in {end - init}s")

def check_null_wells(
    log_name,
    log_table, 
    wells_table, 
    connection, 
    name_column_name="well_name", 
):
    """
    Identifies null and empty wells.
    
    By default, when insert_logs doesn't find a specific log related to a
    specific well, it insert a null value to the target table. Null 
    wells are those that contains logs but not the chosen one. On the 
    contrary, empty wells are those with no log content.
    
    ARGUMENTS
    ---------
        log_name : str
            Log name as reported by wellman.
        
        log_table : str
            PSQL log table target.
        
        wells_table : str
            PSQL wells table, where the basic well info
            is stored.
        
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
        
        name_column_name : str
            Well name column in PSQL wells table. Default: well_name.
            
        id_column_name : str
            Well id column's name. Default: well_id.
    
    RETURNS
    -------
        tuple
            Lists of null and empty wells.
            
    FOOT NOTES
    ----------
         The construction of the PSQL can be improved.
    
    """
    
    # Fetch Nulls from 
    check_nulls_query = f"SELECT {name_column_name} FROM {log_table} "
    check_nulls_query += f"INNER JOIN {wells_table} "
    check_nulls_query += f"USING({name_column_name}) WHERE {log_name} is NULL"
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

def insert_markers_query(well_name, table_name, on_conflict_do="NOTHING"):
    """
    Creates a query to insert  markers into a table. Done by well.
    
    If there is no marker, the return will be an invalid query.
    
    ARGUMENTS
    ---------
        well_name : str
            Well's database name.
            
        table_name : str
            PSQL table target.
            
        on_conflict_do : str
            PSQL statements for data updates. (DO) NOTHING by default.
    
    RETURNS
    -------
        str
            Marker's insertion Query.
    """
    od_markers = wm.getMarkers(well_name)
    #Recover table columns (same as df)
    table_columns = pp.string_replacement(str(od_markers[0]))
    #PSQL statements
    if od_markers[0]:
        insert_statement = f"INSERT INTO {table_name}(well_name,{table_columns}) "
        values_statement = f"VALUES('{well_name}', "
        conflict_statement = f"ON CONFLICT (well_name) DO "
        conflict_statement += f"{on_conflict_do};"
        for index, depth in enumerate(od_markers[1]):
            if index != len(od_markers[1]) - 1:
                values_statement += f"{depth},"
            if index == len(od_markers[1]) - 1:
                values_statement += f"{depth}) "
        insert_query = insert_statement + values_statement + conflict_statement
    else:
        insert_statement = f"INSERT INTO {table_name}(well_name) "
        values_statement = f"VALUES('{well_name}')"
        conflict_statement = f"ON CONFLICT (well_name) DO "
        conflict_statement += f"{on_conflict_do};"
        insert_query = insert_statement + values_statement + conflict_statement
    return insert_query



# DEPRECATED FUNCTIONS
def insert_wells(
    name_column_name,
    table_name,
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
    wells_table_creation function
    
    ARGUMENTS
    ---------        
        well_name_column : str
            Well name column in PSQL tables. Default: well_name.
            
        table_name : str
            PSQL table target.
            
        connection : psycopg2.extensions.connection
            Parameters to create a connection between end user and PSQL 
            server.
            
        values_statement : list (optional)
            List of PSQL statements for a more flexible value insertion.
       
    RETURN
    ------
           str
               Finalization message + execution time.
     
     FOOT NOTES
     ----------
         This function is.. a bit of hardcoded. I'm very sorry.
         values_statement argument is provided in order to allow
         a more flexible insertion if you are using other columns.
    NO MORE
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
        insert_statement += values_statement + conflict_statement
        pp.execute_psql_command(insert_statement, connection)
        end2 = time.time()
        print(
            f"Well {well_dict['Name']} insertion completed in {end2 - init2}s"
        )
    end = time.time()
    return f"Wells insertion completed in {end - init}s"



### To code
# def insert_log(
#     well_name, 
#     log_name,
#     table_name, 
#     connection,
#     on_conflict_do="NOTHING"
# ):
#     """
#     Insert a single log (of given well) into a table.
    
#     If there is no log, null values will be inserted in
#     the table.
    
#     ARGUMENTS
#     ---------
#         well_name : str
#             Well's database name.
        
#         log_name : str
#             Log name as reported by wellman.
            
#         table_name : str
#             PSQL table target.
        
#         connection : psycopg2.extensions.connection
#             Parameters to create a connection between end user and PSQL 
#             server.
            
#         on_conflict_do : str
#             PSQL statements for data updates. (DO) NOTHING by default.
    
#     RETURNS
#     -------
#         str
#             Prints the finalization of the inserting process. Includes
#             execution time.
            
#     FOOT NOTES
#     ----------
#          The construction of the PSQL can be improved.
            
#     """
#     init = time.time()
#     # column names & fetch log
#     table_columns = pp.fetch_column_names(table_name, connection)
#     log = fetch_opendtect_well_log(well_name, log_name)
#     # PSQL statements
#     insert_statement = f"INSERT INTO {table_name}({pp.string_replacement(table_columns)})"
#     values_statement = f"VALUES('{well_name}', "
#     conflict_statement = f"ON CONFLICT (well_name) DO "
#     conflict_statement += f"{on_conflict_do};"
#     for sample in range(log[0] + 1):
#         if log[0][sample] != log[0][-1]:
#             values_statement += f"{log[0][sample]}, {log[1][sample]}, "
#         if log[0][sample] == log[0][-1]:
#             values_statement += f"{log[0][sample]}, {log[1][sample]})"
#     insertion_query = insert_statement + values_statement + conflict_statement
#     return insertion_query