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
        table_name : psql table
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
         This method is.. a litle bit of hardcoded. I'm very sorry.
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