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