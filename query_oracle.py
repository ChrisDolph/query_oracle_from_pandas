import cx_Oracle as cxo
import pandas as pd

def load_query(sql_file_path_string,username, password):

    """load_query accepts a file path string argument and returns a pandas dataframe object. This path should be to a text file or .sql file containing a sql query.
    an Oracle database instance will be created and the query will be executed. The results are returned as a dataframe.
    The Oracle connection and text file are immediatly closed after the execution of the query. ***Warning: Does not currently handle for inline notes in sql code.***"""

    try:
        sql_file = open(sql_file_path_string,'r')
        sql_query = sql_file.read().replace('\n',' ')
        dsn_tns = cxo.makedsn(YOUR_HOST,YOUR_PORT,service_name = YOUR_SERVICE_NAME)
        conn = cxo.connect(user = username, password = password,dsn = dsn_tns)
        query_result = pd.read_sql(sql_query,conn)
        return query_result

    except:
        print('An error occured with the load query function')

    finally:
        sql_file.close()
        conn.close()

def type_query(sql_query,username, password):

    """type_query accepts a valid sql query as a string argument and returns a pandas dataframe object.
    An Oracle instance will be created and the query will be executed and the results returned as a dataframe.
    The Oracle connection is immediatly closed after the execution of the query."""

    try:
        dsn_tns = cxo.makedsn(YOUR_HOST,YOUR_PORT,service_name = YOUR_SERVICE_NAME)
        conn = cxo.connect(user = username, password = password,dsn = dsn_tns)
        query_result = pd.read_sql(sql_query,conn)
        return query_result

    except:
        print('An error occured with the type query function')

    finally:
        conn.close()

