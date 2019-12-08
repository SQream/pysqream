.. role:: bash(code)
   :language: bash
   
===== 
Python connector for SQream DB
===== 

**Version:**  3.0.0

**Supported SQream DB versions:** 2.13 onward

Installing
----------

Install with `pip`, by running
:bash:`pip install pysqream`.

Usage example:
----------

.. code-block:: python

    ==========================================================================================  ===================================
    ## Import and establish a connection                                                      
    ==========================================================================================  ===================================
    #                                                                                           ---------------------------------  
    import pysqream                                                                           
    # Connection parameters: IP, Port, Database, Username, Password, Clustered, Use_Ssl
    sqream_connection_params = '127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False  
    con = pysqream.connect(*sqream_connection_params)                                               
    ==========================================================================================  ===================================

    ## Run queries using the API 
    #  -------------------------     
    # Create a table
    statement = 'create or replace table table_name (int_column int)'
    con.execute(statement) 

    # Insert sample data
    statement = 'insert into table_name(int_column) values (5), (6)'
    con.execute(statement)

    # Retreive data
    statement = 'select int_column from table_name'
    result_rows = con.execute(statement).fetchall()

    ## When done
    #  ----------------------------
    con.close()
    

Example of data retrieval:
----------

.. code-block:: python

    # Here we create the according table by
    # executing a "create or replace table table_name (int_column int, varchar_column varchar(10))" statement

    statement = 'select int_column, varchar_column from table_name'
    con.execute(statement)

    first_row = con.fetchone()
    second_row = con.fetchone()
    third_and_fourth_rows = con.fetchmany(2)
    remaining_rows = con.fetchall() 

    con.close()


Example of a SET data loop for data loading:
----------
.. code-block:: python

    # here we create the according table by executing a 
    # "create or replace table table_name (int_column int, varchar_column varchar(10))" statement
    
    data_rows = [(1, 's1'), (2, 's2'), (3, 's3')]
    
    # each interogation symbol represent a column to which the network insertion can push
    statement = 'insert into table_name(int_column, varchar_column) values(?, ?)' 
    con.executemany(statement, data_rows)
        
    con.close()
    

Example inserting from a csv to SQream
----------
.. code-block:: python

    def insert_from_csv(con,table_name,csv_filename, field_delimiter = ',', null_markers = []):
    
        # get info on the columns for the insert statement
    
        # you can get this info after preparing the insert, but we need to at
        # least know the number of columns to be able to construct the insert
        # statement
    
        column_info = con.execute(f"select * from {table_name} limit 0").description
    
        def parse_datetime(v):
            try:
                return datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try: 
                    return datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return datetime.datetime.strptime(row[i], '%Y-%m-%d')
    
        # insert the csv
        qstring = ','.join(['?'] * len(column_info))
        with open(csv_filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=field_delimiter)
        con.executemany(f"insert into {table_name} values ({qstring})", [row for row in csv_reader]):
                    
        
Example saving the results of a query to a csv file
----------
.. code-block:: python

    def save_query(con, query, csv_filename, field_delimiter, null_marker):
        
        column_info = con.execute(query).description
        with open(csv_filename, 'x', newline='') as csvfile:
            wr = csv.writer(csvfile, delimiter=field_delimiter,quoting=csv.QUOTE_MINIMAL)
            
            for result_row in con:
                    csv_row = []
                    wr.writerow(result_row)
       
API Reference
-------------

**Initialization - Termination**

.. code-block:: python
    
    import pysqream
    
    # arg types are: string, integer, string, string, string, boolean, boolean
    con = pysqream.connect(ip, port, database, username, password, clustered, timeout) 
     
    # closes the connection completely, destructing the socket, a call to "connect(..)" needs to be done do continue
    con.close()
   

**High level protocol functions**

.. code-block:: python

    con.execute(statement) #string of the query to run
    con.executemany(insert_statement, rows) # parametered insert query
    con.fetchall()   # Get all results of select query
    con.fetchmany(num_rows) # Get num_rows results of select query
    con.fetchone()          # Get one result of select query
    
**Unsupported**

``execute() with parameters``

``setinputsizes()``

``setoutputsize()``
