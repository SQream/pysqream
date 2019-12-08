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
              
    from time import time 
    from datetime import date, datetime
     
    import pysqream  


    # Sample data to insert into SQream
    data = (False,2, 22, 222, 2222, 3.0, 4.0, "yada" , "yada" , date(2016, 12, 23), datetime(2016, 12, 23, 16, 56,45, 000))
    amount = 10**6

    # Connect and create table. Connection params are:
    # ip, port, database, username, password, clustered, use_ssl
    con = pysqream.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False) 
    create = 'create or replace table perf (b bool, t tinyint, sm smallint, i int, bi bigint, f real, d double, s varchar(10), ss nvarchar(10), dt date, dtt datetime)'
    con.execute(create) 
        
    #Insert data 
    print ("Starting insert")
    insert = 'insert into perf values (?,?,?,?,?,?,?,?,?,?,?)'
    start = time()
    con.executemany(insert, [data] * amount) 
    print (f"Total insert time for {amount} rows: {time() - start}") 

    # Get data back if desired
    con.execute('select count(*) from perf')
    result = con.fetchall()
    print (f"Count of inserted rows: {result[0][0]}")

    # When done
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

``execute()`` with parameters

``setinputsizes()``

``setoutputsize()``
