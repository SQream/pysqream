.. role:: bash(code)
   :language: bash
   
===== 
Python connector for SQream DB
===== 

* **Version:**  3.0.0

* **Supported SQream DB versions:** >= 2.13, 2019.2 recommended

The Python connector for SQream DB is a Python DB API 2.0-compliant interface for developing Python applications with SQream DB.
The connector allows executing statements, running queries, and inserting data.

Requirements
------------

Python 3.7+
Cython (Optional, faster performance) - `pip3 install cython`

Installing
----------

Install with `pip`, by running:

:bash:`pip install pysqream`

Usage example:
----------

This example loads 1 million rows of dummy data to a SQream DB instance


.. code-block:: python
              
    from time import time 
    from datetime import date, datetime
     
    import pysqream  


    # Connect and create table. Connection params are:
    # IP/Hostname, port, database name, username, password, connect to a cluster / single host, use SSL connection
    con = pysqream.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False) 
    
    # Immediately after connection, we create the dummy table
    create = 'create or replace table perf (b bool, t tinyint, sm smallint, i int, bi bigint, f real, d double, s varchar(10), ss nvarchar(10), dt date, dtt datetime)'
    con.execute(create) 
        
    # Insert data 
    print ("Starting insert")
    # Create dummy data which matches the table we created
    data = (False, 2, 12, 145, 84124234, 3.141, -4.3, "Varchar text" , "International text" , date(2019, 12, 17), datetime(1955, 11, 04, 01, 23, 00, 000))
    amount = 10**6

    insert = 'insert into perf values (?,?,?,?,?,?,?,?,?,?,?)'
    start = time()
    con.executemany(insert, [data] * amount) 
    print (f"Total insert time for {amount} rows: {time() - start}") 

    # Verify that the data was inserted correctly
    con.execute('select count(*) from perf')
    result = con.fetchall() # `fetchall` collects the entire data set
    print (f"Count of inserted rows: {result[0][0]}")

    # When done, close the connection
    con.close()
    

Example of data retrieval methods:
----------

.. code-block:: python

    # Assume a table structure:
    # "CREATE TABLE table_name (int_column int, varchar_column varchar(10))"

    # The select statement:
    statement = 'SELECT int_column, varchar_column FROM table_name'
    con.execute(statement)

    first_row = con.fetchone() # Fetch one row at a time (first row)
    second_row = con.fetchone() # Fetch one row at a time (second row)
    
    # executing `fetchone` twice is equivalent to this form:
    third_and_fourth_rows = con.fetchmany(2)
    
    # To get all rows at once, use `fetchall`
    remaining_rows = con.fetchall() 

    con.close()


Example of a SET data loop for data loading:
----------
.. code-block:: python

    # Assume a table structure:
    # "CREATE TABLE table_name (int_column int, varchar_column varchar(10))"
    
    # Each `?` placeholder represents a column value that will be inserted
    statement = 'INSERT INTO table_name(int_column, varchar_column) VALUES(?, ?)'
    
    # To insert data, we execute the statement with `executemany`, and pass an array of values alongside it
    data_rows = [(1, 's1'), (2, 's2'), (3, 's3')] # Sample data
    con.executemany(statement, data_rows)
    
    con.close()
    

Example inserting data from a CSV
----------
.. code-block:: python

    def insert_from_csv(con, table_name, csv_filename, field_delimiter = ',', null_markers = []):
    
        # We will first ask SQream DB for some table information.
        # This is important for understanding the number of columns, and will help
        # to create an INSERT statement
   
        column_info = con.execute(f"select * from {table_name} limit 0").description

        
        def parse_datetime(v):
            try:
                return datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try: 
                    return datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return datetime.datetime.strptime(row[i], '%Y-%m-%d')
    
        # Create enough placeholders (`?`) for the INSERT query string
        qstring = ','.join(['?'] * len(column_info))
        insert_statement = f"insert into {table_name} values ({qstring})"
        
        # Open the CSV file
        with open(csv_filename, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=field_delimiter)
        
        # Execute the INSERT statement with the CSV data
        con.executemany(insert_statement, [row for row in csv_reader]):
                    
        
Example saving the results of a query to a csv file
----------
.. code-block:: python

    def save_query(con, query, csv_filename, field_delimiter, null_marker):
        # The query string has been passed from the outside, so we will now execute it:
        column_info = con.execute(query).description
        
        # With the query information, we will write a new CSV file
        with open(csv_filename, 'x', newline='') as csvfile:
            wr = csv.writer(csvfile, delimiter=field_delimiter,quoting=csv.QUOTE_MINIMAL)
            # For each result row in a query, write the data out
            for result_row in con:
                    csv_row = []
                    wr.writerow(result_row)
       
API Reference
-------------

**Initialization - Termination**

.. code-block:: python
    
    import pysqream
    
    # Argument types are: string, integer, string, string, string, boolean, boolean
    con = pysqream.connect(ip, port, database, username, password, clustered, timeout) 
     
    # closes the connection completely, destructing the socket.
    con.close()
    # The connection can't be reused, until "connect(...)" is called
   

**High level protocol functions**

.. code-block:: python

    con.execute(statement) # Accepts a query string to execute
    con.executemany(insert_statement, rows) # Used exclusively for INSERT statements
    con.fetchall()          # Get all results from a SELECT query
    con.fetchmany(num_rows) # Get num_rows results from a SELECT query
    con.fetchone()          # Get one result from a SELECT query


**Unsupported functionality**

``execute()`` with parameters

``setinputsizes()``

``setoutputsize()``
