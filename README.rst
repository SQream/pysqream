.. role:: bash(code)
   :language: bash
   
===== 
Python connector for SQream DB
===== 

**Version:**  2.1.4

**Supported SQream DB versions:** 1.13 onward

Installing
----------

Install with `pip`, by running
:bash:`pip install pysqream`.

Usage example:
----------

.. code-block:: python

    ## Import and establish a connection  
    #  ---------------------------------   
    import pysqream

    # version information
    print pysqream.version_info()

    con = pysqream.Connector()
    # Connection parameters: IP, Port, Database, Username, Password, Clustered, Timeout(sec)
    sqream_connection_params = '127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, 30
    con.connect(*sqream_connection_params)


    ## Run queries using the API 
    #  -------------------------     
    # Create a table
    statement = 'create or replace table table_name (int_column int)'
    con.prepare(statement)
    con.execute()
    con.close()

    # Insert sample data
    statement = 'insert into table_name(int_column) values (5), (6)'
    con.prepare(statement)
    con.execute()
    con.close()

    # Retreive data
    statement = 'select int_column from table_name'
    con.prepare(statement)
    con.execute()
    con.next_row()

    # Pull out the actual data
    first_row_int = con.get_int(1)
    con.next_row()
    second_row_int = con.get_int(1)
    con.next_row()
    print (first_row_int, second_row_int)
    con.close()


    ## After running all statements
    #  ----------------------------
    con.close_connection()
    

Example of GET data loop:
----------

.. code-block:: python

    # Here we create the according table by
    # executing a "create or replace table table_name (int_column int, varchar_column varchar(10))" statement

    row1 = []
    row2 = []

    statement = 'select int_column, varchar_column from table_name'
    con.prepare(statement)
    con.execute()

    while con.next_row():
        row1.append(con.get_int(1))
        row2.append(con.get_string(2))

    con.close()
    con.close_connection()

Example of a SET data loop for data loading:
----------
.. code-block:: python

    # here we create the according table by executing a 
    # "create or replace table table_name (int_column int, varchar_column varchar(10))" statement
    
    row1 = [1,2,3]
    row2 = ["s1","s2","s3"]
    length_of_arrays = 3
    
    # each interogation symbol represent a column to which the network insertion can push
    statement = 'insert into table_name(int_column, varchar_column) values(?, ?)' 
    con.prepare(statement)
    con.execute()

    for idx in range(length_of_arrays):
        con.set_int(1, row1[idx])      # we put a value at column 1 of the table
        con.set_varchar(2, row2[idx])  # we put a value at column 2 of the table
        con.next_row()
        
    con.close()
    con.close_connection()
    
Example inserting from a csv to SQream
----------
.. code-block:: python

    def insert_from_csv(con,table_name,csv_filename, field_delimiter = ',', null_markers = []):
    
        # get info on the columns for the insert statement
    
        # you can get this info after preparing the insert, but we need to at
        # least know the number of columns to be able to construct the insert
        # statement
    
        with pysqream.sqream_run(con,f"select * from {table_name} limit 0") as con:
            column_types = con.get_column_types()

    
        def parse_datetime(v):
            try:
                return datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try: 
                    return datetime.datetime.strptime(row[i], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return datetime.datetime.strptime(row[i], '%Y-%m-%d')
    
        # insert the csv
        qstring = ",".join(['?'] * len(column_types))
        with pysqream.sqream_run(con, f"insert into {table_name} values ({qstring})") as con:
            with open(csv_filename, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=field_delimiter)
                for row in csv_reader:
                    for i,(t,v) in enumerate(zip(column_types, row)):
                        ii = i + 1
                        if row[i] in null_markers:
                            con.set_null(ii)
                        elif t.tid == 'Tinyint':
                            con.set_ubyte(ii, int(row[i]))
                        elif t.tid == "Smallint":
                            con.set_short(ii, int(row[i]))
                        elif t.tid == "Int":
                            con.set_int(ii, int(row[i]))
                        elif t.tid == "Bigint":
                            con.set_long(ii, int(row[i]))
                        elif t.tid == "Real":
                            con.set_float(ii, float(row[i]))
                        elif t.tid == "Float":
                            con.set_double(ii, float(row[i]))
                        elif t.tid == "Date":
                            dt = datetime.datetime.strptime(row[i], "%Y-%m-%d")
                            dt = datetime.date(dt.year, dt.month, dt.day)
                            con.set_date(ii, dt)
                        elif t.tid == "DateTime":
                            dt = parse_datetime(row[i])
                            con.set_datetime(ii, dt)
                        elif t.tid == "Varchar":
                            con.set_varchar(ii, row[i])
                        elif t.tid == "NVarchar":
                            con.set_nvarchar(ii, row[i])
                    con.next_row()
        
Example saving the results of a query to a csv file
----------
.. code-block:: python

    def save_query(con, query, csv_filename, field_delimiter, null_marker):
    
        with pysqream.sqream_run(con, query) as con:
            column_types = con.get_column_types()
            with open(csv_filename, 'x', newline='') as csvfile:
                wr = csv.writer(csvfile, delimiter=field_delimiter,quoting=csv.QUOTE_MINIMAL)
                while con.next_row():
                    csv_row = []
                    for i,t in enumerate(column_types):
                        ii = i + 1
                        if con.is_null(ii):
                            csv_row.append(null_marker)
                        elif t.tid == 'Tinyint':
                            csv_row.append(con.get_ubyte(ii))
                        elif t.tid == "Smallint":
                            csv_row.append(con.get_short(ii))
                        elif t.tid == "Int":
                            csv_row.append(con.get_int(ii))
                        elif t.tid == "Bigint":
                            csv_row.append(con.get_long(ii))
                        elif t.tid == "Real":
                            csv_row.append(con.get_float(ii))
                        elif t.tid == "Float":
                            csv_row.append(con.get_double(ii))
                        elif t.tid == "Date":
                            csv_row.append(con.get_date(ii))
                        elif t.tid == "DateTime":
                            csv_row.append(con.get_datetime(ii))
                        elif t.tid == "Varchar":
                            csv_row.append(con.get_varchar(ii))
                        elif t.tid == "NVarchar":
                            csv_row.append(con.get_nvarchar(ii))
                    wr.writerow(csv_row)
       
API Reference
-------------

All functions are accessed through the Connector class imported from pysqream.py:

**Initialization - Termination**

.. code-block:: python
    
    import pysqream
    con = pysqream.Connector()
    
    # arg types are: string, integer, string, string, string, boolean, integer
    con.connect(ip, port, database, username, password, clustered, timeout) 
    
    # closes the statement (to do after execute + necessary fetch/put to close the statement and be 
    # able to open another one through prepare())
    con.close() 
    
    # closes the connection completely, destructing the socket, a call to "connect(..)" needs to be done do continue
    con.close_connection() 
   

**High level protocol functions**

.. code-block:: python

    con.prepare(statement) #string of the query to run
    con.execute()

    # if the statement is an insert it produces a put and for select it produces a fetch, rows are 
    # incremented through that function (see Usage example)
    con.next_row() 

**Get column based data**

By column id or column name (integer or string)

.. code-block:: python
    
    is_null(col_id_or_col_name)
    get_bool(col_id_or_col_name)
    get_ubyte(col_id_or_col_name)
    get_short(col_id_or_col_name)
    get_int(col_id_or_col_name)
    get_long(col_id_or_col_name)
    get_float(col_id_or_col_name)
    get_double(col_id_or_col_name)
    get_date(col_id_or_col_name)
    get_datetime(col_id_or_col_name)
    get_varchar(col_id_or_col_name)
    get_nvarchar(col_id_or_col_name)


**Set column based data**

By column id

.. code-block:: python

    set_null(col)
    set_bool(col, val)
    set_ubyte(col, val)
    set_short(col, val)
    set_int(col, val)
    set_long(col, val)
    set_float(col, val)
    set_double(col, val)
    set_date(col, val)
    set_datetime(col, val)
    set_varchar(col, val)
    set_nvarchar(col, val)
