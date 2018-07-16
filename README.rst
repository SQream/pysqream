Python connector for SQream DB
----------
**Version:**  2.1.0

**Supported SQreamDB versions:** 2.11 onward

- For SQream versions 2.10 and below, please use version 2.0.2 of the connector, under "releases" tab.


Usage example:

.. code-block:: python

    ## Import and establish a connection  
    #  ---------------------------------   
    import SQream_python_connector

    # version information
    print SQream_python_connector.version_info()

    con = SQream_python_connector.Connector()
    # Connection parameters: IP, Port, Database, Username, Password, Clustered, Timeout(sec)
    sqream_connection_params = '127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, 30
    con.connect(*sqream_connection_params)


    ## Run queries using the API 
    #  -------------------------     
    # Create a table
    statement = 'create or replace table table_name (int_column int)'
    con.prepare_(statement)
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
    

Example of classic Get data loop:

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

Example of classic Set data loop, using network streaming (also called Network Insert):

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

    con.close()
    con.close_connection()
    
API Reference
-------------

All functions are accessed through the Connector class imported from SQream_Python_Connector.py:

**Initialization - Termination**

.. code-block:: python
    
    import SQream_python_connector
    con = SQream_python_connector.Connector()
    
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
