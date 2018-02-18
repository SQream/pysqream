Python2.7/3.x Driver for SQream DB
----------

Usage example:

.. code-block:: python

    ## Import and establish a connection  
    #  ---------------------------------   
    import PySqreamConn

    # version information
    print PySqreamConn.version_info()

    con = PySqreamConn.Connector()
    # Connection parameters: IP, Port, Database, Username, Password, Clustered, Timeout(sec)
    sqream_connection_params = '127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, 30
    con.connect(*sqream_connection_params)


    ## Run queries using the API 
    #  -------------------------     
    # Create a table
    statement = 'create or replace table table_name (int_column int)'
    con.prepare_statement(statement)
    con.execute()
    con.close_statement()

    # Insert sample data
    statement = 'insert into table_name(int_column) values (5), (6)'
    con.prepare_statement(statement)
    con.execute()
    con.close_statement()

    # Retreive data
    statement = 'select int_column from table_name'
    con.prepare_statement(statement)
    con.execute()
    con.next_row()

    # Pull out the actual data
    first_row_int = con.get_int(1)
    con.next_row()
    second_row_int = con.get_int(1)
    con.next_row()
    print (first_row_int, second_row_int)
    con.close_statement()


    ## After running all statements
    #  ----------------------------
    con.close()
