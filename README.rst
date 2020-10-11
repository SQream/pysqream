.. role:: bash(code)
   :language: bash
   
===================================
Python connector for SQream DB
===================================

* **Version:**  3.1.0

* **Supported SQream DB versions:** >= 2.13, 2019.2 or newer recommended

The Python connector for SQream DB is a Python DB API 2.0-compliant interface for developing Python applications with SQream DB.

The SQream Python connector provides an interface for creating and running Python applications that can connect to a SQream DB database. It provides a lighter-weight alternative to working through native C++ or Java bindings, including JDBC and ODBC drivers.

pysqream conforms to Python DB-API specifications `PEP-249 <https://www.python.org/dev/peps/pep-0249/>`_

``pysqream`` is native and pure Python, with minimal requirements. It can be installed with ``pip`` on any operating system, including Linux, Windows, and macOS.

For more information and a full API reference, see `SQream documentation's pysqream guide <https://sqream-docs.readthedocs.io/en/latest/guides/client_drivers/python/index.html>`_ .

Requirements
====================

* Python 3.6.5+, with 3.7+ highly recommended

* Cython (Optional, faster performance) - `pip3 install --upgrade cython`

Installing the Python connector
==================================

Prerequisites
----------------

1. Python
^^^^^^^^^^^^

The connector requires Python 3.6.5 or newer. To verify your version of Python:

.. code-block:: console

   $ python --version
   Python 3.7.3
   

Note: If both Python 2.x and 3.x are installed, you can run ``python3`` and ``pip3`` instead of ``python`` and ``pip`` respectively for the rest of this guide

2. PIP
^^^^^^^^^^^^
The Python connector is installed via ``pip``, the Python package manager and installer.

We recommend upgrading to the latest version of ``pip`` before installing. To verify that you are on the latest version, run the following command:

.. code-block:: console

   $ python -m pip install --upgrade pip
   Collecting pip
      Downloading https://files.pythonhosted.org/packages/00/b6/9cfa56b4081ad13874b0c6f96af8ce16cfbc1cb06bedf8e9164ce5551ec1/pip-19.3.1-py2.py3-none-any.whl (1.4MB)
        |████████████████████████████████| 1.4MB 1.6MB/s
   Installing collected packages: pip
     Found existing installation: pip 19.1.1
       Uninstalling pip-19.1.1:
         Successfully uninstalled pip-19.1.1
   Successfully installed pip-19.3.1

.. note:: 
   * On macOS, you may want to use virtualenv to install Python and the connector, to ensure compatibility with the built-in Python environment
   *  If you encounter an error including ``SSLError`` or ``WARNING: pip is configured with locations that require TLS/SSL, however the ssl module in Python is not available.`` - please be sure to reinstall Python with SSL enabled, or use virtualenv or Anaconda.

3. OpenSSL for Linux
^^^^^^^^^^^^^^^^^^^^^^^^^^

Some distributions of Python do not include OpenSSL. The Python connector relies on OpenSSL for secure connections to SQream DB.

* To install OpenSSL on RHEL/CentOS

   .. code-block:: console
   
      $ sudo yum install -y libffi-devel openssl-devel

* To install OpenSSL on Ubuntu

   .. code-block:: console
   
      $ sudo apt-get install libssl-dev libffi-dev -y

4. Cython (optional)
^^^^^^^^^^^^^^^^^^^^^^^^

Optional but recommended is Cython, which improves performance of Python applications.

   .. code-block:: console
   
      $ pip install cython

Install via pip
-----------------

The Python connector is available via `PyPi <https://pypi.org/project/pysqream/>`_.

Install the connector with ``pip``:

.. code-block:: console
   
   $ pip install pysqream

``pip`` will automatically installs all necessary libraries and modules.

Validate the installation
-----------------------------

Create a file called ``test.py`` (make sure to replace the parameters in the connection with the respective parameters for your SQream DB installation):

.. code-block:: python
   
   #!/usr/bin/env python

   import pysqream

   """
   Connection parameters include:
   * IP/Hostname
   * Port
   * database name
   * username
   * password 
   * Connect through load balancer, or direct to worker (Default: false - direct to worker)
   * use SSL connection (default: false)
   * Optional service queue (default: 'sqream')
   """

   # Create a connection object

   con = pysqream.connect(host='127.0.0.1', port=5000, database='master'
                      , username='sqream', password='sqream'
                      , clustered=False)

   # Create a new cursor
   cur = con.cursor()

   # Prepare and execute a query
   cur.execute('select show_version()')

   result = cur.fetchall() # `fetchall` gets the entire data set

   print (f"Version: {result[0][0]}")

   # This should print the SQream DB version. For example ``Version: v2020.1``.

   # Finally, close the connection

   con.close()

Run the test file to verify that you can connect to SQream DB:

.. code-block:: console
   
   $ python test.py
   Version: v2020.1

If all went well, you are now ready to build an application using the SQream DB Python connector!

If any connection error appears, verify that you have access to a running SQream DB and that the connection parameters are correct.

Logging
-------

To enable logging, pass a path to a log file in the connection string as follows:

.. code-block:: python
   
   con = pysqream.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False, log = '/path/to/logfile.xx')

Or pass True to save to `'/tmp/sqream_dbapi.log'`:

.. code-block:: python
   
   con = pysqream.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False, log =True)
    


Further examples
====================

Data load example
-------------------

This example loads 10,000 rows of dummy data to a SQream DB instance

.. code-block:: python
   
   import pysqream
   from datetime import date, datetime
   from time import time

   con = pysqream.connect(host='127.0.0.1', port=3108, database='master'
                      , username='rhendricks', password='Tr0ub4dor&3'
                      , clustered=True)
   
   # Create a table for loading
   create = 'create or replace table perf (b bool, t tinyint, sm smallint, i int, bi bigint, f real, d double, s varchar(12), ss nvarchar(20), dt date, dtt datetime)'
   con.execute(create)

   # After creating the table, we can load data into it with the INSERT command

   # Create dummy data which matches the table we created
   data = (False, 2, 12, 145, 84124234, 3.141, -4.3, "Marty McFly" , u"キウイは楽しい鳥です" , date(2019, 12, 17), datetime(1955, 11, 4, 1, 23, 0, 0))
   
   
   row_count = 10**4

   # Get a new cursor
   cur = con.cursor()
   insert = 'insert into perf values (?,?,?,?,?,?,?,?,?,?,?)'
   start = time()
   cur.executemany(insert, [data] * row_count)
   print (f"Total insert time for {row_count} rows: {time() - start} seconds")

   # Close this cursor
   cur.close()
   
   # Verify that the data was inserted correctly
   # Get a new cursor
   cur = con.cursor()
   cur.execute('select count(*) from perf')
   result = cur.fetchall() # `fetchall` collects the entire data set
   print (f"Count of inserted rows: {result[0][0]}")

   # When done, close the cursor
   cur.close()
   
   # Close the connection
   con.close()


Example of data retrieval methods
-----------------------------------------

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


Example of a SET data loop for data loading
-----------------------------------------------------

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
-----------------------------------------

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
-------------------------------------------------------------

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
       

