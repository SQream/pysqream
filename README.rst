===== 
SQream Python package
===== 

Modules
-----

- DB-API compliant connector

Requirements
-----

**All modules:**
    
- Python 3.6+

**DB-API:**

- Cython (Optional, faster performance) - :code:`pip3 install cython`


Usage
-----

**DB-API:**

.. code-block:: python

    from time import time 
    from datetime import date, datetime
    import sys, os

    # If using dbapi.py directly (not via pip), add to path first
    # sys.path.append('path/to/pysqream') 
    from pysqream import dbapi  

    # Sample data to insert into SQream
    data = (False,2, 22, 222, 2222, 3.0, 4.0, "yada" , "yada" , date(2016, 12, 23), datetime(2016, 12, 23, 16, 56,45, 000))
    amount = 10**6

    # Connect and create table
    # host, port, database, username, password, clustered, use_ssl, service, reconnect_attempts, reconnect_interval
    con = dbapi.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, False, 'sqream', 3, 10)  
    create = 'create or replace table perf (b bool, t tinyint, sm smallint, i int, bi bigint, f real, d double, s varchar(10),  ss nvarchar(10), dt date, dtt datetime)'
    con.execute(create) 
        
    #Insert data 
    print ("Starting insert")
    insert = 'insert into perf values (?,?,?,?,?,?,?,?,?,?,?)'
    start = time()
    con.executemany(insert, [data for _ in range(amount)]) 
    print (f"Total insert time for {amount} rows: {time() - start}") 

    # Get data back if desired
    con.execute('select count(*) from perf')
    result = con.fetchall()
    print (f"Count of inserted rows: {result[0][0]}")
