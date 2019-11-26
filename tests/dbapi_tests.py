import os, sys
import numpy as np
from time import sleep
from datetime import datetime, date
from subprocess import Popen, PIPE
# from random import random, randint
from numpy.random import rand, randint, uniform
from itertools import product

sys.path.append(os.getcwd().rsplit('/', 1)[0] + '/pysqream')
import dbapi 

POP_SQREAM_AT_START = False   # True
CLOSE_WHEN_DONE = False 

## Setup server and storage params
up_by = 2  #1 when in tests directory
sqream_dir = os.getcwd().rsplit('/', up_by)[0] + '/'
sqream_path = sqream_dir + '/build/sqreamd'
home = '/' + '/'.join(Popen(b'pwd', stdout=PIPE).communicate()[0].decode('utf8').split('/')[1:3]) +'/'
cluster_dir = home + 'tpch_100gb'
other_params = ''


## Tests setup
#  -----------

varchar_length = 10
nvarchar_length = 10
errors = []

def generate_varchar(length):
    return ''.join(chr(num) for num in randint(1,128, length))


col_types =     {'bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'date', 'datetime', 'varchar({})'.format(varchar_length), 'nvarchar({})'.format(varchar_length)}  
# col_types =     f'nvarchar({varchar_length})', 
pos_test_vals = {'bool':    (0, 1, True, False),   
                 'tinyint':    (randint(0, 255), randint(0, 255), 0, 255),
                 'smallint':    (randint(-32768, 32767), 0, -32768, 32767),
                 'int':      (randint(-2147483648, 2147483647), 0, -2147483648, 2147483647),
                 'bigint':   (randint(-9223372036854775808, 9223372036854775807), 0, -9223372036854775808, 9223372036854775807),
                 'real':     (float('inf'), float('-inf'), float('+0'), float('-0'), round(uniform(1e-6, 1e6), 5)),   # float('nan')    
                 'double':     (float('inf'), float('-inf'), float('+0'), float('-0'), uniform(1e-6, 1e6)),  # float('nan') 
                 'date':     (date(1998, 9, 24), date(1998, 9, 24), date(1998, 9, 24), date(1998, 9, 24)),
                 'datetime': (datetime(1998, 9, 24, 17, 25, 46), datetime(1998, 9, 24, 17, 25, 46), datetime(1998, 9, 24, 17, 25, 46), datetime(1998, 9, 24, 17, 25, 46)),
                 'varchar':  (generate_varchar(varchar_length), generate_varchar(varchar_length), generate_varchar(varchar_length), generate_varchar(varchar_length)),
                 'nvarchar': ('א', 'א', 'א', 'א')
                  }

neg_test_vals =  {'bool':    (2, 3.6, 'ssup', None),   
                 'tinyint':  (258, 3.6, 'ssup', False, None),
                 'smallint': (40000, 3.6, 'ssup', False, None),
                 'int':      (9999999999, 3.6, 'ssup', False, None),
                 'bigint':   ( 3.6, 'ssup', False, None),
                 'real':     ('ssup', False, None),
                 'double':   ('ssup', False, None),
                 'date':     (5, (-8, 9, 1), (2012, 15, 6), (2012, 9, 45), 'ssup', False, None),
                 'datetime': (5, (-8, 9, 1, 0, 0, 0), (2012, 15, 6, 0, 0, 0), (2012, 9, 45, 0, 0, 0), (2012, 9, 14, 26, 0, 0), (2012, 9, 14, 13, 89, 0), 'ssup', False ),
                 'varchar':  (5, (1, 2), True, None),
                 'nvarchar': (5, (1, 2), True, None)
                  }        

query_types =   'prepare', 'execute', 'close', 'fetch', 'next_row'  


## Tests
#  -----

TESTS_PASS = True

'''
rows = (True, 1, 11, 111, 1111, 1.0, 2.0, "shoko", "shoko2"), (False,2, 22, 222, 2222, 3.0, 4.0, "yada", "yada"),
con.execute('create or replace table fud (b bool, t tinyint, sm smallint, i int, bi bigint, f real, d double, s varchar(10), ss nvarchar(10))') 
con.executemany('insert into fud values (?,?,?,?,?,?,?,?,?)', rows) 
con.execute('select * from fud') 
res = con.fetchall()
'''

# Start connection
con = dbapi.connect('127.0.0.1', 5000,'master', 'sqream', 'sqream', False, False) 


def positive_tests():

    global TESTS_PASS

    print ('\nPositive Set/Get Tests')
    print   ('----------------------')

    for col_type in col_types:

        con.execute(f'create or replace table test (t_{col_type.split("(")[0]} {col_type})')

        col_type = col_type.split('(')[0]
        # print (col_type)  # dbg
        for val in pos_test_vals[col_type]:
            print ('Trying get() on column type {} with value {}'.format(col_type, val))
            
            # Clean, set and get a value
            con.execute('truncate table test')
            rows = [(val,)]
            con.executemany('insert into test values (?)', rows)
            con.execute('select * from test')
            res = con.fetchall()[0][0]

            # Compare
            if (val != res) and (type(val) is str and val.rstrip() != res):
                if col_type not in ('date', 'datetime', 'real'): 
                    print ("bobo", repr(val), type(val), repr(res), type(res))
                    print (val.rstrip() == res)
                    error = f"TEST ERROR: No match with get_{col_type}() by index with values: {repr(val)}, {repr(res)}"
                    print (error)
                    TESTS_PASS = False
                elif col_type == 'real' and round(val) != round(res):
                    # Single precision packing and unpacking is inaccurate:
                    # unpack('f', pack('f', 255759.83335))[0] == 255759.828125
                    error = "TEST ERROR: No match with get_{}() by index with values: {}, {}".format(col_type, repr(val), repr(res))
                    print (error)
                    TESTS_PASS = False
                elif col_type in ('date', 'datetime') and datetime(*val) != res and date(*val) != res: 
                    error = "TEST ERROR: No match with get_{}() by index with values: {}, {}".format(col_type, val, res)
                    print (error)
                    TESTS_PASS = False
          

    print ('\nPositive Null Tests')
    print ('-------------------')

    for col_type in col_types:

        print ("null test on type: ", col_type)
        # Create appropriate table
        con.execute(f'create or replace table test (t_{col_type.split("(")[0]} {col_type})')
        
        col_type = col_type.split('(')[0]
        
        # Set null, get it back       
        con.executemany('insert into test values (?)', [(None,)])
        con.execute('select * from test')
        res = con.fetchall()[0][0]

        # Compare
        if res not in (None, ):
            print ("TEST ERROR: Error setting null on column type: ", col_type)
            print ("Got: ", res, type(res))
            TESTS_PASS = False



def negative_tests():
    ''' Negative Set/Get Tests '''

    global TESTS_PASS

    print ("\nNegative Tests")
    print ('--------------') 
    
    # '''
    # Bad value type for packing test
    for col_type in col_types:

        con.execute(f'create or replace table test (t_{col_type.split("(")[0]} {col_type})')

        col_type = col_type.split('(')[0]
        for val in neg_test_vals[col_type]:
            print (f'Trying to insert to column type {col_type} with value {val}')
            
            # Clean, set and get a value
            con.execute('truncate table test')
            rows = [(val,)]
            try:
                con.executemany('insert into test values (?)', rows)
            except dbapi.ProgrammingError as e:
                if "Trying to insert unsuitable types" not in repr(e) and "Error packing columns. Check that all types" not in repr(e):
                    raise Exception(f"Didn't raise correct error on bad type insert to column of type {col_type} with value {val}")
                    TESTS_PASS = False
    # '''

    # Inconsistent sizes test
    con.execute(f'create or replace table test (t_int int, q_int int)')
    try:
        con.executemany('insert into test values (?, ?)', [(5,), (6, 9), (7, 8)])
    except dbapi.ProgrammingError as e:
        if "Incosistent data sequences passed" not in repr(e):
            raise Exception(f"Didn't raise correct error on using inconsistent lengths on insert")
            TESTS_PASS = False


def parametered_test():
    ''' Parametered query tests '''

    global TESTS_PASS

    print ('\nParametered Tests')
    print ('-----------------')

    params = 6,
    con.execute(f'create or replace table test (t_int int)')
    con.executemany('insert into test values (?)', [(5,), (6,), (7,)])
    con.execute('select * from test where t_int > ?', params)
    res = con.fetchall()

    if res[0][0] != 7:
        print (f"parametered test fail, expected value {params} but got {res[0][0]}")
        TESTS_PASS = False



tests = {'pos'    : positive_tests, 
        'param'   : parametered_test,
        'neg'     : negative_tests    
        }



def main():

    args = sys.argv
    tests_to_run = 'pos', 'neg'  #, 'param', 
    # tests_to_run = 'neg'

    # Get SQream path and test names to run if given
    if len(args) >=2:
        sqream_path = args[1]
    if len(args) == 3:
        tests_to_run = args[2]

    # Remove previous instances and pop sqreamd
    if POP_SQREAM_AT_START: 
        Popen(('killall', '-9', 'sqreamd'))  
        sqreamd = Popen((sqream_path, cluster_dir))   

    # Wait for sqreamd to load
    # con = dbapi.connect('127.0.0.1', 5000, False, False, 'master', 'sqream', 'sqream') 
    # print ("connection: ", con)

    from time import time
    start = time()
    # Run selected tests (all by default)
    for test in tests_to_run:
        tests[test]()
    # print ("total:", time())

    # Check stats
    if TESTS_PASS:
        print ("Tests passed")

    ## Close SQream
    if CLOSE_WHEN_DONE:
        Popen(('killall', '-9', 'sqreamd'))  


    return TESTS_PASS


if __name__ =='__main__':

    main()
