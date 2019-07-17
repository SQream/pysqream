# encoding: utf-8
from __future__ import print_function

import SQream_python_connector
from SQream_python_connector import BadTypeForSetFunction, RowFillException, ValueRangeException, FaultyDateTuple, FaultyDateTimeTuple, WrongGetStatement, WrongSetStatement
import os, sys
import numpy as np
from time import sleep
from datetime import datetime, date
from subprocess import Popen, PIPE
# from random import random, randint
from numpy.random import rand, randint, uniform
from itertools import product

# Compatibility related 
VER = sys.version_info
MAJOR = VER[0]
if MAJOR == 3:
    unicode  = str    # to allow dual compatibility
    long = int
else:
    reload(sys)
    sys.setdefaultencoding('utf-8')

POP_SQREAM_AT_START = False   # True
CLOSE_WHEN_DONE = False 


## Setup server and storage params
up_by = 2  #1 when in tests directory
sqream_dir = os.getcwd().rsplit('/', up_by)[0] + '/'
sqream_path = sqream_dir + '/build/sqreamd'
home = '/' + '/'.join(Popen(b'pwd', stdout=PIPE).communicate()[0].decode('utf8').split('/')[1:3]) +'/'
cluster_dir = home + 'tpch_100gb'
other_params = ''
con = SQream_python_connector.Connector()

## Tests setup
#  -----------

varchar_length = 10
nvarchar_length = 10
errors = []

def generate_varchar(length):
    return ''.join(chr(num) for num in randint(1,128, length))


col_types =     {'bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'date', 'datetime', 'varchar({})'.format(varchar_length), 'nvarchar({})'.format(varchar_length)}  

pos_test_vals = {'bool':    (0, 1, True, False),   
                 'tinyint':    (randint(0, 255), randint(0, 255), 0, 255),
                 'smallint':    (randint(-32768, 32767), 0, -32768, 32767),
                 'int':      (randint(-2147483648, 2147483647), 0, -2147483648, 2147483647),
                 'bigint':   (long(randint(-9223372036854775808, 9223372036854775807)), long(0), long(-9223372036854775808), long(9223372036854775807)),
                 'real':     (float('inf'), float('-inf'), float('+0'), float('-0'), round(uniform(1e-6, 1e6), 5)),   # float('nan')    
                 'double':     (float('inf'), float('-inf'), float('+0'), float('-0'), uniform(1e-6, 1e6)),  # float('nan') 
                 'date':     ((1998, 9, 24), (1998, 9, 24), (1998, 9, 24), (1998, 9, 24)),
                 'datetime': ((1998, 9, 24, 17, 25, 46), (1998, 9, 24, 17, 25, 46), (1998, 9, 24, 17, 25, 46), (1998, 9, 24, 17, 25, 46)),
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


## Helper functions
#  ----------------

def _prepare_table(col_type):
    ''' Helper function for creating the appropriate test table'''

    con.prepare('create or replace table test (t_{} {})'.format(col_type.split('(')[0], col_type))  
    con.execute() 
    con.close()


## Tests
#  -----

TESTS_PASS = True

def positive_tests():
    ''' Positive Set/Get Tests '''

    global TESTS_PASS

    print ('Starting Positive Set/Get Tests')
    print ('-------------------------------')

    for col_type in col_types:

        _prepare_table(col_type)

        col_type = col_type.split('(')[0]
        # print (col_type)  # dbg
        for val in pos_test_vals[col_type]:
            print ('Trying get() on column type {} with value {}'.format(col_type, val))
            # Clean up the table
            con.prepare('truncate table test')
            con.execute()
            con.close()

            # Insert value
            con.prepare('insert into test values (?)')
            con.execute()
            getattr(con, 'set_' + col_type)(1, val)
            con.next_row()
            con.close()

            # Get it back
            con.prepare('select * from test')
            con.execute()
            con.next_row()
            index_res = getattr(con, 'get_' + col_type)(1)
            name_res = getattr(con, 'get_' + col_type)('t_'+col_type)
            con.close()
            
            if 'nvarchar' in col_type:
                index_res = index_res.decode('utf8')
                name_res = name_res.decode('utf8')
                val = val.strip()

            # Compare
            # print (val, type(val), index_res, type(index_res))  #dbg
            if val != index_res:
                if col_type not in ('date', 'datetime', 'real'): 
                    print (repr(val), type(val), repr(index_res), type(index_res))
                    error = "TEST ERROR: No match with get_{}() by index with values: {}, {}".format(col_type, repr(val), repr(index_res))
                    print (error)
                    TESTS_PASS = False
                elif col_type == 'real' and round(val) != round(index_res):
                    # Single precision packing and unpacking is inaccurate:
                    # unpack('f', pack('f', 255759.83335))[0] == 255759.828125
                    error = "TEST ERROR: No match with get_{}() by index with values: {}, {}".format(col_type, repr(val), repr(index_res))
                    print (error)
                    TESTS_PASS = False
                elif col_type in ('date', 'datetime') and datetime(*val) != index_res and date(*val) != index_res: 
                    error = "TEST ERROR: No match with get_{}() by index with values: {}, {}".format(col_type, val, index_res)
                    print (error)
                    TESTS_PASS = False
            
            if val != name_res:
                if col_type not in ('date', 'datetime', 'real'): 
                    error = "TEST ERROR: No match with get_{}() by name with values: {}, {}".format(col_type, val, index_res)
                    print (error)
                    TESTS_PASS = False
                elif col_type == 'real' and round(val) != round(index_res):
                    error = "TEST ERROR: No match with get_{}() by name with values: {}, {}".format(col_type, repr(val), repr(index_res))
                    print (error)
                    TESTS_PASS = False
                elif col_type in ('date', 'datetime') and datetime(*val) != index_res and date(*val) != index_res:
                    error = "TEST ERROR: No match with get_{}() by name with values: {}, {}".format(col_type, val, index_res)
                    print (error)
                    TESTS_PASS = False      
    print ('')


def negative_tests():
    ''' Negative Set/Get Tests '''

    print ("Starting Negative Set/Get Tests")
    print ('-------------------------------') 

    for col_type in col_types:    
        # Create appropriate table
        _prepare_table(col_type)

        trimmed_col_type = col_type.split('(')[0]        
        # print ('column type in check: ', col_type)   #dbg

        # '''
        # Using the wrong set() function 
        # ------------------------------
        for neg_col_type in col_types-{col_type}:

            neg_col_type = neg_col_type.split('(')[0]
            pos_val = pos_test_vals[neg_col_type][0]

            # Insert using wrong function
            con.prepare('insert into test values (?)')
            con.execute()
            try: 
                getattr(con, 'set_' + neg_col_type)(1, pos_val)
            except WrongSetStatement:
                pass            
            except Exception as e: # Not the right error
                print ('TEST ERROR: Bad error returned on setting type {} on column type {}'.format(neg_col_type, col_type))
                print (e)
                
                TESTS_PASS = False
            else:  # Incorrect set statement managed to get through
                # if not (neg_col_type, col_type) == ('ftFloat', 'ftDouble'):
                print ('TEST ERROR: No error returned on setting type {} on column type {}'.format(neg_col_type, col_type))
                TESTS_PASS = False
            
            try:
                con.next_row()   # Where should it fail
            except RowFillException:
                pass
            except:
                print ('TEST ERROR: next_row() did not catch error correctly on incomplete row fill')
                TESTS_PASS = False
            else:
                # if not (neg_col_type, col_type) == ('ftFloat', 'ftDouble'):
                print ('TEST ERROR: No error returned on setting type on incomplete row fill')
                TESTS_PASS = False

            con.close()
        

        # Using the the right set() function with a bad value
        # ---------------------------------------------------
        for neg_val in neg_test_vals[trimmed_col_type]:
            # print ('neg_val: ', neg_val) #dbg

            # Insert bad values with the correct function
            con.prepare('insert into test values (?)')
            con.execute()
            try:
                getattr(con, 'set_' + trimmed_col_type)(1, neg_val)
            
            except FaultyDateTuple:
                # print (trimmed_col_type, neg_val)  # dbg
                pass
            except FaultyDateTimeTuple:
                # print (trimmed_col_type, neg_val)  # dbg
                pass
            except BadTypeForSetFunction:
                # print (trimmed_col_type, neg_val)  # dbg
                pass
            except:
                print ('TEST ERROR: Wrong error on setting {} to a {} column'.format(neg_val, trimmed_col_type))
                TESTS_PASS = False
            else:
                # if not (neg_col_type, col_type) == ('ftFloat', 'ftDouble'):
                print ('TEST ERROR: No error returned on setting {} to a {} column'.format(neg_val, trimmed_col_type))
                TESTS_PASS = False
            
            con.close()


        # Using the wrong get() function
        # ------------------------------
        print ('Trying the wrong get() functions on column type', col_type)
        # Insert a proper value
        pos_val = pos_test_vals[trimmed_col_type][0]
        con.prepare('insert into test values (?)')
        con.execute()
        getattr(con, 'set_' + trimmed_col_type)(1, pos_val)
        con.next_row()
        con.close()

        # Try to get the value using the wrong get()
        for neg_col_type in col_types-{col_type}:

            neg_col_type =  neg_col_type.split('(')[0]  
            con.prepare('select * from test')
            con.execute()
            con.next_row()
            
            # Wrong get by index
            try:
                index_res = getattr(con, 'get_' + neg_col_type)(1)
            except WrongGetStatement:
                # cool beans
                pass
            except:
                print ('TEST ERROR: Wrong error on using wrong get statement by index')
                TESTS_PASS = False
            else:
                # if not (neg_col_type, col_type) == ('ftFloat', 'ftDouble'):
                print ('TEST ERROR: No error returned on on using {} get statement on a {} column by index'.format(neg_col_type, col_type))
                TESTS_PASS = False
                con.close() 

            # Wrong get by column name
            try:
                name_res = getattr(con, 'get_' + neg_col_type)('t_'+trimmed_col_type)
            except WrongGetStatement:
                # cool beans
                 pass
            except:
                print ('TEST ERROR: Wrong error on using wrong get statement by column name')
                print (col_type, neg_col_type)
                TESTS_PASS = False
            else:
                # if not (neg_col_type, col_type) == ('ftFloat', 'ftDouble'):
                print ('TEST ERROR: No error returned on on using wrong get statement by column name')
                TESTS_PASS = False
            
            con.close() 


def positive_null():
    ''' Positive Null Tests '''

    print ('Positive Null Tests')
    print ('-------------------')

    for col_type in col_types:

        # Create appropriate table
        _prepare_table(col_type)
        
        col_type = col_type.split('(')[0]
        
        # Set null       
        con.prepare('insert into test values (?)')
        con.execute()
        con.set_null(1)
        con.next_row()
        con.close()

        # Get it back
        con.prepare('select * from test')
        con.execute()
        con.next_row()
        index_res = getattr(con, 'get_' + col_type)(1)
        name_res = getattr(con, 'get_' + col_type)('t_'+col_type)
        con.close()

        # Compare
        if index_res != '\\N':
            print ("TEST ERROR: Error setting null on column type: ", col_type)
            TESTS_PASS = False
   

def negative_null():
    ''' Negative Null Tests '''

    print ('Negative Null Tests')
    print ('-------------------')

    for col_type in col_types:

        # Create appropriate table
        _prepare_table(col_type)
        
        col_type = col_type.split('(')[0]
        
        # Set null       
        con.prepare('insert into test values (?)')
        con.execute()
        try:
            con.set_null(2)
        except IndexError:
            pass
        except:
            print ('TEST ERROR: Wrong error on using set_null() with a bad index')
            TESTS_PASS = False
        else:
            print ('TEST ERROR: No error returned on on using set_null() with a bad index')
            TESTS_PASS = False
        
        con.close()


def metadata_test():

    print ('Metadata test')
    print ('-------------------')

    # Create appropriate table
    con.prepare('create or replace table test (x int, y varchar(10))')  
    con.execute() 
    con.close()
 
    con.prepare("insert into test values (5, 'shoko')")  
    con.execute() 
    con.close()

    con.prepare('select * from test')  
    con.execute() 
    con.close()

    col1, col2 = con.get_metadata()

    if not (col1.name == 'x' and col1.is_tvc is False and col1.is_nullable is True and col1.type.tid == 'Int' and col1.type.size == 4):
        print ('TEST ERROR: metadata returned incorrect data')
        TESTS_PASS = False



def statement_type():

    print ('Statement type test')
    print ('-------------------')

    # Create appropriate table
    con.prepare('create or replace table test (x int, y varchar(10))')  
    con.execute() 
    if not (con.get_statement_type() == 'DML'):
        raise Exception ('TEST ERROR: wrong statement type returned on dml statement')
    con.close()
 
    con.prepare("insert into test values (?, ?)")
    con.execute() 
    con.set_int(1,5)
    con.set_varchar(2, "shoko")  
    if not (con.get_statement_type() == 'INSERT'):
        raise Exception ('TEST ERROR: wrong statement type returned on insert statement')
    con.close()

    con.prepare('select * from test')  
    con.execute() 
    if not (con.get_statement_type() == 'SELECT'):
        raise Exception ('TEST ERROR: wrong statement type returned on select statement')
    con.close()


'''
def sequence_test():

    # ----------------
    print ('Sequence Testing')
    print ('----------------')
    # command_list = ('execute', 'close', 'next', 'insert', 'select')  # 'create',
    # command_list = ('execute', 'close', 'next') #, 'insert', 'select')  # 'create',
    commands = {'execute': (con.execute, None),
                'close':  (con.close, None),
                'next':   (con.next_row, None)
                # 'create': (con.prepare, ('create or replace table test (t_{} {})'.format(col_type.split('(')[0], col_type))),
                # 'insert': (con.prepare, ('insert into test values (?)')),
                # 'select': (con.prepare, ('select * from test'))
                }           

    def run_seq(seq_len = 2):
        
        global TESTS_PASS
        
        command_lst = list(product(commands, repeat=seq_len))
        for seq in command_lst:   #range(20):
            # command_lst = [command_list[index] for index in randint(0, len(command_list), seq_len).tolist()]
            # command_seq = [commands[comm_name][0] for comm_name in command_list]    
            try:
                print ('Trying sequence: ', seq)
                [commands[command][0]() for command in seq]
            except RuntimeError:
                print ("raised a RuntimeError")
                con.close()      # ala commands['close'][0]()
                TESTS_PASS = False

            else:
                print ("okey dokey")

    run_seq(2)


# map (lambda location: commands[command_list[location]][0](commands[command_list[location]][1]), random_seq)
# map (lambda location: commands[command_list[location]][0](), random_seq)
#'''


def encoding_test():

    print ('Varcahr encoding test')
    print ('---------------------')

    # Create appropriate table
    con.prepare('create or replace table test (x varchar(10))')  
    con.execute() 
    print ("Encoding used for varchar:", con._sc.varchar_enc)
    con.close()
 
    con.prepare("insert into test values (?)")
    con.execute() 
    insert_varc = "shoko"
    print ("inserted value: ", insert_varc)
    con.set_varchar(1, insert_varc)  
    con.next_row()
    con.close()

    con.prepare('select * from test')  
    con.execute() 
    con.next_row()
    # result_varc = con.get_varchar(1)
    print ("returned value:", con.get_varchar(1))
    con.close()     


tests = {'pos' : positive_tests,
         'neg' : negative_tests,
         'pos_null': positive_null,
         'neg_null': negative_null,
         'metadata': metadata_test,
         'stmt_type': statement_type,
         'encoding_test': encoding_test
        }


def main():

    args = sys.argv
    tests_to_run = 'pos', 'neg', 'pos_null', 'neg_null', 'metadata', 'stmt_type', 'encoding_test'
    # tests_to_run = 'encoding_test',
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
    sqream_connection_params = '127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, 30
    initial_varchar_encoding = SQream_python_connector.VARCHAR_ENC
    con.connect(*sqream_connection_params)

    '''
    while True:
        try:
            con.connect(*sqream_connection_params)
        except:
            sleep(0.1)
        else:
            break
    '''
    # Run selected tests (all by default)
    for test in tests_to_run:
        tests[test]()

    # Check stats
    if TESTS_PASS:
        print ("Tests passed")

    ## Close SQream
    if CLOSE_WHEN_DONE:
        Popen(('killall', '-9', 'sqreamd'))  


    return TESTS_PASS


if __name__ =='__main__':

    main()








# q('create or replace table test (t_int int, t_bigint bigint, t_short short, t_long long, t_vc50 varchar(50), t_nvc50 nvarchar(50), t_date date, t_datetime datetime)')         
