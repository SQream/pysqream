from datetime import datetime, date, timezone
from numpy.random import randint, uniform
from math import floor
from queue import Queue
from subprocess import Popen
from time import sleep

import threading, sys, os
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream/')
import dbapi

q = Queue()
varchar_length = 10
nvarchar_length = 10
max_bigint = sys.maxsize if sys.platform not in ('win32', 'cygwin') else 2147483647

def generate_varchar(length):
    return ''.join(chr(num) for num in randint(32, 128, length))

col_types = {'bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'date', 'datetime', 'varchar({})'.format(varchar_length), 'nvarchar({})'.format(varchar_length)}

pos_test_vals = {'bool': (0, 1, True, False, 2, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'tinyint': (randint(0, 255), randint(0, 255), 0, 255, True, False),
                 'smallint': (randint(-32768, 32767), 0, -32768, 32767, True, False),
                 'int': (randint(-2147483648, 2147483647), 0, -2147483648, 2147483647, True, False),
                 'bigint': (randint(1-max_bigint, max_bigint), 0, 1-max_bigint, max_bigint, True, False),
                 'real': (float('inf'), float('-inf'), float('+0'), float('-0'), round(uniform(1e-6, 1e6), 5), 837326.52428, True, False),   # float('nan')
                 'double': (float('inf'), float('-inf'), float('+0'), float('-0'), uniform(1e-6, 1e6), True, False),  # float('nan')
                 'date': (date(1998, 9, 24), date(2020, 12, 1), date(1997, 5, 9), date(1993, 7, 13)),
                 'datetime': (datetime(1001, 1, 1, 10, 10, 10), datetime(1997, 11, 30, 10, 10, 10), datetime(1987, 7, 27, 20, 15, 45), datetime(1993, 12, 20, 17, 25, 46)),
                 'varchar': (generate_varchar(varchar_length), generate_varchar(varchar_length), generate_varchar(varchar_length), 'b   '),
                 'nvarchar': ('א', 'א  ', '', 'ab א')}

neg_test_vals = {'tinyint': (258, 3.6, 'test',  (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'smallint': (40000, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'int': (9999999999, 3.6, 'test',  (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'bigint': (92233720368547758070, 3.6, 'test', (1997, 12, 12, 10, 10, 10)),
                 'real': ('test', (1997, 12, 12, 10, 10, 10)),
                 'double': ('test', (1997, 12, 12, 10, 10, 10)),
                 'date': (5, 3.6, (-8, 9, 1), (2012, 15, 6), (2012, 9, 45), 'test', False, True),
                 'datetime': (5, 3.6, (-8, 9, 1, 0, 0, 0), (2012, 15, 6, 0, 0, 0), (2012, 9, 45, 0, 0, 0), (2012, 9, 14, 26, 0, 0), (2012, 9, 14, 13, 89, 0), 'test', False, True),
                 'varchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True),
                 'nvarchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True)}


def start_stop(op = 'start', build_dir=None, ip=None):

    Popen(('killall', '-9', 'sqreamd'))  
    sleep(5)
    Popen(('killall', '-9', 'server_picker'))  
    sleep(5)
    Popen(('killall', '-9', 'metadata_server'))  
    sleep(5)
    
    if op =='start':
        Popen((build_dir + 'metadata_server'))   
        sleep(5)
        Popen((build_dir + 'server_picker', ip, '3105'))   
        sleep(5)
        Popen((build_dir + 'sqreamd' ))   
        sleep(5)

    sleep(5)

def connect_dbapi(clustered=False, use_ssl=False):
        
        port = (3109 if use_ssl else 3108) if clustered else (5001 if use_ssl else 5000)
        
        return dbapi.connect(ip, port, 'master', 'sqream', 'sqream', clustered, use_ssl)

con = None


def connection_tests(build_dir = None, ip = None):
    
    print("Restart the server when a connection is open within 30 seconds") 
    con = connect_dbapi(False, False)
    
    '''
    print("stopped sqream server, sleeping 5 seconds")
    start_stop('stop', build_dir, ip)
    sleep(5)
    start_stop('start', build_dir, ip)
    print("started sqream server, trying to run a statement")
    try:
        con.execute("select 1")
    except Exception as e:
        if "SQreamd connection interrupted" not in repr(e):
            raise Exception("bad error message")
    # '''

    def test_connection_params(expected_err, ip='127.0.0.1', port=5000, database='master', user='sqream', password='sqream', clustered=False, use_ssl=False):

        try:
            dbapi.connect(ip, port, database, user, password, clustered, use_ssl)
        except Exception as e:
            if expected_err not in repr(e):
                raise Exception("bad error message")

    print("Connection tests - wrong ip")
    # test_connection_params('123.4.5.6', 5000, 'master', 'sqream', 'sqream', False, False), "perhaps wrong IP?")
    try:
        dbapi.connect('123.4.5.6', 5000, 'master', 'sqream', 'sqream', False, False)
    except Exception as e:
        if "perhaps wrong IP?" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - wrong port")
    try:
        dbapi.connect('127.0.0.1', 6000, 'master', 'sqream', 'sqream', False, False)
    except Exception as e:
        if "Connection refused" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - wrong database")
    try:
        dbapi.connect('127.0.0.1', 5000, 'wrong_db', 'sqream', 'sqream', False, False)
    except Exception as e:
        if "Database 'wrong_db' does not exist" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - wrong username")
    try:
        dbapi.connect('127.0.0.1', 5000, 'master', 'wrong_username', 'sqream', False, False)
    except Exception as e:
        if "role 'wrong_username' doesn't exist" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - wrong password")
    try:
        dbapi.connect('127.0.0.1', 5000, 'master', 'sqream', 'wrong_pw', False, False)
    except Exception as e:
        if "wrong password for role 'sqream'" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - close() function")
    con = connect_dbapi()
    con.close()
    try:
        con.execute('select 1')
    except Exception as e:
      if "Connection has been closed" not in repr(e):
          raise Exception("bad error message")    

    print("Connection tests - close_connection() function")
    con = connect_dbapi()
    con.close_connection()
    try:
        con.execute('select 1')
    except Exception as e:
        if "Connection has been closed" not in repr(e):
            raise Exception("bad error message")
    
    print("Connection tests - Trying to close a connection that is already closed with close()")
    con = connect_dbapi()
    con.close()
    try:
        con.close()
    except Exception as e:
        if "Trying to close a connection that's already closed" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - Trying to close a connection that is already closed with close_connection()")
    con = connect_dbapi()
    con.close_connection()
    try:
        con.close_connection()
    except Exception as e:
        if "Trying to close a connection that's already closed" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - negative test for use_ssl=True")
    try:
        dbapi.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', False, True)
    except Exception as e:   
        if "Using use_ssl=True but connected to non ssl sqreamd port" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - positive test for use_ssl=True")
    con = connect_dbapi(False, True)
    res = con.execute('select 1').fetchall()[0][0]
    if res != 1:
        if f'expected to get 1, instead got {res}' not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - negative test for clustered=True")
    try:
        dbapi.connect('127.0.0.1', 5000, 'master', 'sqream', 'sqream', True, False)
    except Exception as e:
        if "Connected with clustered=True, but apparently not a server picker port" not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - positive test for clustered=True")
    con = connect_dbapi(True, False)
    res = con.execute('select 1').fetchall()[0][0]
    if res != 1:
        if f'expected to get 1, instead got {res}' not in repr(e):
            raise Exception("bad error message")

    print("Connection tests - both clustered and use_ssl flags on True")
    con = connect_dbapi(True, True)
    res = con.execute('select 1').fetchall()[0][0]
    if res != 1:
        if f'expected to get 1, instead got {res}' not in repr(e):
            raise Exception("bad error message")


def positive_tests():

    for col_type in col_types:
        trimmed_col_type = col_type.split('(')[0]
        
        print(f'Inserted values test for column type {col_type}')
        con.execute(f"create or replace table test (t_{trimmed_col_type} {col_type})")
        for val in pos_test_vals[trimmed_col_type]:
            con.execute('truncate table test')
            rows = [(val,)]
            con.executemany("insert into test values (?)", rows)
            res = con.execute("select * from test").fetchall()[0][0]
            
            # Compare
            if val != res:
                if trimmed_col_type not in ('bool', 'varchar', 'date', 'datetime', 'real'):
                    print((repr(val), type(val), repr(res), type(res)))
                    raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))
                elif trimmed_col_type == 'bool' and val != 0:
                    if res is not True:
                        raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get 'True', instead got {} on datatype {}".format(repr(res), trimmed_col_type))
                elif trimmed_col_type == 'varchar' and val.strip() != res:
                    raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))
                elif trimmed_col_type in ('date', 'datetime') and datetime(*val) != res and date(*val) != res:
                    raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))
                elif trimmed_col_type == 'real' and abs(res-val) > 0.1:
                    # Single precision packing and unpacking is inaccurate:
                    # unpack('f', pack('f', 255759.83335))[0] == 255759.828125
                    raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))

        print(f'Null test for column type: {col_type}')
        con.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
        con.executemany('insert into test values (?)', [(None,)])
        res = con.execute('select * from test').fetchall()[0][0]
        if res not in (None,):
            raise Exception("TEST ERROR: Error setting null on column type: {}\nGot: {}, {}".format(trimmed_col_type, res, type(res)))
    
    print("Case statement with nulls")
    con.execute("create or replace table test (xint int)")
    con.executemany('insert into test values (?)', [(5,), (None,), (6,), (7,), (None,), (8,), (None,)])
    con.executemany("select case when xint is null then 1 else 0 end from test")
    expected_list = [0, 1, 0, 0, 1, 0, 1]
    res_list = []
    res_list += [x[0] for x in con.fetchall()]
    if expected_list != res_list:
        raise Exception("expected to get {}, instead got {}".format(expected_list, res_list))

    print("Testing select true/false")
    con.execute("select false")
    res = con.fetchall()[0][0]
    if res != 0:
        raise Exception("Expected to get result 0, instead got {}".format(res))
    con.execute("select true")
    res = con.fetchall()[0][0]
    if res != 1:
        raise Exception("Expected to get result 1, instead got {}".format(res))

    print("Running a statement when there is an open statement")
    con.execute("select 1")
    sleep(10)
    res = con.execute("select 1").fetchall()[0][0]
    if res != 1:
        raise Exception(f'expected to get result 1, instead got {res}')


def negative_tests():
    ''' Negative Set/Get tests '''

    for col_type in col_types:
        if col_type == 'bool':
            continue
        print("Negative tests for column type: {}".format(col_type))
        trimmed_col_type = col_type.split('(')[0]
        print("prepare a table")
        con.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
        for val in neg_test_vals[trimmed_col_type]:
            print("Insert value {} into data type {}".format(repr(val), repr(trimmed_col_type)))
            rows = [(val,)]
            try:
                con.executemany("insert into test values (?)", rows)
            except Exception as e:
                if "Error packing columns. Check that all types match the respective column types" not in repr(e):
                    raise Exception(f'bad error message')

    print("Inconsistent sizes test")
    con.execute("create or replace table test (xint int, yint int)")
    try:
        con.executemany('insert into test values (?, ?)', [(5,), (6, 9), (7, 8)])
    except Exception as e:
        if "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length" not in repr(e):
            raise Exception(f'bad error message')

    print("Varchar - Conversion of a varchar to a smaller length")
    con.execute("create or replace table test (test varchar(10))")
    try:
        con.executemany("insert into test values ('aa12345678910')")
    except Exception as e:
        if "expected response statementPrepared but got" not in repr(e):
                        raise Exception(f'bad error message')

    print("Nvarchar - Conversion of a varchar to a smaller length")
    con.execute("create or replace table test (test nvarchar(10))")
    try:
        con.executemany("insert into test values ('aa12345678910')")
    except Exception as e:
        if "expected response executed but got" not in repr(e):
            raise Exception(f'bad error message')

    print("Incorrect usage of fetchmany - fetch without a statement")
    con.execute("create or replace table test (xint int)")
    try:
        con.fetchmany(2)
    except Exception as e:
        if "No open statement while attempting fetch operation" not in repr(e):
            raise Exception(f'bad error message')

    print("Incorrect usage of fetchall")
    con.execute("create or replace table test (xint int)")
    con.executemany("select * from test")
    try:
        con.fetchall(5)
    except Exception as e:
        if "Bad argument to fetchall" not in repr(e):
            raise Exception(f'bad error message')

    print("Incorrect usage of fetchone")
    con.execute("create or replace table test (xint int)")
    con.executemany("select * from test")
    try:
        con.fetchone(5)
    except Exception as e:
        if "Bad argument to fetchone" not in repr(e):
            raise Exception(f'bad error message')

    print("Multi statements test")
    try:
        con.execute("select 1; select 1;")
    except Exception as e:
        if "expected one statement, got 2" not in repr(e):
            raise Exception(f'bad error message')

    print("Parametered query tests")
    params = 6
    con.execute("create or replace table test (xint int)")
    con.executemany('insert into test values (?)', [(5,), (6,), (7,)])
    try:    
        con.execute('select * from test where xint > ?', str(params))
    except Exception as e:
        if "Parametered queries not supported" not in repr(e):
            raise Exception(f'bad error message')

    print("running execute on a closed cursor")
    cur = con.cursor()
    cur.close()
    try:
        cur.execute("select 1")
    except Exception as e:
        if "Cursor has been closed" not in repr(e):
            raise Exception(f'bad error message')


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


def fetch_tests():

    print("positive fetch tests")
    con.execute("create or replace table test (xint int)")
    con.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
    # fetchmany(1) vs fetchone()
    con.execute("select * from test")
    res = con.fetchmany(1)[0]
    con.execute("select * from test")
    res2 = con.fetchone()[0]
    if res != res2:
        raise Exception("fetchmany(1) and fetchone() didn't return the same value. fetchmany(1) is {} and fetchone() is {}".format(res, res2))
    # fetchmany(-1) vs fetchall()
    con.execute("select * from test")
    res3 = con.fetchmany(-1)
    con.execute("select * from test")
    res4 = con.fetchall()
    if res3 != res4:
        raise Exception("fetchmany(-1) and fetchall() didn't return the same value. fetchmany(-1) is {} and fetchall() is {}".format(res3, res4))
    # fetchone() loop
    con.execute("select * from test")
    for i in range(1, 11):
        x = con.fetchone()[0]
        if x != i:
            raise Exception("fetchone() returned {} instead of {}".format(x, i))

    print("combined fetch functions")
    con.execute("create or replace table test (xint int)")
    con.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
    con.execute("select * from test")
    expected_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    res_list = []
    res_list.append(con.fetchone()[0])
    res_list += [x[0] for x in con.fetchmany(2)]
    res_list.append(con.fetchone()[0])
    res_list += [x[0] for x in con.fetchall()]
    if expected_list != res_list:
        raise Exception("expected to get {}, instead got {}".format(expected_list, res_list))
    
    print("fetch functions after all the data has already been read")
    con.execute("create or replace table test (xint int)")
    con.executemany('insert into test values (?)', [(1,)])
    con.execute("select * from test")
    x = con.fetchone()[0]
    res = con.fetchone()
    if res is not None:
        raise Exception(f"expected to get an empty result from fetchone, instead got {res}")
    res = con.fetchall()
    if res != []:
        raise Exception(f"expected to get an empty result from fetchall, instead got {res}")
    res = con.fetchmany(1)
    if res != []:
        raise Exception(f"expected to get an empty result from fetchmany, instead got {res}")


def cursor_tests():

    print("running two statements on the same cursor connection")
    vals = [1]
    cur = con.cursor()
    cur.execute("select 1")
    res1 = cur.fetchall()[0][0]
    vals.append(res1)
    cur.execute("select 1")
    res2 = cur.fetchall()[0][0]
    vals.append(res2)
    if not all(x == vals[0] for x in vals):
        raise Exception(f"expected to get result 1, instead got {res1} and {res2}")

    print("running a statement through cursor when there is an open statement")
    cur = con.cursor()
    cur.execute("select 1")
    sleep(10)
    cur.execute("select 1")
    res = cur.fetchall()[0][0]
    if res != 1:
        raise Exception(f"expected to get result 1, instead got {res}")
    
    print("fetch functions after all the data has already been read through cursor")
    cur = con.cursor()
    cur.execute("create or replace table test (xint int)")
    cur.executemany('insert into test values (?)', [(1,)])
    cur.execute("select * from test")
    x = cur.fetchone()[0]
    res = cur.fetchone()
    if res is not None:
        raise Exception("expected to get an empty result from fetchone, instead got {}".format(res))
    res = con.fetchall()
    if res != []:
        raise Exception("expected to get an empty result from fetchall, instead got {}".format(res))
    res = con.fetchmany(1)
    if res != []:
        raise Exception("expected to get an empty result from fetchmany, instead got {}".format(res))


def string_tests():
    
    print("insert and return UTF-8")
    con.execute("create or replace table test (xvarchar varchar(20))")
    con.executemany('insert into test values (?)', [(u"hello world",), ("hello world",)])
    con.execute("select * from test")
    res = con.fetchall()
    if res[0][0] != res[1][0]:
        raise Exception("expected to get identical strings from select statement. instead got {} and {}".format(res[0][0], res[1][0]))
    
    print("strings with escaped characters")
    con.execute("create or replace table test (xvarchar varchar(20))")
    values = [("\t",), ("\n",), ("\\n",), ("\\\n",), (" \\",), ("\\\\",), (" \nt",), ("'abd''ef'",), ("abd""ef",), ("abd\"ef",)]
    con.executemany('insert into test values (?)', values)
    con.executemany("select * from test")
    expected_list = ['', '', '\\n', '\\', ' \\', '\\\\', ' \nt', "'abd''ef'", 'abdef', 'abd"ef']
    res_list = []
    res_list += [x[0] for x in con.fetchall()]
    if expected_list != res_list:
        raise Exception("expected to get {}, instead got {}".format(expected_list, res_list))


def datetime_tests():

    print("insert different timezones datetime")
    t1 = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    t2 = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    con.execute("create or replace table test (xdatetime datetime)")
    con.executemany('insert into test values (?)', [(t1,), (t2,)])
    con.execute("select * from test")
    res = con.fetchall()
    if res[0][0] == res[1][0]:
        raise Exception("expected to get different datetimes")
    
    print("insert datetime with microseconds")
    t1 = datetime(1997, 5, 9, 4, 30, 10, 123456)
    t2 = datetime(1997, 5, 9, 4, 30, 10, 987654)
    con.execute("create or replace table test (xdatetime datetime)")
    con.executemany('insert into test values (?)', [(t1,), (t2,)])


def connect_and_execute(num, cursor=False):

    con = connect_dbapi()

    if cursor:
        cur = con.cursor()
        cur.execute("select {}".format(num))
        res = cur.fetchall()
        q.put(res)
    
    con.execute("select {}".format(num))
    res = con.fetchall()
    q.put(res)


def threads_tests():

    print("concurrent inserts with multiple threads")
    t1 = threading.Thread(target=connect_and_execute, args=(3, ))
    t2 = threading.Thread(target=connect_and_execute, args=(3, ))
    t1.start()
    t2.start()
    res1 = q.get()[0][0]
    res2 = q.get()[0][0]
    if res1 != res2:
        raise Exception("expected to get equal values. instead got res1 {} and res2 {}".format(res1, res2))

    print("concurrent inserts with multiple threads through cursor")
    t1 = threading.Thread(target=connect_and_execute, args=(5, True))
    t2 = threading.Thread(target=connect_and_execute, args=(5, True))
    t1.start()
    t2.start()
    res1 = q.get()[0][0]
    res2 = q.get()[0][0]
    if res1 != res2:
        raise Exception("expected to get equal values. instead got res1 {} and res2 {}".format(res1, res2))


def copy_tests():

    print("loading a csv file into a table through dbapi")
    con.execute("copy t from 't.csv' with delimiter ', '")
    con.execute("select count(*) from t")
    res = con.fetchall()[0][0]
    if res != 2000:
        raise Exception("expected to get 2000, instead got {}".format(res))




if __name__ == "__main__":

    args = sys.argv
    ip = args[1] if len(args) > 1 else '127.0.0.1'
    print (f'Tests connecting to: {ip}')
    # start_stop('start', build_dir, ip)

    con = connect_dbapi()
    # connection_tests()
    positive_tests()
    negative_tests()
    fetch_tests()
    cursor_tests()
    string_tests()
    datetime_tests()
    threads_tests()
    # copy_tests()
