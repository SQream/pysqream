from datetime import datetime, date, timezone
from numpy.random import randint, uniform
from math import floor
from queue import Queue
from subprocess import Popen
from time import sleep

import threading, sys, os
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/pysqream/')
import dbapi, pytest

q = Queue()
varchar_length = 10
nvarchar_length = 10
max_bigint = sys.maxsize if sys.platform not in ('win32', 'cygwin') else 2147483647

def generate_varchar(length):
    return ''.join(chr(num) for num in randint(32, 128, length))

def print_test(test_desc):
    print (f'\033[94mTest: {test_desc}\033[0m')


col_types = {'bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'date', 'datetime', 'varchar({})'.format(varchar_length), 'nvarchar({})'.format(varchar_length)}

pos_test_vals = {'bool': (0, 1, True, False, 2, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'tinyint': (randint(0, 255), randint(0, 255), 0, 255, True, False),
                 'smallint': (randint(-32768, 32767), 0, -32768, 32767, True, False),
                 'int': (randint(-2147483648, 2147483647), 0, -2147483648, 2147483647, True, False),
                 'bigint': (randint(1-max_bigint, max_bigint), 0, 1-max_bigint, max_bigint, True, False),
                 'real': (float('inf'), float('-inf'), float('+0'), float('-0'), round(uniform(1e-6, 1e6), 5), 837326.52428, True, False),   # float('nan')
                 'double': (float('inf'), float('-inf'), float('+0'), float('-0'), uniform(1e-6, 1e6), True, False),  # float('nan')
                 'date': (date(1998, 9, 24), date(2020, 12, 1), date(1997, 5, 9), date(1993, 7, 13), date(1001, 1, 1)),
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

# @pytest.fixture(scope = 'module')
def connect_dbapi(clustered=False, use_ssl=False):
    
    args = sys.argv
    ip = args[1] if len(args) > 1 else '127.0.0.1'
    port = (3109 if use_ssl else 3108) if clustered else (5001 if use_ssl else 5000)
    
    return dbapi.connect(ip, port, 'master', 'sqream', 'sqream', clustered, use_ssl)


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


pytest.con = connect_dbapi()
pytest.ip = '127.0.0.1'
pytest.connect_and_execute = connect_and_execute


class TestPositive:

    def test_positive(self):
        print('positive tests')
        for col_type in col_types:
            trimmed_col_type = col_type.split('(')[0]
            
            print(f'Inserted values test for column type {col_type}')
            pytest.con.execute(f"create or replace table test (t_{trimmed_col_type} {col_type})")
            for val in pos_test_vals[trimmed_col_type]:
                pytest.con.execute('truncate table test')
                rows = [(val,)]
                pytest.con.executemany("insert into test values (?)", rows)
                res = pytest.con.execute("select * from test").fetchall()[0][0]
                # Compare
                error = False
                assert (
                    val == res                                                                            or 
                    (val != res and trimmed_col_type == 'bool' and val != 0 and res == True)              or
                    (val != res and trimmed_col_type == 'varchar' and val != 0 and val.strip() == res)    or
                    (val != res and trimmed_col_type == 'real' and val != 0 and abs(res-val) <= 0.1)
                       )

            print(f'Null test for column type: {col_type}')
            pytest.con.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
            pytest.con.executemany('insert into test values (?)', [(None,)])
            res = pytest.con.execute('select * from test').fetchall()[0][0]
            assert res == None
    

    def test_nulls(self):

        print_test("Case statement with nulls")
        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany('insert into test values (?)', [(5,), (None,), (6,), (7,), (None,), (8,), (None,)])
        pytest.con.executemany("select case when xint is null then 1 else 0 end from test")
        expected_list = [0, 1, 0, 0, 1, 0, 1]
        res_list = []
        res_list += [x[0] for x in pytest.con.fetchall()]
        assert expected_list == res_list


    def test_bool(self):

        print_test("Testing select true/false")
        pytest.con.execute("select false")
        res = pytest.con.fetchall()[0][0]
        assert res == 0
        
        pytest.con.execute("select true")
        res = pytest.con.fetchall()[0][0]
        assert res == 1


    def test_when_running(self):

        print_test("Running a statement when there is an open statement")
        pytest.con.execute("select 1")
        sleep(10)
        res = pytest.con.execute("select 1").fetchall()[0][0]
        assert res == 1


class TestNegative:
    ''' Negative Set/Get tests '''

    def test_negative(self):

        print_test('Negative tests')
        for col_type in col_types:
            if col_type == 'bool':
                continue
            trimmed_col_type = col_type.split('(')[0]
            pytest.con.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
            for val in neg_test_vals[trimmed_col_type]:
                rows = [(val,)]
                with pytest.raises(Exception) as e:
                    pytest.con.executemany("insert into test values (?)", rows)
                assert "Error packing columns. Check that all types match the respective column types" in str(e.value)

    def test_incosistent_sizes(self):

        print_test("Inconsistent sizes test")
        pytest.con.execute("create or replace table test (xint int, yint int)")
        with pytest.raises(Exception) as e:
            pytest.con.executemany('insert into test values (?, ?)', [(5,), (6, 9), (7, 8)])
        assert "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length" in str(e.value)

    def test_varchar_conversion(self):

        print_test("Varchar - Conversion of a varchar to a smaller length")
        pytest.con.execute("create or replace table test (test varchar(10))")
        with pytest.raises(Exception) as e:
            pytest.con.executemany("insert into test values ('aa12345678910')")
        assert "expected response statementPrepared but got" in str(e.value)

    def test_nvarchar_conversion(self):

        print_test("Nvarchar - Conversion of a varchar to a smaller length")
        pytest.con.execute("create or replace table test (test nvarchar(10))")
        with pytest.raises(Exception) as e:
            pytest.con.executemany("insert into test values ('aa12345678910')")
        assert "expected response executed but got" in str(e.value)

    def test_incorrect_fetchmany(self):

        print_test("Incorrect usage of fetchmany - fetch without a statement")
        pytest.con.execute("create or replace table test (xint int)")
        with pytest.raises(Exception) as e:
            pytest.con.fetchmany(2)
        assert "No open statement while attempting fetch operation" in str(e.value)

    def test_incorrect_fetchall(self):

        print_test("Incorrect usage of fetchall")
        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany("select * from test")
        with pytest.raises(Exception) as e:
            pytest.con.fetchall(5)
        assert "Bad argument to fetchall" in str(e.value)

    def test_incorrect_fetchone(self):

        print_test("Incorrect usage of fetchone")
        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany("select * from test")
        with pytest.raises(Exception) as e:
            pytest.con.fetchone(5)
        assert "Bad argument to fetchone" in str(e.value)
    
    def test_multi_statement(self):                

        print_test("Multi statements test")
        with pytest.raises(Exception) as e:
            pytest.con.execute("select 1; select 1;")
        assert "expected one statement, got 2" in str(e.value)
    
    def test_parametered_query(self):        

        print_test("Parametered query tests")
        params = 6
        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany('insert into test values (?)', [(5,), (6,), (7,)])
        with pytest.raises(Exception) as e:
            pytest.con.execute('select * from test where xint > ?', str(params))
        assert "Parametered queries not supported" in str(e.value)
    
    def test_execute_closed_cursor(self):
    
        print_test("running execute on a closed cursor")
        cur = pytest.con.cursor()
        cur.close()
        with pytest.raises(Exception) as e:
            cur.execute("select 1")
        assert "Cursor has been closed" in str(e.value)


class TestFetch:

    def test_fetch(self):

        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
        # fetchmany(1) vs fetchone()
        pytest.con.execute("select * from test")
        res = pytest.con.fetchmany(1)[0][0]
        pytest.con.execute("select * from test")
        res2 = pytest.con.fetchone()[0]
        assert res == res2

        # fetchmany(-1) vs fetchall()
        pytest.con.execute("select * from test")
        res3 = pytest.con.fetchmany(-1)
        pytest.con.execute("select * from test")
        res4 = pytest.con.fetchall()
        assert res3 == res4

        # fetchone() loop
        pytest.con.execute("select * from test")
        for i in range(1, 11):
            x = pytest.con.fetchone()[0]
            assert x == i

    def test_combined_fetch(self):

        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
        pytest.con.execute("select * from test")
        expected_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        res_list = []
        res_list.append(pytest.con.fetchone()[0])
        res_list += [x[0] for x in pytest.con.fetchmany(2)]
        res_list.append(pytest.con.fetchone()[0])
        res_list += [x[0] for x in pytest.con.fetchall()]
        expected_list == res_list
    
    def test_fetch_after_data_read(self):

        pytest.con.execute("create or replace table test (xint int)")
        pytest.con.executemany('insert into test values (?)', [(1,)])
        pytest.con.execute("select * from test")
        x = pytest.con.fetchone()[0]
        res = pytest.con.fetchone()
        assert res is None
        
        res = pytest.con.fetchall()
        assert res == []
        
        res = pytest.con.fetchmany(1)
        assert res == []


class TestCursor:

    def test_cursor_through_clustered(self):

        con_clustered = dbapi.connect(pytest.ip, 3108, 'master', 'sqream', 'sqream', clustered=True)
        cur = con_clustered.cursor()
        assert cur.execute("select 1").fetchall()[0][0] == 1

    def test_two_statements_same_cursor(self):

        vals = [1]
        cur = pytest.con.cursor()
        cur.execute("select 1")
        res1 = cur.fetchall()[0][0]
        vals.append(res1)
        cur.execute("select 1")
        res2 = cur.fetchall()[0][0]
        vals.append(res2)
        assert all(x == vals[0] for x in vals)

    def test_cursor_when_open_statement(self):
        
        cur = pytest.con.cursor()
        cur.execute("select 1")
        sleep(10)
        cur.execute("select 1")
        res = cur.fetchall()[0][0]
        assert res == 1

    def test_fetch_after_all_read(self):   

        cur = pytest.con.cursor()
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,)])
        cur.execute("select * from test")
        x = cur.fetchone()[0]
        res = cur.fetchone()
        assert res is  None
        
        res = pytest.con.fetchall()
        assert res == []
        
        res = pytest.con.fetchmany(1)
        assert res == []


class TestString:

    def test_insert_return_utf8(self):

        pytest.con.execute("create or replace table test (xvarchar varchar(20))")
        pytest.con.executemany('insert into test values (?)', [(u"hello world",), ("hello world",)])
        pytest.con.execute("select * from test")
        res = pytest.con.fetchall()
        assert res[0][0] == res[1][0]
    
    def test_strings_with_escaped_chars(self): 

        pytest.con.execute("create or replace table test (xvarchar varchar(20))")
        values = [("\t",), ("\n",), ("\\n",), ("\\\n",), (" \\",), ("\\\\",), (" \nt",), ("'abd''ef'",), ("abd""ef",), ("abd\"ef",)]
        pytest.con.executemany('insert into test values (?)', values)
        pytest.con.executemany("select * from test")
        expected_list = ['', '', '\\n', '\\', ' \\', '\\\\', ' \nt', "'abd''ef'", 'abdef', 'abd"ef']
        res_list = []
        res_list += [x[0] for x in pytest.con.fetchall()]
        assert expected_list == res_list


class TestDatetime:

    def test_different_timezones(self): 

        t1 = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
        t2 = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
        pytest.con.execute("create or replace table test (xdatetime datetime)")
        pytest.con.executemany('insert into test values (?)', [(t1,), (t2,)])
        pytest.con.execute("select * from test")
        res = pytest.con.fetchall()
        assert res[0][0] != res[1][0]
    
    def test_datetimes_with_microseconds(self): 

        t1 = datetime(1997, 5, 9, 4, 30, 10, 123456)
        t2 = datetime(1997, 5, 9, 4, 30, 10, 987654)
        pytest.con.execute("create or replace table test (xdatetime datetime)")
        pytest.con.executemany('insert into test values (?)', [(t1,), (t2,)])


class TestThreads:

    def test_concurrent_insert(self): 

        t1 = threading.Thread(target=pytest.connect_and_execute, args=(3, ))
        t2 = threading.Thread(target=pytest.connect_and_execute, args=(3, ))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        res1 = q.get()[0][0]
        res2 = q.get()[0][0]
        assert res1 == res2

    def test_concurrent_insert_through_cursor(self):

        t1 = threading.Thread(target=pytest.connect_and_execute, args=(5, True))
        t2 = threading.Thread(target=pytest.connect_and_execute, args=(5, True))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        res1 = q.get()[0][0]
        res2 = q.get()[0][0]
        assert res1 == res2
