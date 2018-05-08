from __future__ import absolute_import, division, print_function, unicode_literals

PYSQREAM_VERSION = "2.0"
"""
Python2.7/3.x connector for SQream DB

Usage example:

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


"""


import sys, socket, json, atexit, select, ssl, logging   
import struct, array

from datetime import date, datetime
from time import time, gmtime, sleep
from struct import pack, unpack
from itertools import groupby
from multiprocessing import Process, Pipe #, Queue
from decimal import Decimal
from operator import add
from threading import Event


# Default constants
PROTOCOL_VERSION = 5
DEFAULT_BUFFER_SIZE = 4096    #65536 
DEFAULT_NETWORK_CHUNKSIZE = 10000  
FLUSH_SIZE = 65536     # Default flush size for set() operations

# Setting the fast version of range() for Py 2
try: 
    range = xrange
except:
    pass

VER = sys.version_info
MAJOR = VER[0]


def version_info():
    info = "PySqreamConn version: {}\nSQream Protocol version: {}".format(PYSQREAM_VERSION, PROTOCOL_VERSION)
    return info

## API exception types
#  ------------------------
class BadTypeForSetFunction(Exception):
    pass

class RowFillException(Exception):
    pass

class ValueRangeException(Exception):
    pass  # not in use

class FaultyDateTuple(Exception):
    pass

class FaultyDateTimeTuple(Exception):
    pass

class WrongGetStatement(Exception):
    pass

class WrongSetStatement(Exception):
    pass

exceptions = BadTypeForSetFunction, RowFillException, ValueRangeException, FaultyDateTuple, FaultyDateTimeTuple, WrongGetStatement, WrongSetStatement

def announce(exception, message = None):
    ''' This can contain logging, optional printing and other handling. If turns
        out to be unused, can be removed'''
    
    raise exception(message)


## Ranges and conversions for SQream types
#  ---------------------------------------

tinyint_range = 0, 255
smallint_range = -32768, 32767
int_range = -2147483648, 2147483647
# bigint_range = -9223372036854775808, 9223372036854775807
float_range = 1.18e-38, 3.4e38

packing_column_codes =   {'ftBool':   'B',   
                        'ftUByte':    'B',
                        'ftShort':    'h',
                        'ftInt':      'i',  
                        'ftLong':     'q',
                        'ftFloat':    'f',                 
                        'ftDouble':   'd',
                        'ftDate':     'i',
                        'ftDateTime': 'q',
                        'ftVarchar':   None,
                        'ftBlob':      None
                        }

sqream_typenames_to_codes = {'BOOLEAN':  'ftBool', 
                            'TINYINT':  'ftUByte',
                            'SMALLINT': 'ftShort',
                            'INT':      'ftInt', 
                            'BIGINT':   'ftLong', 
                            'FLOAT':    'ftDouble',
                            'REAL':     'ftFloat',
                            'DATE':     'ftDate',
                            'DATETIME': 'ftDateTime',
                            'TIMESTAMP':'ftDateTime',
                            'VARCHAR':  'ftVarchar',
                            'NVARCHAR': 'ftBlob'
                            }    


# Datetime conversions from SQream to Python
#  ------------------------------------------

# 4b int to Date
def int_to_date(d):
    y = int((10000 * d + 14780) // 3652425)
    ddd = int(d - (y * 365 + y // 4 - y // 100 + y // 400))
    if (ddd < 0):
        y -= 1
        ddd = int(d - (y * 365 + y // 4 - y // 100 + y // 400))
    mi = int((52 + 100 * ddd) // 3060)
    yyyy = int((y + (mi + 2) // 12))
    mm = int(((mi + 2) % 12 + 1))
    dd = int((ddd - (mi * 306 + 5) // 10 + 1))
    return date(yyyy, mm, dd)

# 8b int to Timestamp
def long_to_datetime(dts):
    u = (dts >> 32)
    l = dts & 0xffffffff
    d = int_to_date(u)
    msec = int(l) % 1000
    l //= 1000
    sec = l % 60
    l //= 60
    min = l % 60
    l //= 60
    hour = int(l)
    return datetime(d.year, d.month, d.day, hour, min, sec, msec)


# Datetime conversions from Python to SQream
#  ------------------------------------------

# "Epoch's" long: 3090091537718528
def dateparts_to_int (year, month, day):      
    ''' Adapted magic from day_g2() @ sqream/cpp/basic/utils/dateutils.h, ~ line 51 '''
    month = (month + 9) % 12;
    year = year - month//10;
    
    # print (365*year + year//4 - year//100 + year//400 + (month*306 + 5)//10 + (day - 1))
    return 365*year + year//4 - year//100 + year//400 + (month*306 + 5)//10 + (day - 1)


def timeparts_to_int(hour, minute, second, msecond=0):
    ''' Based on to_time() @ sqream/cpp/basic/utils/dateutils.h, ~ line 98 '''
    
    return hour*3600*1000 + minute*60*1000 + second*1000 + msecond//1000


def dtparts_to_long  (year, month, day, hour, minute, second, msecond=0):
    ''' Convert a legal 7 figure to a SQream long'''
    date_as_int = dateparts_to_int(year, month, day) 
    time_as_int = timeparts_to_int(hour, minute, second, msecond)

    return (date_as_int << 32) + time_as_int

def validate_datetime_tuple(tup):

    try:
        datetime(*tup)
    except:
        return False
    else:
        return True  


# Conversion helper to deal with dates
def conv_data_type(type, data):
    # Type conversions for unpack
    if type == "ftDate":
        unpack_type = packing_column_codes[type]
        d = unpack(unpack_type, data)[0]
        return int_to_date(d)
    elif type == "ftDateTime":
        unpack_type = packing_column_codes[type]
        dt = unpack(unpack_type, data)[0]
        return long_to_datetime(dt)
    else:
        unpack_type = packing_column_codes[type]
        return unpack(unpack_type, data)[0]


## SQream column class
#  -------------------

class SqreamColumn(object):
    def __init__(self):
        self._type_name = None
        self._type_size = None
        self._column_name = None
        self._column_size = None
        self._isTrueVarChar = False
        self._nullable = False
        self._column_data = []


## Batch class for per-record aggregation
#  ---------------------------------------

class BinaryColumn:
    ''' Generates a binary to be network inserted to SQream.
        Also holds complementary null and length columns. '''
    
    def __init__(self, col_type, nullable = True, varchar_size = 0):
        self.col_type = col_type
        self.nullable = nullable
        self.varchar_size = varchar_size
        self.add_val, self.add_null = self.set_add_val_null()
        self._index = -1      # Number of values inserted, serves as the insertion slot location
        self._data = b''
        # self._data = self.setup_column(col_type)
        self._nulls = bytearray()                        # If nullable
        self._nvarchar_lengths = array.array(str('i'))        # length column for nVarchar


    def reset_data(self):
        ''' Empty all data related content after a sucessful flush'''
        
        self._index = -1      # Number of values inserted, serves as the insertion slot location
        self._data = b''
        # self._data = self.setup_column(col_type)
        self._nulls = bytearray()                           # If nullable
        self._nvarchar_lengths = array.array(str('i'))        # length column for nVarchar


    def set_add_val_null(self):
        ''' Set the appropriate binarization and addition function at initiation time'''     
        
        length = self.varchar_size  
        type_code = packing_column_codes[self.col_type]  # [u'ftInt', True, 4]
        varchar_typecode = '{}s'.format(length)

        pack = struct.pack

        if self.col_type == 'ftVarchar':

            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    self._nulls.append(0)
                    self._data += val.encode('utf-8')[:length].ljust(length, b' ')

                def add_null():
                    self._nulls.append(1)
                    self._data += b''.ljust(length, b' ')
            else:
                def add_val(val):
                    self._data += val.encode('utf-8')[:length].ljust(length, b' ')

        elif self.col_type == 'ftBlob':   
            
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    try:
                        encoded_val = val.encode('utf-8')[:length] #.ljust(length, b' ')
                    except:
                        encoded_val = unicode(val, 'utf-8').encode('utf-8')[:length] #.ljust(length, b' ')
                    self._nvarchar_lengths.append(len(encoded_val))
                    # print  (len(encoded_val))   #dbg
                    self._nulls.append(0)
                    self._data += encoded_val
                    # self._data += val.encode('utf-8')[:length].ljust(length, b' ')  #Py3

                def add_null():
                    self._nvarchar_lengths.append(len(''))
                    self._nulls.append(1)
                    self._data += b''.encode('utf-8')[:length]   #.ljust(length, b' ')
                    # self._data += val.encode('utf-8')[:length].ljust(length, b' ')   #Py3
            else:
                def add_val(val):
                    encoded_val = unicode(val, 'utf-8').encode('utf-8')[:length]  #.ljust(length, b' ')
                    self._nvarchar_lengths.append(len(val.encode('utf-8'))) 
                    self._data += encoded_val
                    # self._data += val.encode('utf-8')[:length].ljust(length, b' ')   #Py3

        elif self.col_type == 'ftDate':
           
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTuple, 'Not a valid Date tuple')

                    self._nulls.append(0)
                    self._data += pack(str(type_code), dateparts_to_int(*val))

                def add_null():
                    self._nulls.append(1)
                    self._data += pack(str(type_code), dateparts_to_int(0, 0, 0))
            else:
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTuple, 'Not a valid Date tuple')

                    self._data += pack(str(type_code), dateparts_to_int(*val))

        elif self.col_type == 'ftDateTime': 
            
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTimeTuple, 'Not a valid Datetime tuple')

                    self._nulls.append(0)
                    self._data += pack(str(type_code), dtparts_to_long(*val))

                def add_null():
                    self._nulls.append(1)
                    self._data += pack(str(type_code), dtparts_to_long(0, 0, 0, 0, 0, 0))
            else:
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTimeTuple, 'Not a valid Datetime tuple')

                    self._data += pack(str(type_code), dtparts_to_long(*val))

        elif self.col_type == 'ftLong':
           
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    self._nulls.append(0)
                    self._data += pack(str(type_code), val)               

                def add_null():
                    self._nulls.append(1)
                    self._data += pack(str(type_code), 0)  
            else:
                def add_val(val):
                    self._data += pack(str(type_code), val)  
        
        elif self.col_type in ('ftBool', 'ftUByte', 'ftShort', 'ftInt', 'ftFloat', 'ftDouble'):
            # Non bigint numerical types
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    self._nulls.append(0)
                    # self._data += array.array(str(type_code), [val]).tostring()
                    self._data += pack(str(type_code), val)

                def add_null():
                    self._nulls.append(1)
                    # self._data += array.array(str(type_code), [0]).tostring()
                    self._data += pack(str(type_code), 0)

            else:
                def add_val(val):
                    # self._data += array.array(str(type_code), [val]).tostring()
                    self._data += pack(str(type_code), val)
      
        else:
            # Oh pew
            print ("That's some Douglas Adams shit")


        return (add_val, add_null) if self.nullable else (add_val, lambda: print("Can't add nulls to a non-nullable column"))


## Internal handler object
#  -----------------------

class SqreamConn(object):
    def __init__(self, username=None, password=None, database=None, host=None, port=None, clustered=False, timeout=15):
        self.get_nulls = self.get_nulls_py2 if MAJOR==2 else self.get_nulls_py3
        self._socket = None
        self._user = username
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._clustered = clustered
        self._timeout = timeout
        self._use_ssl = True

        # API related
        self._query_str = None  # The SQL string entered via statement_handle()
        self._query_data = []   # Keeps a reference of query results, in addition to what's returned to the Connector object
        self._batch = []   # Keeps columns of binarized data to be sent to sqream upon flush() / close()
        # self._meta = meta
        self._ordered_col_names = []   # Set at _prepare_statement() (move to _query_type_in()) or _query_type_out()
        self._col_indices = {}
        self._index = -1        # Index holder for get() / set() functions
        self._num_of_rows = -1
        self._row_size = 0

        # Number of rows after which a flush is performed
        self._row_threshold = 100            # Decided dynamically based on row size
        self._accumulated_threshold = 0      # The row at which the next flush will occur 
        self._rows_inserted = 0
        self._set_flags = [0]

    HEADER_LEN = 10

    def set_socket(self, sock):
        assert isinstance(sock, (object, socket))
        self._socket = sock

    def set_host(self, host):
        self._host = host

    def set_port(self, port):
        self._port = port

    def set_clustered(self, clustered):
        self._clustered = clustered

    def open_socket(self):
        try:
            self.set_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            self._socket.settimeout(self._timeout)
        except socket.error as err:
            self.set_socket(None)
            raise RuntimeError("Error from SQream: " + str(err))
        except:
            raise RuntimeError("Other error")
        else:
            if self._use_ssl:
                self.cloak_socket()

    ''' SSL interlude : 
        ssl.wrap_socket(), ssl.get_server_certificate(addr, ssl_version=PROTOCOL_SSLv23, ca_certs=None)
    '''

    def cloak_socket(self, sock = None):
        ''' Wrap a socket to make it an SSL socket'''

        try:
            self._socket = ssl.wrap_socket(sock or self._socket, ssl_version=ssl.PROTOCOL_TLSv1, ciphers="ADH-AES256-SHA")
        except:
            print ("Error wrapping socket")  # check what exception goes here

    
    def close_socket(self):
        if self._socket:
            try:
                self._socket.close()
                self.set_socket(None)
            except(socket.error, AttributeError):
                pass

    
    def open_connection(self, ip, port):
        
        self.set_host(ip)
        self.set_port(port)

        try:
            self._socket.connect((ip, port))
        except socket.error as err:
            if self._socket:
                self.close_connection()
            raise RuntimeError("Couldn't connect to SQream server - " + str(err))
        except:
            print("Other error upon open connection")

    
    def close_connection(self):
        self.exchange('{"closeConnection":"closeConnection"}')
        self.close_socket()

    
    def create_connection(self, ip, port):
        self.open_socket()
        
        self.open_connection(ip, port)

    @staticmethod
    def len2ind(lens):
        ind = []
        idx = 0
        for i in lens:
            idx += i
            ind.append(idx)
        return ind

    def bytes2val(self, col_type, column_data_row):
        if col_type != "ftVarchar":
            column_data_row = conv_data_type(col_type, column_data_row)
        else:
            column_data_row = column_data_row.replace(b'\x00', b'')
            column_data_row = column_data_row.rstrip()
        return column_data_row

    def readcolumnbytes(self, column_bytes):
        chunks = []
        bytes_rcvd = 0
        while bytes_rcvd < column_bytes:
            chunk = self.socket_recv(min(column_bytes - bytes_rcvd, DEFAULT_BUFFER_SIZE))
            if chunk == b'':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_rcvd += len(chunk)
        column_data = b''.join(chunks)
        return column_data

    @staticmethod
    def cmd2bytes(cmd_str, binary = False):
        ''' Packing command string to bytes and adding 10 bit header '''

        cmd_bytes_1 = bytearray([2])               # Protocol version

        if not binary:
            cmd_bytes_2 = bytearray([1])           # Vote 1 for text
            # cmd_bytes_4 = cmd_str.expandtabs(1).encode('ascii')
            cmd_bytes_4 = cmd_str.encode('ascii')
        else:    
            cmd_bytes_2 = bytearray([2])           # 2 for binary
            cmd_bytes_4 = cmd_str

        cmd_bytes_3 = pack('q', len(cmd_bytes_4))
        cmd_bytes = cmd_bytes_1 + cmd_bytes_2 + cmd_bytes_3 + cmd_bytes_4

        return cmd_bytes

    def socket_recv(self, param):
        try:
            data_recv = self._socket.recv(param)
            # TCP says recv will only read 'up to' param bytes, so keep filling buffer
            remainder = param - len(data_recv)
            while remainder > 0:
                data_recv += self._socket.recv(remainder)
                remainder = param - len(data_recv)
            if b'{"error"' in data_recv:
                raise RuntimeError("Error from SQream: " + repr(data_recv))
        except socket.error as err:
            self.close_connection()
            self.set_socket(None)
            raise RuntimeError("Error from SQream: " + str(err))
        except RuntimeError as e:
            raise RuntimeError(e)
        except:
            raise RuntimeError("Other error while receiving from socket")
        return data_recv

    def _get_msg(self):
        data_recv = self.socket_recv(self.HEADER_LEN)
        ver_num = unpack('b', bytearray([data_recv[0]]))[0]
        if ver_num not in (4,5):        # Expecting 4 or 5        
            raise RuntimeError(
                "SQream protocol version mismatch. Expecting " + str(PROTOCOL_VERSION) + ", but got " + str(
                    ver_num) + ". Is this a newer/older SQream server?")
        val_len = unpack('q', data_recv[2:])[0]
        data_recv = self.socket_recv(val_len)
        return data_recv

    def exchange(self, cmd_str, close=False, binary = False):
        # If close=True, then do not expect to read anything back
        cmd_bytes = self.cmd2bytes(cmd_str, binary)
        try:
            self._socket.settimeout(None)
            self._socket.send(cmd_bytes)
        except socket.error as err:
            self.close_connection()
            self.set_socket(None)
            raise RuntimeError("Error from SQream: " + str(err))
        if close is False:
            return self._get_msg()
        else:
            return

    def connect(self, database, username, password):
        if self._clustered is False:
            self.connect_unclustered(database, username, password)
        else:
            self.connect_clustered(database, username, password)

    # Voodoo, check what this does
    def connect_clustered(self, database, username, password):
        read_len_raw = self.socket_recv(4)  # Read 4 bytes to find length of how much to read
        read_len = unpack('i', read_len_raw)[0]
        if read_len > 15 or read_len < 7:
            raise RuntimeError("Clustered connection requires a length of between 7 and 15, but I got " + str(
                read_len) + ". Perhaps this connection should be unclustered?")
        # Read the number of bytes, which is the IP in string format
        ip_addr = self.socket_recv(read_len)
        # Now read port
        port_raw = self.socket_recv(4)
        port = unpack('i', port_raw)[0]
        if port < 1000 or port > 65535:
            raise RuntimeError("Port out of bounds (1000 - 65535): " + str(port) + ".")
        self.close_connection()
        self.set_host(ip_addr)
        self.set_port(port)
        self.set_clustered(False)
        self.create_connection(ip_addr, port)
        self.connect_unclustered(database, username, password)

    def connect_unclustered(self, database, username, password):
        cmd_str = """{{"connectDatabase":"{0}","password":"{1}","username":"{2}"}}""".format(
            database.replace('"', '\\"')
            , password.replace('"', '\\"')
            , username.replace('"', '\\"'))
        self.exchange(cmd_str)
    
    # Reading bytes in Python 2 and 3
    def get_nulls_py2(self,column_data):
        return map(lambda c: unpack('b', bytes(c))[0], column_data)
        # or return [ord(c) for c in column_data]
   
    def get_nulls_py3(self,column_data):
        return [c for c in column_data]

    
    def _get_table_metadata(self, table_name):
        # An internal version for getting table metadata, using execute_query() directly  

        # Get it back
        self._prepare_statement("select * from sqream_catalog.columns where table_name = '{}'".format(table_name))
        self._execute()
        self._fetch_all()
        self._close_statement()    # needed?
        meta = self._cols_to_rows()
        ordered_col_names = [val for val, gr in groupby([item[5].split('@')[0] for item in meta])]
        column_meta = dict.fromkeys(ordered_col_names)
        nool = False
        row_size = 0
        for item in meta:
            name, null_or_val = item[5].split('@')[0:2]
            # Skipping separate references to null columns, which precede the real columns
            if null_or_val == 'null':
                nool = True
                continue

            # Actual columns get here. If previous one was a null column, nool will remember
            coltype, nullable, length = sqream_typenames_to_codes[item[6]], nool, item[7]
            column_meta[name] = [coltype, nullable, length]
            row_size+= length
            nool = False  # '''
        
        return column_meta, ordered_col_names    # , row_size

        # return list(map(lambda c: (c.get_column_name(), c.get_type_name(), c._nullable, c.get_type_size()), columns))
        # list(map(lambda c: c.get_type_name(), cols))
        # column_meta = dict(zip(self.cols_names(), zip(self.cols_types(), self.cols_nullable(), self.cols_type_sizes())))

    def _get_error(self):
        res = self._get_msg()
        if "error" in res: 
            return res

    def _prepare_statement(self, query_str, meta = None, ordered_col_names = None, auto = True):
        ''' 
        Called by Connector.prepare(). If contains 'insert into' and '?', comes complementary
        with table metadata and the ordered column names  '''            

        # Protocol check
        if PROTOCOL_VERSION == 5:    # remove if/once everyone's on 5
            # getStatementId is new for SQream protocol version 5
            cmd_str = '{"getStatementId" : "getStatementId"}'
            res_id = self.exchange(cmd_str)

        # Send command and validate response from SQream
        cmd_str = """{{"prepareStatement":"{0}","chunkSize":{1}}}""".format(query_str.replace('"', '\\"'),
                                                                            str(DEFAULT_NETWORK_CHUNKSIZE))        
        res = self.exchange(cmd_str)
        if "statementPrepared" in res: 
            
            # Remove previous meta/data
            self._query_data = []
            self._batch = [] 
            self._meta = [] 
            self._set_flags = [0]              # flags columns that were set. gets cleaned by next_row and possibly                 
            self._ordered_col_names = []
            self._col_indices = {}
            self._index = -1
            self._num_of_rows = -1  
            self._row_size = 0
            self._row_threshold = 0
            self._accumulated_threshold = 0 
            self._set_or_get = None    
            # self._row_threshold = 100       # add dynamic calculation 

            # If an insert statement, we would've recieved metadata
            if meta:
                # Getting column names off our query string
                query_words = query_str.split()
                table_name = query_words[2]   # self?
                if query_words[3] == '(':
                    cols_foshow = query_str.replace(')', '(').split('(')[1].split(',')
                    try:
                        # Cols can given as names or indices
                        int(cols_foshow[0])
                    except:
                        # Cols given as names
                        cols_foshow = [col.strip() for col in cols_foshow]

                    else:
                        # Cols given as indices
                        cols_foshow = [ordered_col_names[index] for index in cols_foshow]
                else:
                    # No specific column ids given, take all columns
                    cols_foshow = ordered_col_names
                
                self._ordered_col_names = cols_foshow  # When we jump to _query_type_in(), this will have the column names for insertion
                self._meta = meta
                self._set_flags = [0]*len(self._ordered_col_names)

                # Auto mode - queryTypeIn/Out are not part of the API, called automatically by PrepareStatement
                query_type_in = self._query_type_in() if auto else None

            else:
                query_type_out = self._query_type_out() if auto else None
        return res

    
    # Used by _prepare_statement()
    def _query_type_in(self):
        ''' Retrun value example: 
        {"queryType":[{"nullable":false,"type":["ftVarchar",50,0]},{"nullable":false,"type":["ftVarchar",50,0]},
        {"nullable":false,"type":["ftDateTime",8,0]},{"nullable":false,"type":["ftVarchar",112,0]},'''
        

        out = self.exchange('{"queryTypeIn":"queryTypeIn"}')
        query_type_in = json.loads(out.decode('utf-8'))

        # Calaculate row size and flush threshold if applicable
        # print ([val['type'][1] for val in query_type_in['queryType']])
        self._row_size = sum(val['type'][1] for val in query_type_in['queryType'])
        self._row_threshold = FLUSH_SIZE // self._row_size if self._row_size else 0
        
        '''
        # Setup _batch columns for insert statement if applicable
        for idx, name in enumerate(self._ordered_col_names):
            # coltype, nullable, varchar_length = self._meta[name]
            # col = BinaryColumn(self._meta[name])
            # print (self._meta[name])
            print (self._meta[name])
            self._batch.append(BinaryColumn(*self._meta[name]))
            self._col_indices[name] = idx 

        return query_type_in'''
    
        for col in query_type_in['queryType']:
            self._batch.append(BinaryColumn(col['type'][0], col['nullable'], col['type'][1]))
        
        
        return query_type_in
    

    # Used by _prepare_statement()
    def _query_type_out(self):
        exch = self.exchange('{"queryTypeOut" : "queryTypeOut"}')
        query_type_out = json.loads(exch.decode('utf-8'))
        

        if query_type_out["queryTypeNamed"]: 
            # Setting up column metadata  
            for idx, col_type in enumerate(query_type_out['queryTypeNamed']):
                # Column sizes (row number) is updated at fetch() time
                sq_col = SqreamColumn()
                sq_col._type_name = query_type_out['queryTypeNamed'][idx]['type'][0]
                sq_col._type_size = query_type_out['queryTypeNamed'][idx]['type'][1]
                sq_col._column_name = query_type_out['queryTypeNamed'][idx]['name']
                #def _column_name = (self, column_name): self._column_name = column_name
                self._ordered_col_names.append(sq_col._column_name) 
                # To allow quick switching between column names and locations in the table 
                self._col_indices[sq_col._column_name] = idx   
                sq_col._isTrueVarChar = query_type_out['queryTypeNamed'][idx]['isTrueVarChar']
                sq_col._nullable = query_type_out['queryTypeNamed'][idx]['nullable']
                self._query_data.append(sq_col)

        return query_type_out

    
    def _fetch_all(self, discard = False):
        ''' Perform fetching untill all available date is retrieved'''
        res = True
        if discard:
            while res:
                res=self._fetch_data()
                # flush retrieved data
                col._column_data = []
        else:
            while res:
                res=self._fetch_data()

           
    def _fetch_data(self):
        exch = self.exchange('{"fetch" : "fetch"}')
        res = json.loads(exch.decode('utf-8'))        

        if res["rows"] == 0:
            # No content to read - All data has been assigned to our column objects
            # self._index = self._query_data[0]
            self._num_of_rows = self._num_of_rows if self._num_of_rows else 0
            return False
        
        # Reading and parsing data     
        ignored_header = self.socket_recv(self.HEADER_LEN) # Read to ignore header, which is irrelevant here 
        col_size = list()
        idx_first = 0
        idx_last = 1
        # Metadata store + how many columns to read ([val], [len,blob], [null,val], [null,len,blob])
        for col in self._query_data:   # self._query_data updated by self.query_type_out()
            if col._isTrueVarChar:
                idx_last += 1
            if col._nullable:
                idx_last += 1
            col._column_size = res["colSzs"][idx_first:idx_last]
            idx_first = idx_last
            idx_last += 1

            if col._isTrueVarChar == False and col._nullable == False:
                column_data = self.readcolumnbytes(col._column_size[0])  # , col.get_type_size())
                column_data = [column_data[i:i + col._type_size] for i in
                               range(0, col._column_size[0], col._type_size)]
                column_data = list(map(lambda c: self.bytes2val(col._type_name, c), column_data))

            elif col._isTrueVarChar == False and col._nullable == True:
                column_data = self.readcolumnbytes(col._column_size[0])
                is_null = self.get_nulls(column_data)
                column_data = self.readcolumnbytes(col._column_size[1])  # ,col.get_type_size(), None, is_null)
                column_data = [column_data[i:i + col._type_size] for i in
                               range(0, col._column_size[1], col._type_size)]
                column_data = [self.bytes2val(col._type_name,column_data[idx]) if elem == 0 else u"\\N" for idx, elem in enumerate(is_null)]

            elif col._isTrueVarChar == True and col._nullable == False:
                column_data = self.readcolumnbytes(col._column_size[0])
                column_data = [column_data[i:i + 4] for i in range(0, col._column_size[0], 4)]
                nvarchar_lens = map(lambda c: unpack('i', c)[0], column_data)
                nvarchar_inds = self.len2ind(nvarchar_lens)
                column_data = self.readcolumnbytes(col._column_size[1])  # , None, nvarchar_inds[:-1])
                column_data = [column_data[i:j] for i, j in
                               zip([0] + nvarchar_inds[:-1], nvarchar_inds[:-1] + [None])]

            elif col._isTrueVarChar == True and col._nullable == True:
                column_data = self.readcolumnbytes(col._column_size[0])
                is_null = self.get_nulls(column_data)
                column_data = self.readcolumnbytes(col._column_size[1])
                column_data = [column_data[i:i + 4] for i in range(0, col._column_size[1], 4)]
                nvarchar_lens = map(lambda c: unpack('i', c)[0], column_data)
                nvarchar_inds = self.len2ind(nvarchar_lens)
                column_data = self.readcolumnbytes(col._column_size[2])
                column_data = [column_data[i:j] if k == 0 else u"\\N" for i, j, k in
                               zip([0] + nvarchar_inds[:-1], nvarchar_inds[:-1] + [None], is_null)]
            else:
                raise RuntimeError("Column data encountered malformed column during fetch")

            col._column_data += column_data

        # Update boundary parameter for _next_row() 
        self._num_of_rows = self._query_data[0]._column_size[0]  # 1 is fetched column size
        # print (self._num_of_rows)  #dbg
        return True  # No. of rows recieved was not 0


    def _execute(self):
          self.exchange('{"execute" : "execute"}')


    def _close_statement(self):
        # '''
        if 'add flush condition':  # flush() doesn't fire blanks so it's keewl
            # self._index-=1
            self._flush()  #'''
        
        self.exchange('{"closeStatement":"closeStatement"}')  #'''


    def _get_item(self, col_index_or_name, col_type, null_check = False):
        ''' Retrieves an item from the respective column using the set index. 
            index is modified by next_row() (increase only for a given query) '''

        # Convert column name to index
        if type(col_index_or_name) in (str, unicode):
            try:
                col_index = self._col_indices[col_index_or_name]
            except:
                if self._col_indices:
                    print ("Bad column name")
                else:
                    # if _col_indices is an empty list
                    print ("No select statement issued")
                return 
        else:
            col_index = col_index_or_name -1

        # Trying to get the column
        try:
            col = self._query_data[col_index]
        except IndexError: 
            print("Inexistent column index. Number of columns is " + str(len(self._query_data)))
            return "Inexistent column index. Number of columns is " + str(len(self._query_data))
        except:
            print("No select statement executed")
          
        # Column acquired, check if correct type if this is a get_() request
        if not null_check:
            actual_type = col._type_name
            if not col_type == actual_type: # and not (col_type, actual_type) == ('ftFloat', 'ftDouble'):
                # print (col_type)
                announce(WrongGetStatement, "Incorrect type. Column of type {}, trying to get {}".format(actual_type, col_type))

        # Acquired and verified. Here cometh thy money
        try:
            # print( col._column_data)   #dbg
            res = col._column_data[self._index]
        except IndexError:
            print ("No more rows. Last value was: ", col._column_data[self._index -1])

        return res if not null_check else True if res is None else False

    
    def _set_item(self, col_index_or_name, val, col_type = None, set_null = False):
        ''' set_stuff() redirect here with appropriate type '''
        # Null check
        if val == None and not set_null:
            print ('Null values are to be inserted via set_null()')
            return

        # Convert column name to index, same as in _get_item()
        if type(col_index_or_name) in (str, unicode):
            #OMG we totally don't support
            print ("Setting values by column name not supported")
            return
            
            # When we decide we do support insertion by column name
            try:
                col_index = self._col_indices[col_index_or_name]
            except:
                if self._col_indices:
                    print ("Bad column name")
                else:
                    # if _col_indices is an empty list
                    print ("No insert statement issued")
                return 
        else:
            col_index = col_index_or_name - 1

        # Trying to get the column from the batch           
        try:
            col = self._batch[col_index]
        except IndexError: 
            announce(IndexError, ("Inexistent column index. Number of columns is " + str(len(self._batch))))
            return   
        except:
            print("No insert statement executed")
            return
        
        # Column acquired, type check on non null inserts
        if col_type != col.col_type and not set_null: 
            # print (col_type, col.col_type)  #dbg
            announce(WrongSetStatement, "Incorrect type. Column of type {}, inserted {}".format(col.col_type, col_type))        

        # Inserting value
        if val != None: 
            # Acquired and verified. Here cometh thy money
            col.add_val(val)
            self._set_flags[col_index] = 1
            
        # Inserting null
        else:
            col.add_null()
            self._set_flags[col_index] = 1


    def _flush(self):   # <== check if works on fumes
        ''' Gather a binary chunk from get() statements and send to SQream'''
        # print (self._row_threshold)   #dbg
        chunk =  b''.join((str(col._nulls) + col._nvarchar_lengths.tostring() + str(col._data) for col in self._batch))

        # print ([(str(col._nulls),  col._nvarchar_lengths.tostring(),  str(col._data)) for col in self._batch])  #dbg
        # print ([(col._nvarchar_lengths.tostring()) for col in self._batch])  #dbg
        # print(len(chunk))
        # print (repr(chunk))
        if chunk:
            # Insert sequence - put command, binary chunk
            put_cmd = '{{"put":{}}}'.format(self._index + 1)
            self.exchange(put_cmd, True)
            batch_insert_res = self.exchange(chunk, False, True)   # This one is a binary piece, execute and cmd2bytes were modified 
            
            # Truncate binary column upon success
            if batch_insert_res == '{"putted":"putted"}':
                map(lambda col: col.reset_data(), self._batch)

    
    def _cols_to_rows(self, cols = None):
        ''' Transpose dem columns'''

        cols = cols or self._query_data
        return zip(*(col._column_data for col in cols))      

    
    '''
    # Currently implemented independently at Connector class level   #
    def _next_row(self):
        # Money function - Take care of all background hustle and proceed to a
        # new row if applicable. Currently implemented explicitly in the Connector class
        

        # Get / select operation
        if self._query_data:
            pass
        # Set / insert operation
        elif self._batch:
            pass
        # Denied
        else:
            print ("No select or insert operation executed")
    #'''

## This class should be used to create a connection
#  ------------------------------------------------

api_to_sqream = {'bool':    'ftBool',   
                'ubyte' :    'ftUByte',
                'short' :   'ftShort',
                'int' :     'ftInt',
                'long' :    'ftLong',
                'float' :   'ftFloat',             
                'double' :  'ftDouble',
                'date' :    'ftDate',
                'datetime' :'ftDateTime',
                'string' :  'ftVarchar',
                'nvarchar' : 'ftBlob',
                }


## User facing API object 
# -----------------------

class Connector(object):
    
    def __init__(self):
        # Store the connection
        self._sc = None
        # Store the columns from the result
        self._cols = None
        self._query = None
        
        # For get() API statements
        self._cols_names = None       
        self._col_dict = None                

        ## Aliases for testing convenience
        self.get_varchar = self.get_string
        self.get_tinyint = self.get_ubyte
        self.get_smallint = self.get_short
        self.get_bigint = self.get_long
        self.get_real = self.get_float
        
        self.set_varchar = self.set_string
        self.set_tinyint = self.set_ubyte
        self.set_smallint = self.set_short
        self.set_bigint = self.set_long
        self.set_real = self.set_float

        self.connect_database = self.connect
    

    def connect(self, host, port, database, user, password, clustered, timeout):

        sqream_ssl_port = 5100
        # No connection yet, create a new one
        if self._sc is None:
            try:
                nsc = SqreamConn(clustered=clustered, timeout=timeout)
                nsc._use_ssl = True if port == sqream_ssl_port else False
                nsc.create_connection(host, port)
                nsc.connect(database, user, password)
                self._sc = nsc
            except RuntimeError as e:
                raise RuntimeError(e)
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise
            return self._sc
        else:
            raise RuntimeError(
                'Connection already exists. You must close the current connection before creating a new one')


    def close_connection(self):
        # Close existing connection, if it exists
        if self._sc is None:
            return
        else:
            self._sc.close_connection()
            self._sc = None

                

    #                                   API be here                               #
    #  ------------------------------------------------------------------------   #                


    ## General statements
    #  ------------------

    # execute = lambda: self._sc.exchange('{"execute" : "execute"}')
    # close = lambda: self._sc.exchange('{"closeStatement":"closeStatement"}') 
    
    def execute(self):
      self._sc._execute()
 
    def fetch_data(self):
        self._sc._fetch_data()

    def fetch_discard(self):
        self._sc._fetch_all(True)

    def close(self):
        '''close statement'''

        self._sc._close_statement()
    '''
    def close_connection(self):
        # Same name as the inside function
        self._sc.close_connection() # '''

    def statement_handle(query_str):
        
        if query_str not in (str, unicode):
            print ('SQL statement should be a string')
            return

        self._sc._query_str = query_str
    

    def prepare(self, query_str = None):
        ''' Prepare statement'''

        query_str = query_str if query_str else self._sc._query_str

        if  query_str is None:
            print ('No statement passed')   # raise # fixes
            return
        else:
            query_str = query_str.replace('\n', ' ').replace('\r', '') 
        
        # Prepping metadata in case this is a network insert statement
        if query_str.lower().startswith('insert into') and '?' in query_str:
            # This might not be a valid statement, but getting metadata just in case
            query_words = query_str.split()
            table_name = query_str.split()[2]  #query_words[2]

            # _get_table_metadata() uses _prepare_statement() itself. buster buster
            meta, ordered_col_names = self._sc._get_table_metadata(table_name)
            self._sc._prepare_statement(query_str, meta, ordered_col_names)
        else:
            self._sc._prepare_statement(query_str)

    def get_error(self):
        return self._sc._get_error();

    
    def get_connection(self):
        ''' This returns the internal connection object  #itsintheAPI '''
            
        if not self._sc:
            print ("No open connection")
        else:
            return self._sc


    def next_row(self, _more_to_fetch=[True]):  # more_to_fetch keeps track of fetches, shouldn't be passed #hackyhorace

        # retval = False
        # Select scenario, bring another chunk of data if relevant
        if self._sc._query_data and _more_to_fetch[0]:
            # Optimize to fetch when needed and/or a separate thread
            _more_to_fetch[0] = self._sc._fetch_data()     # Would get called an extra time

        # Select scenario, index advancement if valid
        if self._sc._num_of_rows > 0 and self._sc._index +1 <= self._sc._num_of_rows:   #_num_of_rows decided after a fetch() 
            self._sc._index += 1
            # print(self._sc._index, self._sc._num_of_rows)
            # retval = True
            return True

        # Insert scenario
        if self._sc._num_of_rows < 0:

            # Have all columns been set
            # print (self._sc._set_flags, self._sc._ordered_col_names, sum(self._sc._set_flags), len(self._sc._ordered_col_names))    # dbg
            if sum(self._sc._set_flags) < len(self._sc._ordered_col_names) and len(self._sc._ordered_col_names):
                # print ('debug': self._sc._set_flags, self._sc._ordered_col_names)  #dbg 
                announce(RowFillException, "Not all columns have been set")
                return False
            else:
                # This gets nullified every row
                self._sc._set_flags = [0]*len(self._sc._ordered_col_names)

            # Check flush condition
            # '''
            if self._sc._index > self._sc._row_threshold:
                # print ("row_threshold:", self._sc._row_threshold, "index:", self._sc._index)   #dbg
                self._sc._index += 1
                self._sc._flush()           
                self._sc._index = -1  # Nullifying index count 
                return True
                # self._sc._flush_threshold += self._sc._row_threshold     #'''

            self._sc._index += 1    
            return True

        # check some length indicator for get(), else check buffer stuff 
      
        return False


    def flush(self):
        self._sc._flush()

 
    ## Get() API statements
    #  --------------------
    
    def is_null(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftBool', True)       # api_to_sqream('bool')


    def get_bool(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftBool')       # api_to_sqream('bool')


    def get_string(self, col_index_or_name):
    
        return self._sc._get_item(col_index_or_name, 'ftVarchar')      # api_to_sqream('string')


    def get_nvarchar(self, col_index_or_name):     

        return self._sc._get_item(col_index_or_name, 'ftBlob')         # api_to_sqream('nvarchar')

    
    def get_ubyte(self, col_index_or_name):

       return self._sc._get_item(col_index_or_name, 'ftUByte')


    def get_int(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftInt')


    def get_short(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftShort')


    def get_float(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftFloat')


    def get_double(self, col_index_or_name):

       return self._sc._get_item(col_index_or_name, 'ftDouble')


    def get_long(self, col_index_or_name):

       return self._sc._get_item(col_index_or_name, 'ftLong')


    def get_date(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftDate')


    def get_datetime(self, col_index_or_name):

        return self._sc._get_item(col_index_or_name, 'ftDateTime')


    ## Set() API statements
    #  --------------------

     
    def set_null(self, col_index_or_name):

        # col_index_or_name, val, col_type = None, set_null = False)
        return self._sc._set_item(col_index_or_name, None, None, True)

 
    def set_bool(self, col_index_or_name, val): 
        # '''
        if val not in (0,1):  # supports True and False as well for pyarrow  # if val is not 0 and val is not 1: 
            announce(BadTypeForSetFunction, 'Expecting Boolean value but got {}'.format(val))
        
        return self._sc._set_item(col_index_or_name, val, 'ftBool')       # api_to_sqream('bool')


    def set_string(self, col_index_or_name, val):
             
        if type(val) not in (str, unicode):
            announce(BadTypeForSetFunction, 'Expecting varchar value but got {}'.format(val))

        return self._sc._set_item(col_index_or_name, val, 'ftVarchar')      # api_to_sqream('string')

    
    def set_nvarchar(self, col_index_or_name, val):     

        if type(val) not in (str, unicode):
            announce(BadTypeForSetFunction, 'Expecting nvarchar value but got {}'.format(val))
            
        return self._sc._set_item(col_index_or_name, val, 'ftBlob')         # api_to_sqream('nvarchar')

    
    def set_ubyte(self, col_index_or_name, val):
        
        if type(val) != int or (type(val) == int and not tinyint_range[0] <= val <= tinyint_range[1]):  
            announce(BadTypeForSetFunction, 'Expecting tinyint value but got {}'.format(val))
            
        return self._sc._set_item(col_index_or_name, val, 'ftUByte')


    def set_short(self, col_index_or_name, val):
        
        if type(val) != int or (type(val) == int and not smallint_range[0] <= val <= smallint_range[1]):  
           announce(BadTypeForSetFunction, 'Expecting smallint value but got {}'.format(val))
            
        return self._sc._set_item(col_index_or_name, val, 'ftShort')


    def set_int(self, col_index_or_name, val):
        
        if type(val) != int or (type(val) == int and not int_range[0] <= val <= int_range[1]):  
            announce(BadTypeForSetFunction, 'Expecting int value but got {}'.format(val))

        return self._sc._set_item(col_index_or_name, val, 'ftInt')


    def set_long(self, col_index_or_name, val):
       
        if type(val) != long: # or (type(val) == long and not bigint_range[0] <= val <= bigint_range[1]):  
            if type(val) != int:
                announce(BadTypeForSetFunction, 'Expecting long value but got {}'.format(val))
            else: 
                val = long(val)
        
        return self._sc._set_item(col_index_or_name, val, 'ftLong')


    def set_float(self, col_index_or_name, val):

        # float but no float - Python's float type is double precision by default
        # Also, inf, nan and friends can't be tested as floats
        if type(val) != float or (type(val) == float and not float_range[0] <= abs(val) <= float_range[1]):  
            if val not in (float('-0'), float('+0'), float('inf'), float('-inf'), float('nan')) or val is True or val is False:
                # print (val, type (val))  #dbg   
                announce(BadTypeForSetFunction, 'Expecting real(=float) value but got {}'.format(val))
          
        return self._sc._set_item(col_index_or_name, val, 'ftFloat')


    def set_double(self, col_index_or_name, val):

        if type(val) != float:
            announce(BadTypeForSetFunction, 'Expecting double value but got {}'.format(val))
          
        return self._sc._set_item(col_index_or_name, val, 'ftDouble')


    def set_date(self, col_index_or_name, val):
        
        if type(val) != tuple:
            announce(BadTypeForSetFunction, 'Expecting date value but got {}'.format(val))

        return self._sc._set_item(col_index_or_name, val, 'ftDate')
        

    def set_datetime(self, col_index_or_name, val):
        
        if type(val) != tuple:
            announce(BadTypeForSetFunction, 'Expecting datetime value but got {}'.format(val))

        return self._sc._set_item(col_index_or_name, val, 'ftDateTime')


if __name__ == '__main__':
    print ("This file is to be used as a module\nSee example usage at the top of the file\n")
    print ("SQream Python driver version", PYSQREAM_VERSION, '\n')
