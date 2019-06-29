from __future__ import absolute_import, division, print_function, unicode_literals

PYSQREAM_VERSION = "2.1.3a1"
"""
Python2.7/3.6 connector for SQream DB

Usage example:

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
try:
    from itertools import izip   # python 2
except:
    izip = zip                   # python 3

from multiprocessing import Process, Pipe #, Queue
from decimal import Decimal
from operator import add
from threading import Event
from collections import namedtuple


# Default constants
PROTOCOL_VERSION = 6         
SERVER_PROTOCOL_VERSION = 6       # Update to server's version when BACK_COMPAT is turned on
BACK_COMPAT = True
SUPPORTED_VERSIONS = (4, 5, 6) if BACK_COMPAT else (6,)
DEFAULT_BUFFER_SIZE = 4096    #65536 
DEFAULT_NETWORK_CHUNKSIZE = 10000  
FLUSH_SIZE = 100000     # Default flush size for set() operations

VER = sys.version_info
MAJOR = VER[0]

if MAJOR == 3:
    unicode  = str    # to allow dual compatibility
    long = int
    
def version_info():
    info = "PySqreamConn version: {}\nSQream Protocol version: {}".format(PYSQREAM_VERSION, PROTOCOL_VERSION)
    return info

## API exception types
#  ------------------------
class BadTypeForSetFunction(Exception): pass

class RowFillException(Exception):      pass

class ValueRangeException(Exception):   pass  # not in use

class FaultyDateTuple(Exception):       pass

class FaultyDateTimeTuple(Exception):   pass

class WrongGetStatement(Exception):     pass

class WrongSetStatement(Exception):     pass

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


statement_type = {'DML', 'INSERT', 'SELECT'}

## Metadata related
#  ----------------
sqream_type_id =   {'ftBool':     'Bool',   
                    'ftUByte':    'Tinyint',
                    'ftShort':    'Smallint',
                    'ftInt':      'Int',  
                    'ftLong':     'Bigint',
                    'ftFloat':    'Real',                 
                    'ftDouble':   'Float',
                    'ftDate':     'Date',
                    'ftDateTime': 'DateTime',
                    'ftVarchar':  'Varchar',
                    'ftBlob':     'NVarchar'
                    }


SqreamType = namedtuple('SqreamType', ['tid', 'size'])
# tid is of type sqream_type_id, size is size of one item of this type

class ColumnMetadata(object):

    def __init__(self, name, type_name, type_size, is_nullable, is_tvc):
        self.name = name
        self.type = SqreamType(sqream_type_id[type_name], type_size)
        self.is_nullable = is_nullable
        self.is_tvc = is_tvc



## Batch class for per-record aggregation
#  ---------------------------------------

class Column:
    ''' Generates a binary to be network inserted to SQream.
        Also holds complementary null and length columns. '''
    
    def __init__(self, col_type, nullable = True, varchar_size = 0):

        self.col_type = col_type
        self.nullable = nullable
        self.varchar_size = varchar_size
        self.add_val, self.add_null = self.set_add_val_null()
        
        self.data = []
        self.encoded_data = b''
        self._nulls = bytearray()                        # If nullable
        self._nvarchar_lengths = array.array(str('i'))        # length column for nVarchar


    def reset_data(self):
        ''' Empty all data related content after a sucessful flush'''
        
        self.encoded_data = b''
        # self.encoded_data = self.setup_column(col_type)
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
                    self.encoded_data += val.encode('utf-8')[:length].ljust(length, b' ')

                def add_null():
                    self._nulls.append(1)
                    self.encoded_data += b''.ljust(length, b' ')
            else:
                def add_val(val):
                    self.encoded_data += val.encode('utf-8')[:length].ljust(length, b' ')

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
                    self.encoded_data += encoded_val
                    # self.encoded_data += val.encode('utf-8')[:length].ljust(length, b' ')  #Py3

                def add_null():
                    self._nvarchar_lengths.append(len(''))
                    self._nulls.append(1)
                    self.encoded_data += b''[:length]   #.ljust(length, b' ')
                    # self.encoded_data += val.encode('utf-8')[:length].ljust(length, b' ')   #Py3
            else:
                def add_val(val):
                    encoded_val = unicode(val, 'utf-8').encode('utf-8')[:length]  #.ljust(length, b' ')
                    self._nvarchar_lengths.append(len(val.encode('utf-8'))) 
                    self.encoded_data += encoded_val
                    # self.encoded_data += val.encode('utf-8')[:length].ljust(length, b' ')   #Py3

        elif self.col_type == 'ftDate':
           
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTuple, 'Not a valid Date tuple')

                    self._nulls.append(0)
                    self.encoded_data += pack(str(type_code), dateparts_to_int(*val))

                def add_null():
                    self._nulls.append(1)
                    self.encoded_data += pack(str(type_code), dateparts_to_int(0, 0, 0))
            else:
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTuple, 'Not a valid Date tuple')

                    self.encoded_data += pack(str(type_code), dateparts_to_int(*val))

        elif self.col_type == 'ftDateTime': 
            
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTimeTuple, 'Not a valid Datetime tuple')

                    self._nulls.append(0)
                    self.encoded_data += pack(str(type_code), dtparts_to_long(*val))

                def add_null():
                    self._nulls.append(1)
                    self.encoded_data += pack(str(type_code), dtparts_to_long(0, 0, 0, 0, 0, 0))
            else:
                def add_val(val):
                    if not validate_datetime_tuple(val):
                        announce(FaultyDateTimeTuple, 'Not a valid Datetime tuple')

                    self.encoded_data += pack(str(type_code), dtparts_to_long(*val))

        elif self.col_type == 'ftLong':
           
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    self._nulls.append(0)
                    self.encoded_data += pack(str(type_code), val)               

                def add_null():
                    self._nulls.append(1)
                    self.encoded_data += pack(str(type_code), 0)  
            else:
                def add_val(val):
                    self.encoded_data += pack(str(type_code), val)  
        
        elif self.col_type in ('ftBool', 'ftUByte', 'ftShort', 'ftInt', 'ftFloat', 'ftDouble'):
            # Non bigint numerical types
            if self.nullable:
                # add_val = lambda val: val.encode('utf-8')[:length].ljust(length, b' ')      
                def add_val(val):
                    self._nulls.append(0)
                    # self.encoded_data += array.array(str(type_code), [val]).tostring()
                    self.encoded_data += pack(str(type_code), val)

                def add_null():
                    self._nulls.append(1)
                    # self.encoded_data += array.array(str(type_code), [0]).tostring()
                    self.encoded_data += pack(str(type_code), 0)

            else:
                def add_val(val):
                    # self.encoded_data += array.array(str(type_code), [val]).tostring()
                    self.encoded_data += pack(str(type_code), val)
      
        else:
            # Oh pew
            print ("That's some Douglas Adams shit")


        return (add_val, add_null) if self.nullable else (add_val, lambda: print("Can't add nulls to a non-nullable column"))


## Socketses
#  ---------

def _recieve(byte_num, sock):
    ''' Read a specific amount of bytes from socket'''

    data = bytearray(byte_num)
    idx = 0
    
    while byte_num >0:
        # Get whatever the socket gives and put it inside the bytearray
        recieved = sock.recv(byte_num)
        size = len(recieved)      
        data[idx: idx + size] = recieved

        # Update bytearray index and remaining amount to be fetched from socket
        byte_num -= size
        idx += size

    return data


## SQream related interaction
#  --------------------------

def _get_message_header(data_length, is_text_msg = True, protocol_version = SERVER_PROTOCOL_VERSION):
    ''' Generate SQream's 10 byte header prepended to any message '''
    
    return pack('bb', protocol_version, 1 if is_text_msg else 2) + pack('q', data_length) 


## Internal handler object
#  -----------------------

class SqreamConn(object):
    def __init__(self, username=None, password=None, database=None, host=None, port=None, clustered=False, timeout=15):
        self.get_nulls = self.get_nulls_py2 if MAJOR==2 else self.get_nulls_py3
        self._user = username
        self._password = password
        self._database = database
        self._host = host
        self._port = port
        self._clustered = clustered
        self._timeout = timeout
        self._use_ssl = True
        # API related
        self._row_size = 0

        # Number of rows after which a flush is performed
        self._row_threshold = 100            # Decided dynamically based on row size
        self._set_flags = [0]

    HEADER_LEN = 10

    
    def _get_response(self, sock = None):
        ''' Get answer from SQream after sending a relevant message '''

        sock = sock or self.s
        # Getting 10-byte response header back
        header = _recieve(10, sock)
        server_protocol, bytes_or_text, message_len = header[0], header[1], unpack('q', header[2:10])[0]
        # server_protocol, bytes_or_text, message_len = _recieve(1, self.s), _recieve(1, self.s), _recieve(8, self.s)
         
        return _recieve(message_len, sock).decode('utf8')


    def _send_string(self, json_cmd, get_response = True, is_text_msg = True, sock = None):
 
        sock = sock or self.s
        # Generating the message header, and sending both over the socket
        sock.send( _get_message_header(len(json_cmd)) + json_cmd.encode('utf8'))
        
        if get_response:
            return self._get_response(sock)


    def set_socket(self, sock):
        assert isinstance(sock, (object, socket))
        self.s = sock

    def set_host(self, host):
        self._host = host

    def set_port(self, port):
        self._port = port

    def set_clustered(self, clustered):
        self._clustered = clustered

    def open_socket(self):
        try:
            self.set_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            self.s.settimeout(self._timeout)
        except socket.error as err:
            self.set_socket(None)
            raise RuntimeError("Error from SQream: " + str(err))
        except  Exception as e:
            raise RuntimeError("Other error: " + str(e))
        else:
            if self._use_ssl:
                self.cloak_socket()

    ''' SSL interlude : 
        ssl.wrap_socket(), ssl.get_server_certificate(addr, ssl_version=PROTOCOL_SSLv23, ca_certs=None)
    '''

    def cloak_socket(self, sock = None):
        ''' Wrap a socket to make it an SSL socket'''

        try:
            self.s = ssl.wrap_socket(sock or self.s, ssl_version=ssl.PROTOCOL_TLSv1, ciphers="ADH-AES256-SHA")
        except:
            print ("Error wrapping socket")  # check what exception goes here

    
    def close_socket(self):
        if self.s:
            try:
                self.s.close()
                self.set_socket(None)
            except(socket.error, AttributeError):
                pass

    
    def open_connection(self, ip, port):
        
        self._host = ip
        self._port = port 

        self.s = socket.socket()
        if self._use_ssl:
                self.cloak_socket()

        try:
            self.s.connect((ip, port))
        except socket.error as err:
            if self.s:
                self.close_connection()
            raise RuntimeError("Couldn't connect to SQream server - " + str(err))
        except Exception as e:
            print("Other error upon open connection: " + str(e))

    
    def close_connection(self):
        self.exchange('{"closeConnection":"closeConnection"}')
        self.close_socket()

    

    def create_connection(self, ip, port):
        #self.open_socket()
        
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
            # cmd_bytes_4 = cmd_str.expandtabs(1).encode('utf8')
            cmd_bytes_4 = cmd_str.encode('utf8')
        else:    
            cmd_bytes_2 = bytearray([2])           # 2 for binary
            cmd_bytes_4 = cmd_str

        cmd_bytes_3 = pack('q', len(cmd_bytes_4))
        cmd_bytes = cmd_bytes_1 + cmd_bytes_2 + cmd_bytes_3 + cmd_bytes_4

        return cmd_bytes

    def socket_recv(self, param):
        try:
            data_recv = self.s.recv(param)
            # TCP says recv will only read 'up to' param bytes, so keep filling buffer
            remainder = param - len(data_recv)
            retry_counter = 3     # In case data doesn't come in
            while remainder > 0:
                new_data = self.s.recv(remainder)
                
                # Handling no data coming in on socket
                if not new_data:
                    sleep(1)
                    retry_counter-= 1
                    if not retry_counter:
                        raise Exception('Connection to SQream interrupted')
                    continue

                # Data received    
                data_recv += new_data                
                remainder = param - len(data_recv)
            
            if b'{"error"' in data_recv:
                raise RuntimeError("Error from SQream: " + repr(data_recv))
        except socket.error as err:
            self.close_connection()
            self.set_socket(None)
            raise RuntimeError("Error from SQream: " + str(err))
        except RuntimeError as e:
            raise RuntimeError(e)
        except Exception as e:
            raise RuntimeError("Other error while receiving from socket: " + str(e))
        return data_recv

    
    def _get_msg(self):
        global SERVER_PROTOCOL_VERSION
        data_recv = self.socket_recv(self.HEADER_LEN)
        # print ("data recieved: ", repr(data_recv))   # dbg
        SERVER_PROTOCOL_VERSION = unpack('b', bytearray([data_recv[0]]))[0]
        if SERVER_PROTOCOL_VERSION not in SUPPORTED_VERSIONS:               
            raise RuntimeError(
                "SQream protocol version mismatch. Expecting " + str(PROTOCOL_VERSION) + ", but got " + str(
                    SERVER_PROTOCOL_VERSION) + ". Is this a newer/older SQream server?")
        val_len = unpack('q', data_recv[2:])[0]
        data_recv = self.socket_recv(val_len)
        return data_recv

    
    def exchange(self, cmd_str, close=False, binary = False):
        # If close=True, then do not expect to read anything back
        cmd_bytes = self.cmd2bytes(cmd_str, binary)
        try:
            self.s.settimeout(None)
            self.s.send(cmd_bytes)
        except socket.error as err:
            self.close_connection()
            self.set_socket(None)
            raise RuntimeError("Error from SQream: " + str(err))
        if close is False:
            # print ("message sent: ", cmd_str)  #dbg
            return self._get_msg()
        else:
            return


    def connect_database(self, database, username, password, service):
        ''' Connect to server picker and establish the correct ip/port if needed,
            then connect to the database '''

        if self._clustered is True:
            
            cmd_str = '{{"connectDatabase":"{0}","password":"{1}","username":"{2}"}}'.format(
            database.replace('"', '\\"'), password.replace('"', '\\"'), username.replace('"', '\\"'))

            self.exchange(cmd_str, True)  # Send connection string to picker, don't recieve/parsr standard response

            # Parse picker response to get ip and port
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
            
            # Reestablish socket with the given ip and port
            self.s.close()
            self.s = socket.socket()
            if self._use_ssl:
                self.cloak_socket()     
            self.s.connect((ip_addr, port))
        
        # At this point we are in contact with the proper ip/port, establish coms
        if service is None:
            cmd_str = """{{"connectDatabase":"{0}","password":"{1}","username":"{2}"}}""".format(
                database.replace('"', '\\"')
                , password.replace('"', '\\"')
                , username.replace('"', '\\"'))
        else:
             cmd_str = """{{"connectDatabase":"{0}","password":"{1}","username":"{2}", "service":"{3}"}}""".format(
                database.replace('"', '\\"')
                , password.replace('"', '\\"')
                , username.replace('"', '\\"')
                , service.replace('"', '\\"'))   
        
        self._connection_id = json.loads(self.exchange(cmd_str).decode('utf-8')).get('connectionId', '')


    # Reading bytes in Python 2 and 3
    def get_nulls_py2(self,column_data):
        return map(lambda c: unpack('b', bytes(c))[0], column_data)
        # or return [ord(c) for c in column_data]
   
    def get_nulls_py3(self,column_data):
        return [c for c in column_data]

    def _get_error(self):
        res = self._get_msg()
        if "error" in res: 
            return res

    
    def _prepare_statement(self, query_str):
        ''' 
        Called by Connector.prepare(). If contains 'insert into' and '?', comes complementary
        with table metadata and the ordered column names  '''            

        # Protocol check
        if SERVER_PROTOCOL_VERSION in (5,6):    # remove if/once everyone's on 5
            # getStatementId is new for SQream protocol version 5
            cmd_str = '{"getStatementId" : "getStatementId"}'
            self._statement_id = json.loads(self.exchange(cmd_str).decode('utf-8'))['statementId']

        # Send command and validate response from SQream
        cmd_str = """{{"prepareStatement":"{0}","chunkSize":{1}}}""".format(query_str.replace('"', '\\"'),
                                                                            str(DEFAULT_NETWORK_CHUNKSIZE))        
        res = self.exchange(cmd_str)
        #{"ip":"192.168.0.176","listener_id":0,"port":5000,"port_ssl":5001,"reconnect":true,"statementPrepared":true}
        self._balancer_params = json.loads(res.decode('utf-8'))

        # if b'statementPrepared' in res: 
        if self._balancer_params['statementPrepared']: 
            
            # Remove previous meta/data
            self._set_flags = [0]              # flags columns that were set. gets cleaned by next_row and possibly                 
            self._ordered_col_names = []
            self._col_indices = {}
            self._row_size = 0
            self._row_threshold = 0
            self.meta = []
            self.total_fetched = 0    # total amount of rows fetched so far
            self.current_row = 0      # number of rows that have been dispatched by next_row() = number of calls to next_row()
                           
        # Protocol versions 5 and below, queryType is called after prepareStatement
        if SERVER_PROTOCOL_VERSION < 6:
            self._get_query_type()

        return res

    
    def _execute(self):
        
        # If 'reconnect' paramater in the json response to prepareStatement is presenet and set to True
        if self._balancer_params.get('reconnect'):
            # print('reconnecting via _execute')
            self.close_socket() # no closeStatement / closeConnection statements on reconnection, dump and go
            port = self._balancer_params['port_ssl'] if self._use_ssl else self._balancer_params['port']
            # print('params for reconnection:', self._balancer_params['ip'], port) 
            self.create_connection(self._balancer_params['ip'], port)
    
            cmd_str =  '{{"service": "{}", "reconnectDatabase":"{}", "connectionId":{}, "listenerId":{},"username":"{}", "password":"{}"}}'.format(
                self.service, self._database, self._connection_id, self._balancer_params['listener_id'], self._user, self._password)
            self.exchange(cmd_str)
    
            cmd_str =  '{{"reconstructStatement": {}}}'.format(self._statement_id)
            self.exchange(cmd_str)
    
        self.exchange('{"execute":"execute"}')

        if SERVER_PROTOCOL_VERSION == 6:
            self._get_query_type()


    def _get_query_type(self):
        ''' Send one or two queryType requests to SQream to determine the type of the query and get metadata'''

        type_data = self._query_type('in')
        if not type_data:
            # Query_type_in returned empty
            type_data = self._query_type('out')
            self.statement_type = 'SELECT' if type_data else 'DML'
        else: 
            # query_type_in returned non-empty - insert statement
            self.statement_type = 'INSERT' 



    def _query_type(self, mode):
        ''' Query SQream for metadata, called automatically after prepare_statement '''

        # At some point in the future, query types will / should be merged
        cmd_str = '{"queryTypeIn": "queryTypeIn"}' if mode == 'in' else '{"queryTypeOut" : "queryTypeOut"}'
        json_key = 'queryType' if mode == 'in' else 'queryTypeNamed'

        res = self.exchange(cmd_str)
        self.column_json = json.loads(res.decode('utf8'))[json_key]   
        
        # Preallocate the list and an empty column name to index dictionary
        self.cols = [None] * len(self.column_json)
        self.meta = [None] * len(self.column_json)
        self._col_indices = {}

        if mode == 'in':
            # Insert statement, update variables for auto-flush and column set tracker
            self._row_size = sum(col['type'][1] for col in self.column_json)
            self._row_threshold = FLUSH_SIZE / self._row_size if self._row_size else 0  # calculate inside the network stuff
            self._set_flags = [0] * len(self.column_json)
            
            for idx, col in enumerate(self.column_json):
                # col['name'] =  col.get('name', '')
                self.meta[idx] = ColumnMetadata('', col['type'][0], col['type'][1], col['nullable'], col['isTrueVarChar'])
                self.cols[idx] = Column(col['type'][0], col['nullable'], col['type'][1])
        
        elif mode =='out':
            for idx, col in enumerate(self.column_json):
                # Column sizes (row number) is updated at fetch() time
                # col['name'] =  col.get('name', '')
                self.meta[idx] = ColumnMetadata(col['name'], col['type'][0], col['type'][1], col['nullable'], col['isTrueVarChar'])

                sq_col = Column(col['type'][0], col['nullable'], col['type'][1])
                sq_col._type_name = col['type'][0]
                sq_col._type_size = col['type'][1]
                sq_col._column_name = col['name']
                #def _column_name = (self, column_name): self._column_name = column_name
                self._ordered_col_names.append(sq_col._column_name) 
                # To allow quick switching between column names and locations in the table 
                self._col_indices[sq_col._column_name] = idx   
                sq_col._isTrueVarChar = col['isTrueVarChar']
                sq_col._nullable = col['nullable']
                self.cols[idx] = sq_col

      
        return self.cols

    

    def _fetch_all(self, discard = False):
        ''' Perform fetching untill all available date is retrieved'''
        res = True
        if discard:
            while res:
                res=self._fetch()
                # flush retrieved data
                col.data = []
        else:
            while res:
                res=self._fetch()

           
    def _fetch(self):
        exch = self.exchange('{"fetch" : "fetch"}')
        res = json.loads(exch.decode('utf-8'))        
        num_rows_fetched, column_sizes = res['rows'], res['colSzs']
        
        if num_rows_fetched == 0:
            # No content to read - All data has been assigned to our column objects
            return num_rows_fetched
        
        # Reading and parsing data     
        ignored_header = self.socket_recv(self.HEADER_LEN) # Read to ignore header, which is irrelevant here 
        col_size = list()
        idx_first = 0
        idx_last = 1
        # Metadata store + how many columns to read ([val], [len,blob], [null,val], [null,len,blob])
        for col in self.cols:   # self._query_data updated by self.self.column_json()
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

            col.data += column_data

        # Update boundary parameter for _next_row() 
        # self._num_of_rows = self._query_data[0]._column_size[0]  # 1 is fetched column size
        # print (self._num_of_rows)  #dbg
        return res['rows']  # No. of rows recieved 



    def _close_statement(self):
        if 'add flush condition':  # flush() doesn't fire blanks so it's keewl
            self._flush()  
        
        self.exchange('{"closeStatement":"closeStatement"}')  


    def _get_item(self, col_index_or_name, col_type, null_check = False):
        ''' Retrieves an item from the respective column using the set index. 
            index is modified by next_row() (increase only for a given query) '''

        # Convert column name to index
        if type(col_index_or_name) in (str, unicode):
            try:
                col_index = self._col_indices[col_index_or_name]
            except:
                if self._col_indices:
                    print ("Bad column name on get function")
                else:
                    # if _col_indices is an empty list
                    print ("No select statement issued")
                return 
        else:
            col_index = col_index_or_name -1

        # Trying to get the column
        try:
            col = self.cols[col_index]
        except IndexError: 
            print("Inexistent column index. Number of columns is " + str(len(self.cols)))
            return "Inexistent column index. Number of columns is " + str(len(self.cols))
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
            # print( col.data, self.current_row)   #dbg
            res = col.data[self.current_row-1]
        except IndexError:
            res = None
            print ("No more rows. Last value was: ", col.data[self.current_row -2 ])

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
                    print ("Bad column name on set function")
                else:
                    # if _col_indices is an empty list
                    print ("No insert statement issued")
                return 
        else:
            col_index = col_index_or_name - 1

        # Trying to get the column from the batch           
        try:
            col = self.cols[col_index]
        except IndexError: 
            announce(IndexError, ("Inexistent column index. Number of columns is " + str(len(self.cols))))
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

        # chunk =  b''.join((str(col._nulls) + col._nvarchar_lengths.tostring() + str(col.encoded_data) for col in self.cols))
        chunk =  b''.join((col._nulls.decode().encode('utf8') + col._nvarchar_lengths.tostring() + col.encoded_data for col in self.cols))
        # print ([(str(col._nulls),  col._nvarchar_lengths.tostring(),  str(col._data)) for col in self._batch])  #dbg
        # print ([(col._nvarchar_lengths.tostring()) for col in self._batch])  #dbg
        # print(len(chunk))
        # print (repr(chunk))
        if chunk:
            # Insert sequence - put command, binary chunk
            put_cmd = '{{"put":{}}}'.format(self.current_row)
            self.exchange(put_cmd, True)
            batch_insert_res = self.exchange(chunk, False, True)   # This one is a binary piece, execute and cmd2bytes were modified 
            
            # Truncate binary column upon success
            if batch_insert_res == b'{"putted":"putted"}':
                [col.reset_data() for col in self.cols]

    
    def _cols_to_rows(self, cols = None):
        ''' Transpose dem columns'''

        cols = cols or self.cols
        return zip(*(col.data for col in cols))      

    
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

api_to_sqream = {'bool':     'ftBool',   
                'ubyte' :    'ftUByte',
                'short' :    'ftShort',
                'int' :      'ftInt',
                'long' :     'ftLong',
                'float' :    'ftFloat',             
                'double' :   'ftDouble',
                'date' :     'ftDate',
                'datetime' : 'ftDateTime',
                'string' :   'ftVarchar',
                'nvarchar' : 'ftBlob',
                }


## User facing API object 
# -----------------------

class Connector(object):
    
    def __init__(self):
        # Store the connection
        self._sc = None
        # Store the columns from the result
        self.cols = None
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
    
         

    def connect(self, host, port, database, user, password, clustered, timeout, service = 'sqream'):
        sqream_ssl_port = 5100
        # No connection yet, create a new one
        if self._sc is None:
            # (self, username, password, database, host, port, clustered=False, timeout=15)
            self._sc = SqreamConn(database=database, username=user, password=password, clustered=clustered, timeout=timeout)
            self._sc._use_ssl = True if port == sqream_ssl_port else False
            self._sc.service = service
            self._sc._host = host
            self._sc._port = port 
            try:
                self._sc.s = socket.socket()
                if self._sc._use_ssl:
                    self.cloak_socket()     
                self._sc.s.connect((host, port))
                self._sc.connect_database(database, user, password, service)
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
            # self._sc.close_socket()
            self._sc.close_connection()
            self._sc = None

                

    #                                   API be here                               #
    #  ------------------------------------------------------------------------   #                


    ## General statements
    #  ------------------
    
    def execute(self):
      self._sc._execute()
   
    def get_statement_type(self):
        return self._sc.statement_type

    def get_statement_id(self):
        return self._sc._statement_id

    def get_metadata(self):
        return self._sc.meta

    def get_column_types(self):
        return [ i.type for i in self._sc.meta ]
    
    def get_column_names(self):
        return [ i.name for i in self._sc.meta ]
    
    def fetch_all_as_dict(self):
        buf = []
        types = self.get_column_types()
        names = self.get_column_names()
        lu = {'Bool' :      lambda i: self.get_bool(i)
              ,'Varchar' :  lambda i: self.get_string(i)
              ,'NVarchar' : lambda i: self.get_nvarchar(i)
              ,'Tinyint' :  lambda i: self.get_ubyte(i)
              ,'Int' :      lambda i: self.get_int(i)
              ,'Smallint' : lambda i: self.get_short(i)
              ,'Float' :    lambda i: self.get_float(i)
              ,'Double' :   lambda i: self.get_double(i)
              ,'Bigint' :   lambda i: self.get_long(i)
              ,'Date' :     lambda i: self.get_date(i)
              ,'DateTime' : lambda i: self.get_datetime(i)}
        
        while self.next_row():
            k = {}
            for i in range(0,len(types)):
                k[names[i]] = lu[types[i].tid](i+1)
            buf.append(k)
        return buf
    
    def close(self):
        '''close statement'''

        if self._sc.statement_type == 'INSERT':  # flush() doesn't fire blanks so it's keewl
            self._sc._flush()  #'''
        
        self._sc.exchange('{"closeStatement":"closeStatement"}')  #'''



    def prepare(self, query_str):
        ''' Prepare statement'''

        query_str = query_str.replace('\n', ' ').replace('\r', '') 
        
        self._sc._prepare_statement(query_str)

    

    def next_row(self):  

        if self._sc.statement_type == 'DML':
             raise Exception('Called next_row on a non Insert/Select query')

        elif self._sc.statement_type == 'SELECT':      # Select query
            if (self._sc.current_row  == self._sc.total_fetched): 
                # fetch more data from SQream
                num_rows_fetched = self._sc._fetch()
                if num_rows_fetched == 0:
                    return False 
                
                self._sc.total_fetched += num_rows_fetched
            
            self._sc.current_row += 1 

        elif self._sc.statement_type == 'INSERT':  # Insert query
            if sum(self._sc._set_flags) < len(self._sc.cols):
                raise RowFillException('Not all columns have been set')

            # Reset row flags and raise counter
            self._sc._set_flags = [0] * len(self._sc.column_json)
            self._sc.current_row += 1  

            # Flush and reset row counter if needed
            if self._sc.current_row >= self._sc._row_threshold:
                self._sc._flush()
                self._sc.current_row = 0  

        return True

 
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
            announce(BadTypeForSetFunction, 'Expecting tinyint value but got {}, which is of type {}'.format(val, str(type(val))))
            
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
