import socket, ssl, selectors, json, datetime, sys, itertools as it
import logging, time, traceback
from struct import pack, unpack
from collections import deque


PROTOCOL_VERSION = 8
VARCHAR_ENCODING = 'ascii'


def printdbg(*debug_print, dbg = True):
    if dbg:
        print(*debug_print)


sql_to_sqream_type = {
    'bool'     : ('ftBool', 1),
    'tinyint'  : ('ftUByte', 1),
    'smallint' : ('ftShort', 2),
    'int'      : ('ftInt', 4),
    'bigint'   : ('ftLong', 8),
    'real'     : ('ftFloat', 4),
    'float'    : ('ftDouble', 8),   # Yeah I know
    'double'   : ('ftDouble', 8),
    'date'     : ('ftDate', 4),
    'datetime' : ('ftDateTime', 8),
    'varchar'  : ('ftVarchar', None),
    'text'     : ('ftBlob', 0),
}


class MockException(Exception):
    pass


class MockSock:
    ''' Mock server connection class '''
    
    def __init__(self, ip = '127.0.0.1', port = 5000, use_ssl=False):
        self.ip, self.port, self.use_ssl = ip, port, use_ssl
        # self.db = sqlite3.connect('memory')
        self.created_tables = {}
        self.table_meta = {}
        self.table_data = {}
        self.sel = selectors.DefaultSelector()
        self.ss = socket.socket()
        if self.use_ssl:
            self.ss = ssl.wrap_socket(self.s)

        self.ss.bind((ip, port))
        # self.ss.setblocking(False)
        self.ss.listen()
        self.accept()
        # self.sel.register(self.ss, selectors.EVENT_READ, accept)
        

    def accept(self, blocking = True):
        ''' Distributing connections to sockets '''

        print (f'Listening on {self.port}')
        conn, addr = self.ss.accept()
        print (f'Connection accepted on address: {addr}')
        # conn.setblocking(blocking)
        # sel.register(conn, selectors.EVENT_READ, read)
        self.start(conn)

        
    def start(self, conn):

        stmt_id = 1
        expecting = 'connectDatabase'
        while True:
            ''' 
            # Selector path
            for key, mask in sel.select():
                key.data(key.fileobj, mask)
            # '''

            header = conn.recv(10)
            if not header:
                continue
            '''
            client_protocol = header[0]
            if client_protocol != PROTOCOL_VERSION
                raise MockException(f'Protocol mismatch, client version - {client_protocol}, server version - {PROTOCOL_VERSION}')
            bytes_or_text =  header[1]
            # '''
            printdbg ("header: ", header)
            message_len = unpack('q', header[2:10])[0]
            printdbg(f'message len:{message_len}')
            data = conn.recv(message_len)
            self.msg = json.loads(data.decode('utf8'))
            printdbg(f'received: {self.msg}')  
            
            # Close Connection requested
            if 'closeConnection' in self.msg:
                rsp = f'{{"connectionClosed":"connectionClosed"}}'
                self.send_json(conn, rsp)
                break      

            # Not a request to close connection, fall into state machine
            self.validate_msg(expecting)

            # Connect database
            if 'connectDatabase' in self.msg:
                rsp = f'{{"connectionId":1,"databaseConnected":"databaseConnected","varcharEncoding":"{VARCHAR_ENCODING}"}}'
                self.send_json(conn, rsp)
                expecting = 'getStatementId'
                continue

            # Get statement ID
            if 'getStatementId' in self.msg:
                rsp = f'{{"statementId":{stmt_id}}}'
                self.send_json(conn, rsp)
                stmt_id +=1
                expecting = 'prepareStatement'
                continue

            # Prepare statement
            if 'prepareStatement' in self.msg:
                query = self.msg['prepareStatement'].lower()
                self.query_type = query.split()[0]

                if self.query_type == 'create':
                    self.generate_table_metadata(query)

                    '''
                    printdbg(f'\nMeta for create query:')
                    for meta in self.table_meta["bobo"]:
                        printdbg(meta)
                    '''

                elif self.query_type == 'insert':
                    table_name = query.split()[2].lower()
                    if table_name not in self.table_meta:
                        raise MockException(f"Trying to insert to table {table_name} which wasn't created")
                    self.table_data[table_name] = deque()

                elif self.query_type == 'select':
                    try:
                        table_name = query.split()[3].lower()
                    except IndexError:
                        # None table select, e.g. 'select 1'
                        self.query_type = 'select_const'
                    else:
                        self.query_type = 'select_table'

                else:
                    # Query didn't start with create, select or insert
                    raise MockException(f"Query started with {self.query_type}, u trollin'?")


                rsp = f'{{"ip":"127.0.0.1","listener_id":0,"port":5000,"port_ssl":5001,"reconnect":false,"statementPrepared":true}}'
                self.send_json(conn, rsp)
                expecting = 'execute'
                continue

            # Execute
            if 'execute' in self.msg:
                rsp = f'{{"executed":"executed"}}'
                self.send_json(conn, rsp)
                expecting = 'queryTypeIn'
                continue

            # Insert query metadata
            if 'queryTypeIn' in self.msg:
                if self.query_type in ('create', 'select_const', 'select_table'):
                    # Only giving meta for insert queries
                    rsp = f'{{"queryType":[]}}' 
                    expecting = 'queryTypeOut'
                else:
                    # Query type is insert
                    rsp = f'{{"queryType":[{self.table_meta[table_name][0]}]}}' 
                    # printdbg (f'querytypeIn reponse:\n{rsp}')
                    expecting = 'put', 'closeStatement'

                self.send_json(conn, rsp)
                continue

            # Select query metadata
            if 'queryTypeOut' in self.msg:
                if self.query_type in ('create', 'insert') or self.query_type == 'select_const':
                    # Not a select from table query
                    rsp = f'{{"queryTypeNamed":[]}}'
                    expecting = 'closeStatement'
                else:
                    # Select from table
                    rsp = f'{{"queryTypeNamed":[{self.table_meta[table_name][0]}]}}' 
                    expecting = 'fetch', 'closeStatement'

                self.send_json(conn, rsp)
                continue

            # Sending binary buffer via netowrk insert
            if 'put' in self.msg:
                rows_num      = int(self.msg['put'])
                                
                # Get binary data
                binary_header = conn.recv(10)
                binary_len    = unpack('q', binary_header[2:10])[0]
                binary_data   = self.receive(conn, binary_len)
                # binary_data   = conn.recv(unpack('q', conn.recv(10)[2:10])[0])

                # Calculating colSzs
                col_sizes     = self.calculate_col_szs(table_name, rows_num, binary_data) 

               # Verify size is equal the sum of col_sizes
                self.table_data[table_name].append((rows_num, col_sizes, binary_data))
                # printdbg (f'binary data appended: {self.table_data}')
                rsp = f'{{"putted":"putted"}}'  
                self.send_json(conn, rsp)
                expecting = 'put', 'closeStatement'
                continue

            # Fetching inserted data from table
            if 'fetch' in self.msg:
                rows, col_szs, buf = self.table_data[table_name].popleft() if self.table_data[table_name] else (0, '', b'')
                '{"colSzs":[],"rows":0}'
                rsp = f'{{"colSzs":[{col_szs}],"rows":{rows}}}'  
                self.send_json(conn, rsp)
                conn.send(self.generate_message_header(len(buf)) + buf)
                
                expecting = 'fetch', 'closeStatement'
                continue

            # Close statement
            if 'closeStatement' in self.msg:
                rsp = f'{{"statementClosed":"statementClosed"}}'  
                self.send_json(conn, rsp)
                expecting = 'getStatementId'
                continue


    def send_json(self, conn, response):

        printdbg(f'\033[94msending: {response}\033[0m' + ('\n' if 'statementClosed' in response else ''))  
        return conn.send(self.generate_message_header(len(response)) + response.encode('utf8'))
    

    def receive(self, sock, byte_num, timeout=None):
        ''' Read a specific amount of bytes from a given socket '''

        data = bytearray(byte_num)
        view = memoryview(data)
        total = 0

        if timeout:
            sock.settimeout(timeout)
        
        while view:
            # Get whatever the socket gives and put it inside the bytearray
            received = sock.recv_into(view)
            if received == 0:
                raise ConnectionRefusedError('Client connection interrupted - 0 returned by socket')
            view = view[received:]
            total += received

        if timeout:
            sock.settimeout(None)
            
        
        return data


    # Non socket aux. functionality
    #

    def generate_message_header(self, data_length, is_text_msg=True, protocol_version=PROTOCOL_VERSION):
        ''' Generate SQream's 10 byte header prepended to any message '''

        return pack('bb', protocol_version, 1 if is_text_msg else 2) + pack('q', data_length)


    def validate_msg(self, expected, msg = None):

        expected = (expected,) if isinstance(expected, str) else expected
        if not any(expected_msg in (msg or self.msg) for expected_msg in expected):
            raise MockException(f'\nExpected {expected} json but got:\n{msg}')

            
    # Query parsing
    #  

    # Create
    def generate_table_metadata(self, query):
        ''' SQLite does not supply metadata after select query, keeping it during create '''

        table_name            = query[query.index('table') + 6 : query.index('(')].strip()
        names, types, is_null = zip(*((item.split()[0], item.split()[1], item.split()[-2]) for item in query[query.index('(')+1:-1].split(',')))
        types                 = [col_type.split('(') for col_type in types]
        
        self.cols_meta = [
            ('true' if col_type[0] == 'text' else 'false', 
             names[idx], 
             'false' if is_null[idx] == 'not' else 'true',
             sql_to_sqream_type[col_type[0]][0], 
             sql_to_sqream_type[col_type[0]][1] if col_type[0] != 'varchar' else int(col_type[1][:-1]) 
            )
            for idx, col_type in enumerate(types)
         ]

        # Col sizes template, used for the response to "fetch"
        # text columns sizes need to be extracted deliberately from the binary buffer
        col_szs_tups = ((1*(meta[2] == 'true'), 4*(meta[0] == 'true'), meta[-1] if meta[-1] !=0 else -1) for meta in self.cols_meta)
        col_szs      = [num for tup in col_szs_tups for num in tup if num !=0]

        # {"isTrueVarChar":{is_tvc},"name":"{name}","nullable":{is_nul},"type":["{col_type}",{col_size},0]}
        col_meta_st = '{{"isTrueVarChar":{},"name":"{}","nullable":{},"type":["{}",{},0]}}'
        
        self.table_meta[table_name] = [','.join(col_meta_st.format(*col_meta) for col_meta in self.cols_meta), col_szs]
        

    # Insert
    def calculate_col_szs(self, table_name, rows_num, binary_data):
        '''  The size of most columns is known in advance, except text columns '''

        col_sizes =  [size * rows_num for size in self.table_meta[table_name][1]]
        text_sizes_start = 0
        for idx in range(len(col_sizes)-1):
            if col_sizes[idx+1] < 0:
                # Next one up is an unknown size of a text data column, here cometh thy money
                col_sizes[idx+1] = sum(unpack(f'{rows_num}i', binary_data[text_sizes_start : text_sizes_start + col_sizes[idx]]))
                # printdbg(f'text col size: {col_sizes[idx+1]}')

            text_sizes_start += col_sizes[idx]


        return ','.join(str(size) for size in col_sizes)

    # Select



class MockListener:
    ''' Accept connections and create MockSock classes to handle them '''

    def __init__(self, ip = '127.0.0.1', port = 5000, use_ssl=False):
        self.ip, self.port, self.use_ssl = ip, port, use_ssl
        # self.db = sqlite3.connect('memory')
        self.sel = selectors.DefaultSelector()
        self.ss = socket.socket()
        if self.use_ssl:
            self.ss = ssl.wrap_socket(self.s)

        self.ss.bind((ip, port))
        # self.ss.setblocking(False)
        self.ss.listen()
        

    def accept(self):

        print (f'Listening on {self.port}')
        conn, addr = self.ss.accept()
        print (f'Connection accepted on address: {addr}')
        # conn.setblocking(blocking)
        # sel.register(conn, selectors.EVENT_READ, read)
        self.start(conn)



def bind(ip = '127.0.0.1', port = 5000):

    mock_sock = MockSock(ip, port)

    return mock_sock


if __name__ == '__main__':

    args = sys.argv
    # ip, port = args[1:]
    ip = '127.0.0.1'
    # ip = ''
    mock = bind(ip, 5000)
