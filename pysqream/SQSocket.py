
from pysqream.logger import *
from pysqream.globals import PROTOCOL_VERSION, SUPPORTED_PROTOCOLS, clean_sqream_errors
import socket
import ssl
import sys
import array
from struct import pack, unpack
from threading import Lock


class SQSocket:
    ''' Extended socket class with some'''

    def __init__(self, ip, port, use_ssl=False):
        self.ip, self.port, self.use_ssl = ip, port, use_ssl
        self._setup_socket(ip, port)

    def _setup_socket(self, ip, port):

        self.s = socket.socket()
        if self.use_ssl:
            # Python 3.10 SSL fix
            # 3.10 has increased the default TLS security settings,
            # need to downgrade to be compatible with current versions of sqream
            if sys.version_info.minor >= 10:
                self.ssl_context = ssl._create_unverified_context()
                self.ssl_context.set_ciphers('DEFAULT')  # 'AES256-SHA', 'RSA'
                # self.ssl_context.verify_mode = ssl.VerifyMode.CERT_NONE
                # self.ssl_context.options &= ~ssl.OP_NO_SSLv3
                self.s = self.ssl_context.wrap_socket(self.s, server_hostname=ip)
            else:
                self.s = ssl.wrap_socket(self.s)
        try:
            self.timeout(10)
            self.s.connect((ip, port))
        except ConnectionRefusedError as e:
            log_and_raise(ConnectionRefusedError, "Connection refused, perhaps wrong IP?")
        except ConnectionResetError:
            log_and_raise(Exception, 'Trying to connect to an SSL port with use_ssl = False')
        except Exception as e:
            if 'timeout' in repr(e):
                log_and_raise(Exception, "Timeout when connecting to SQream, perhaps wrong IP?")
            elif '[SSL: UNKNOWN_PROTOCOL] unknown protocol' in repr(e) or '[SSL: WRONG_VERSION_NUMBER]' in repr(e):
                log_and_raise(Exception, 'Using use_ssl=True but connected to non ssl sqreamd port')
            elif 'EOF occurred in violation of protocol (_ssl.c:' in repr(e):
                log_and_raise(Exception, 'Using use_ssl=True but connected to non ssl sqreamd port')
            else:
                log_and_raise(Exception, e)
        else:
            self.timeout(None)

    # General socket / tls socket functionality
    #

    def _check_server_up(self, ip=None, port=None, use_ssl=None):

        try:
            SQSocket(ip or self.ip, port or self.port, use_ssl or self.use_ssl)
        except ConnectionRefusedError:
            log_and_raise(ConnectionRefusedError, f"Connection to SQream interrupted")

    def send(self, data):

        # print ("sending: ", data)
        # try:
        return self.s.send(data)

        # except BrokenPipeError:
        #    raise BrokenPipeError('No connection to SQream. Try reconnecting')

    def close(self):
        return self.s.close()

    def timeout(self, timeout='not passed'):

        if timeout == 'not passed':
            return self.s.gettimeout()

        self.s.settimeout(timeout)

    # Extended functionality
    #

    def reconnect(self, ip=None, port=None):

        self.s.close()
        self._setup_socket(ip or self.ip, port or self.port)


class Client:

    def __init__(self, socket):
        self.socket = socket

    def receive(self, byte_num, timeout=None):
        ''' Read a specific amount of bytes from a given socket '''

        data = bytearray(byte_num)
        view = memoryview(data)
        total = 0

        if timeout:
            self.socket.settimeout(timeout)

        while view:
            # Get whatever the socket gives and put it inside the bytearray
            received = self.socket.s.recv_into(view)
            if received == 0:
                log_and_raise(ConnectionRefusedError, f'SQreamd connection interrupted - 0 returned by socket')
            view = view[received:]
            total += received

        if timeout:
            self.socket.settimeout(None)

        return data

    def get_response(self, is_text_msg=True):
        ''' Get answer JSON string from SQream after sending a relevant message '''
        lock = Lock()

        # Getting 10-byte response header back
        with lock:
            header = self.receive(10)
        server_protocol = header[0]
        if server_protocol not in SUPPORTED_PROTOCOLS:
            log_and_raise(Exception,
                          f'Protocol mismatch, client version - {PROTOCOL_VERSION}, server version - {server_protocol}')
        # bytes_or_text =  header[1]
        message_len = unpack('q', header[2:10])[0]

        with lock:
            receive = self.receive(message_len).decode(
                'utf8') if is_text_msg else self.receive(message_len)

        return receive

    # Non socket aux. functionality
    #
    def generate_message_header(self, data_length, is_text_msg=True, protocol_version=PROTOCOL_VERSION):
        """Generate SQream's 10 byte header prepended to any message"""

        return pack('bb', protocol_version, 1 if is_text_msg else 2) + pack(
            'q', data_length)

    def validate_response(self, response, expected):

        if expected not in response:
            # Color first line of SQream error (before the haskell thingy starts) in Red
            response = '\033[31m' + (response.split('\\n')[0] if clean_sqream_errors else response) + '\033[0m'
            log_and_raise(Exception, f'\nexpected response {expected} but got:\n\n {response}')

    def send_string(self, json_cmd, get_response=True, is_text_msg=True, sock=None):
        ''' Encode a JSON string and send to SQream. Optionally get response '''

        # Generating the message header, and sending both over the socket
        printdbg(f'string sent: {json_cmd}')
        self.socket.send(self.generate_message_header(len(json_cmd)) + json_cmd.encode('utf8'))

        if get_response:
            return self.get_response(is_text_msg)
