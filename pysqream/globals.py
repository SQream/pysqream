from pysqream.utils import get_ram_windows, get_ram_linux
import sys

__version__ = '3.2.2'
buf_maps, buf_views = [], []
WIN = True if sys.platform in ('win32', 'cygwin') else False
MAC = True if sys.platform in ('darwin') else False
PROTOCOL_VERSION = 8
SUPPORTED_PROTOCOLS = 6, 7, 8
BUFFER_SIZE = 100 * int(1e6)  # For setting auto-flushing on netrwork insert
ROWS_PER_FLUSH = 100000
DEFAULT_CHUNKSIZE = 0  # Dummy variable for some jsons
FETCH_MANY_DEFAULT = 1  # default parameter for fetchmany()
VARCHAR_ENCODING = 'ascii'

CYTHON = False # Cython IS NOT SUPPORTED
clean_sqream_errors = False
support_pandas = False
dbg = False

if WIN:
    get_ram = get_ram_windows()
elif MAC:
    get_ram = None
else:
    get_ram = get_ram_linux()

try:
    import pyarrow as pa
    from pyarrow import csv
    import numpy as np
    ARROW = True
except:
    ARROW = False
else:
    sqream_to_pa = {
        'ftBool':     pa.bool_(),
        'ftUByte':    pa.uint8(),
        'ftShort':    pa.int16(),
        'ftInt':      pa.int32(),
        'ftLong':     pa.int64(),
        'ftFloat':    pa.float32(),
        'ftDouble':   pa.float64(),
        'ftDate':     pa.timestamp('ns'),
        'ftDateTime': pa.timestamp('ns'),
        'ftVarchar':  pa.string(),
        'ftBlob':     pa.utf8(),
        'ftNumeric':  pa.decimal128(38, 11)
    }

# For encoding data to be sent to SQream using struct.pack() and for type checking by _set_val()
type_to_letter = {
    'ftBool': '?',
    'ftUByte': 'B',
    'ftShort': 'h',
    'ftInt': 'i',
    'ftLong': 'q',
    'ftFloat': 'f',
    'ftDouble': 'd',
    'ftDate': 'i',
    'ftDateTime': 'q',
    'ftVarchar': 's',
    'ftBlob': 's',
    'ftNumeric': '4i'
}

typecodes = {
    'ftBool': 'NUMBER',
    'ftUByte': 'NUMBER',
    'ftInt': 'NUMBER',
    'ftShort': 'NUMBER',
    'ftLong': 'NUMBER',
    'ftDouble': 'NUMBER',
    'ftFloat': 'NUMBER',
    'ftDate': 'DATETIME',
    'ftDateTime': 'DATETIME',
    'ftVarchar': 'STRING',
    'ftBlob': 'STRING',
    'ftNumeric': 'NUMBER'
}
