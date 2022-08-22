import utils
import sys

__version__ = '3.1.8'
buf_maps, buf_views = [], []
WIN = True if sys.platform in ('win32', 'cygwin') else False
PROTOCOL_VERSION = 8
SUPPORTED_PROTOCOLS = 6, 7, 8
BUFFER_SIZE = 100 * int(1e6)  # For setting auto-flushing on netrwork insert
ROWS_PER_FLUSH = 100000
DEFAULT_CHUNKSIZE = 0  # Dummy variable for some jsons
FETCH_MANY_DEFAULT = 1  # default parameter for fetchmany()
VARCHAR_ENCODING = 'ascii'

clean_sqream_errors = True
support_pandas = False
dbg = False
logger  = logging.getLogger("dbapi_logger")
logger.setLevel(logging.DEBUG)
logger.disabled = True
get_ram = utils.get_ram_windows() if WIN else utils.get_ram_linux()

logger  = logging.getLogger("dbapi_logger")
logger.setLevel(logging.DEBUG)
logger.disabled = True


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