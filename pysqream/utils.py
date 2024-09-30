import re
from packaging import version
from subprocess import Popen, PIPE


def get_ram_linux():
    vmstat, err = Popen('vmstat -s'.split(), stdout=PIPE, stderr=PIPE).communicate()

    return int(vmstat.splitlines()[0].split()[0])


def get_ram_windows():
    pass


## Version compare
def version_compare(v1, v2) :
    if (v2 is None or v1 is None):
        return None
    r1 = re.search("\\d{4}(\\.\\d+)+", v1)
    r2 = re.search("\\d{4}(\\.\\d+)+", v2)
    if (r2 is None or r1 is None):
        return None
    v1 = version.parse(r1.group(0))
    v2 = version.parse(r2.group(0))
    return -1 if v1 < v2 else 1 if v1 > v2 else 0


def get_array_size(data_size: int, buffer_length: int) -> int:
    """Get the SQream ARRAY size by inner data size and buffer length

    Args:
        data_size: integer with the size of data inside ARRAY, for
          example for INT is 4, for BOOL is 1, etc.
        buffer_length: length of a chunk of buffer connected with one
          array

    Returns:
        An integer representing size of an ARRAY with fixed sized data
    """
    aligned_block_size = (data_size + 1) * 8  # data + 1 byte for null
    div, mod = divmod(buffer_length, aligned_block_size)
    size = div * 8
    if mod:
        size += int((mod - 8) / data_size)
    return size


def false_generator():
    """Generate endless sequence of False values

    Used for providing to zip(data, false_generator()) within data that
    is not nullable, so is_null value will also goes as False
    independent of size of data

    Example:
        >>> for val, is_null in zip([1, 2, 3, 4]], false_generator()):
        ...     print(val,is_null)
        ...
        1 False
        2 False
        3 False
        4 False

    Returns:
        A generator object that produces False value for each iteration
          endlessly
    """
    while True:
        yield False


class Error(Exception):
    pass

class Warning(Exception):
    pass

class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class ArraysAreDisabled(DatabaseError):
    pass
