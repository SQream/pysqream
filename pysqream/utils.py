"""Additional utilities of common purpose"""
import os
import re
from packaging import version


def get_ram_linux():
    """Get RAM on Linux OS"""
    # Don't need to run subprocess
    mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    return mem_bytes // (1024)  # in kilobytes for compatibility


def get_ram_windows():
    """Dummy function"""


def version_compare(ver1, ver2):
    """
    Compares two versions of SQream and returns the result.

    Args:
        ver1 (str): The first version string to compare.
        ver2 (str): The second version string to compare.

    Returns:
        int: Returns -1 if ver1 is less than ver2, 1 if ver1 is greater
             than ver2, or 0 if ver1 is equal to ver2. Returns None if
             any of the version strings are None or if the version
             format is invalid.
    """
    if None in (ver2, ver1):
        return None
    match1 = re.search("\\d{4}(\\.\\d+)+", ver2)
    match2 = re.search("\\d{4}(\\.\\d+)+", ver2)
    if None in (match2, match1):
        return None
    ver1 = version.parse(match1.group(0))
    ver2 = version.parse(match2.group(0))
    return -1 if ver1 < ver2 else 1 if ver1 > ver2 else 0


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
