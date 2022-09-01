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