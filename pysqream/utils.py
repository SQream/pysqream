

def get_ram_linux():
    vmstat, err = Popen('vmstat -s'.split(), stdout=PIPE, stderr=PIPE).communicate()

    return int(vmstat.splitlines()[0].split()[0])


def get_ram_windows():
    pass
