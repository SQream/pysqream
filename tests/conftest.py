

def pytest_addoption(parser):
    parser.addoption("--ip", action="store", help="SQream Server ip", default="192.168.0.35")


def pytest_generate_tests(metafunc):
    metafunc.config.getoption("ip")
