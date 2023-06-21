"""Contains base classes for testing pysqream package"""
import logging
import pytest

import pysqream


logger = logging.getLogger(__name__)


def _get_logger():
    return logger


Logger = _get_logger  # for compatibility


def connect_dbapi(ip, clustered=False, use_ssl=False):
    port = (3109 if use_ssl else 3108) if clustered else (5001 if use_ssl else 5000)
    return pysqream.connect(ip, port, 'master', 'sqream', 'sqream', clustered, use_ssl)


class TestBase:
    """
    Base for building class-based tests with debugging before and after tests
    """
    con = None

    @pytest.fixture(autouse=True)
    # pylint: disable=invalid-name
    def Test_setup_teardown(self, ip_address, conn):
        """Use pytest's function to run for each test of descendants"""
        # Keep for compatibility
        logger.debug("Before Scenario")
        logger.debug("Connect to server %s", ip_address)
        self.con = conn
        yield
        logger.debug("Close Session to server %s", ip_address)
        logger.debug("After Scenario")

# TestBaseWithoutBeforeAfter didn't do anything, it is not required
