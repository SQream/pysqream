import os, sys
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from .pysqream import connect, __version__, enable_logs, stop_logs