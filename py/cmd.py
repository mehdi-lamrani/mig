pyspark --master local[56] --driver-memory 92g --py-files /devp/py/tools.py

import sys
sys.path.append('/devp/py')
import importlib
importlib.reload(tools)
from tools import *