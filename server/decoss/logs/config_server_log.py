import logging
import sys
import os
sys.path.append(os.path.join(os.getcwd(), '../..'))
from JIM.config import *

formatter = logging.Formatter(FORMATTER)
log = logging.getLogger('server')
sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(formatter)
log.addHandler(sh)
fh = logging.FileHandler("../server.log", encoding='utf-8')
fh.setFormatter(logging.Formatter(FORMATTER))
fh.setLevel(logging.DEBUG)
log.addHandler(fh)