# -*- coding:utf-8 -*-
# Author: yizr

import os
import sys

__abs_file__ = os.path.abspath(__file__)
core_dir = os.path.abspath(__file__)
mod_dir = os.path.dirname(core_dir)
code_dir = os.path.dirname(mod_dir)
sys.path.append(code_dir)

from .bean import RiskBaseBean, db, realsedb
from .transaction import transcation, commit
from .exception import log_exception
from lib.yom import resource
