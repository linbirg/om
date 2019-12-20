#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import os
import sys

__abs_file__ = os.path.abspath(__file__)
base_dir = os.path.dirname(__abs_file__)
dao_dir = os.path.dirname(base_dir)
module_dir = os.path.dirname(dao_dir)
code_dir = os.path.dirname(module_dir)
sys.path.append(code_dir)

# from lib import dbutil
from lib.yom_pg import PgCharField, PgIntField, PgDoubleField, PgTimeStampField, PgStringField, PgTextField, PgNumericField


class ClientIDField(PgCharField):
    def __init__(self, primary_key=False, desc='客户编码'):
        super().__init__(name='f_client_id',
                         primary_key=primary_key,
                         size=12,
                         default=None,
                         desc=desc)


class SeatIDField(PgCharField):
    def __init__(self, primary_key=False, desc='席位代码'):
        super().__init__(name='f_seat_id',
                         primary_key=primary_key,
                         size=8,
                         default=None,
                         desc=desc)


class TMoneyField(PgIntField):
    def __init__(self, name, primary_key=False, default=0, desc=''):
        super().__init__(name=name,
                         primary_key=primary_key,
                         default=default,
                         desc=desc)


class TGroupIDField(PgIntField):
    def __init__(self, name='F_GROUP_ID', primary_key=False, desc='合约组合代码'):
        super().__init__(name=name, primary_key=primary_key, desc=desc)


class UpdateAtField(PgTimeStampField):
    def __init__(self,
                 name='F_UPDATE_TIMESTAMP',
                 primary_key=False,
                 desc='更新时间'):
        super().__init__(name=name,
                         primary_key=primary_key,
                         default=None,
                         desc=desc)


class CreateAtField(PgTimeStampField):
    def __init__(self,
                 name='F_CREATE_TIMESTAMP',
                 primary_key=False,
                 desc='创建时间'):
        super().__init__(name=name,
                         primary_key=primary_key,
                         default=None,
                         desc=desc)
