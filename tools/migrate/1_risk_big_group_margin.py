#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import os
import sys

__abs_file__ = os.path.abspath(__file__)
tool_dir = os.path.dirname(os.path.dirname(__abs_file__))
code_dir = os.path.dirname(tool_dir)
sys.path.append(code_dir)

from .rake_migrate import RakeMigrate

from lib import dbutil

import module.dao.base.pg_field_desc as fd


class createBigGroupMargin(RakeMigrate):
    def __init__(self):
        super().__init__()
        self.db_conn = dbutil.get_connection('risk_db')

    def up(self):
        self.create_table(
            't_irsk_big_group_margin', fd.ClientIDField(primary_key=True),
            fd.SeatIDField(primary_key=True),
            fd.TGroupIDField(primary_key=True),
            fd.TMoneyField(name='f_bi_exch_long_posi_margin',
                           default=0,
                           desc='1 交易所合约组双边持仓多仓占用保证金'),
            fd.TMoneyField(name='f_bi_exch_short_posi_margin',
                           default=0,
                           desc='2 交易所合约组双边持仓空仓占用保证金'),
            fd.TMoneyField(name='f_bi_mem_long_posi_margin',
                           default=0,
                           desc='3 会员合约组双边持仓多仓占用保证金'),
            fd.TMoneyField(name='f_bi_mem_short_posi_margin',
                           default=0,
                           desc='4 会员合约组双边持仓空仓占用保证金'),
            fd.TMoneyField(name='f_bi_exch_long_frozen_margin',
                           default=0,
                           desc='5 交易所合约组双边多仓冻结保证金'),
            fd.TMoneyField(name='f_bi_exch_short_frozen_margin',
                           default=0,
                           desc='6 交易所合约组双边空仓冻结保证金'),
            fd.TMoneyField(name='f_bi_mem_long_frozen_margin',
                           default=0,
                           desc='7 会员合约组双边多仓冻结保证金'),
            fd.TMoneyField(name='f_bi_mem_short_frozen_margin',
                           default=0,
                           desc='8 会员合约组双边空仓冻结保证金'),
            fd.TMoneyField(name='f_big_exch_margin',
                           default=0,
                           desc='9 交易所合约组大边保证金'),
            fd.TMoneyField(name='f_big_mem_margin',
                           default=0,
                           desc='10 会员合约组大边保证金'),
            fd.TMoneyField(name='f_big_exch_frozen_margin',
                           default=0,
                           desc='11 交易所合约组大边冻结保证金'),
            fd.TMoneyField(name='f_big_mem_frozen_margin',
                           default=0,
                           desc='12 会员合约组大边冻结保证金'), fd.CreateAtField(),
            fd.UpdateAtField())

    def down(self):
        self.drop('t_irsk_big_group_margin')
