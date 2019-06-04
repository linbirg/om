#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
test_dir = os.path.dirname(__abs_file__)
om_dir = os.path.dirname(test_dir)
sys.path.append(om_dir)

from yom import Model, CharField

import unittest

import cx_Oracle as cx


class RiskOrderDao(Model):
    __table__ = 'T_IRSK_ORDER'

    local_order_no = CharField(name='f_local_order_no',
                               primary_key=True,
                               size=16)
    seat_id = CharField(name='f_seat_id', primary_key=True, size=8)
    order_no = CharField(name='f_order_no', size=16)


def out_method(name):
    print('now in out_method:%s' % name)


class Session(object):
    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def my_func(self):
        print('in my func, ok fund.')

    def call_agent_method(self, agent, method, *args, **kwargs):
        agent_method = getattr(agent, method)
        return agent_method(*args, **kwargs)

    def __getattr__(self, method_name):
        def _missing(*args, **kwargs):
            print("A missing method was called.")
            print("The object was %r, the method was %r. " %
                  (self, method_name))
            print("It was called with %r and %r as arguments" % (args, kwargs))

            # out_method(*args, **kwargs)
            RiskOrderDao.db_conn = self.db

            if method_name in dir(RiskOrderDao):
                return self.call_agent_method(RiskOrderDao, method_name, *args,
                                              **kwargs)
            # return RiskOrderDao.method_name(*args, **kwargs)

        return _missing


class TestCasePackageFpOrder(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_handle_req(self):
        s = Session()
        s.db = cx.connect('%s/%s@%s' %
                          ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))
        s.my_func()

        s.un_know(name='hello 3')

        order = s.find_one(local_order_no='81905070000009', seat_id='701211')
        print('order no:', order.order_no)

        orders = s.find(seat_id='701211')
        for o in orders:
            print(o.local_order_no, o.order_no)


if __name__ == "__main__":
    unittest.main()