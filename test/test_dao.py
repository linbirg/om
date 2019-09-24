#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
test_dir = os.path.dirname(__abs_file__)
om_dir = os.path.dirname(test_dir)
sys.path.append(om_dir)

from yom import Model, CharField, Dao, IntField

import unittest

import cx_Oracle as cx


class RiskCustRisk(Model):
    __table__ = 't_irsk_mem_cust_risk'
    client_id = CharField(name='f_client_id', primary_key=True, size=12)
    seat_id = CharField(name='f_seat_id', primary_key=True, size=8)
    risk_days = IntField(name='F_RISK_DAYS', size=10)


class RiskCustRiskDao(Dao):
    def __init__(self, db_conn):
        return super().__init__(db_conn, RiskCustRisk)


class TestCasePackageFpOrder(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @unittest.skip('')
    def test_select_page(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        dao = RiskCustRiskDao(db_conn)

        clients = dao.select_page()
        print('len:', len(clients))
        for c in clients:
            print(c)

        clients = dao.select_page(where='F_RISK_DAYS > :days',
                                  order_by='f_client_id asc',
                                  days=1)
        print('len:', len(clients))

        for c in clients:
            print(c)

    # @unittest.skip('')
    def test_find_page(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        dao = RiskCustRiskDao(db_conn)

        clients = dao.find_page(risk_days=4, first=1, last=2)
        for c in clients:
            print(c)


if __name__ == "__main__":
    unittest.main()