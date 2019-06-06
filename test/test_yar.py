#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
test_dir = os.path.dirname(__abs_file__)
om_dir = os.path.dirname(test_dir)
sys.path.append(om_dir)

from yar import Model, CharField, IntField

import unittest

import cx_Oracle as cx


class RiskCustRisk(Model):
    __table__ = 't_irsk_mem_cust_risk'
    client_id = CharField(name='f_client_id', primary_key=True, size=12)
    seat_id = CharField(name='f_seat_id', primary_key=True, size=8)
    risk_days = IntField(name='F_RISK_DAYS', size=10)


# class RiskCustRiskDao(Dao):
#     def __init__(self, db_conn):
#         return super().__init__(db_conn, RiskCustRisk)


class RiskOrder(Model):
    __table__ = 'T_IRSK_ORDER'

    local_order_no = CharField(name='f_local_order_no',
                               primary_key=True,
                               size=16)
    seat_id = CharField(name='f_seat_id', primary_key=True, size=8)
    order_no = CharField(name='f_order_no', size=16)


class TestCasePackageFpOrder(unittest.TestCase):
    def setUp(self):
        self.db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

    def tearDown(self):
        pass
        # if self.db_conn:
        #     self.db_conn.close()
        #     self.db_conn = None

    @unittest.skip('')
    def test_select(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        rs = Model.select(
            'select * from t_irsk_order where f_order_no=:order_no',
            args={'order_no': '02000014        '},
            db_conn=db_conn)
        print(rs)
        orders = [RiskOrder(**RiskOrder.row_mapper(r)) for r in rs]
        print(orders)

    @unittest.skip('')
    def test_excute(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))
        rows = Model.execute(
            'update t_irsk_order set f_order_no =:order_no where f_local_order_no=:local_order_no',
            args={
                'order_no': '02000015',
                'local_order_no': '81905070000009  '
            },
            db_conn=db_conn)

        db_conn.commit()
        db_conn.close()
        print(rows)

    @unittest.skip('')
    def test_find_where(self):
        with self.db_conn as db_conn:
            RiskOrder._Model__db_conn = db_conn
            # args = {'seat_id': '701211  '}
            orders = RiskOrder.find_where('f_seat_id=:seat_id',
                                          seat_id='701211  ')
            for o in orders:
                print(o)

    @unittest.skip('')
    def test_find_page(self):
        with self.db_conn as db_conn:
            RiskCustRisk._Model__db_conn = db_conn

            orders = RiskCustRisk.find_page(order_by='f_risk_days desc',
                                            risk_days=0)
            for o in orders:
                print(o)

    @unittest.skip('')
    def test_find(self):
        with self.db_conn as db_conn:
            RiskCustRisk._Model__db_conn = db_conn

            orders = RiskCustRisk.find(seat_id='702621')
            for o in orders:
                print(o)

    @unittest.skip('')
    def test_count(self):
        with self.db_conn as db_conn:
            RiskCustRisk._Model__db_conn = db_conn

            cnt = RiskCustRisk.count()
            print('total:', cnt)

            cnt = RiskCustRisk.count(seat_id='702621')
            print('one seat:', cnt)

            cnt = RiskCustRisk.count_where('f_risk_days > :days', days=3)
            print('risk days bigger:', cnt)

    @unittest.skip('')
    def test_save(self):
        with self.db_conn as db_conn:
            RiskOrder._Model__db_conn = db_conn
            order = RiskOrder(local_order_no='123456789',
                              seat_id='703511',
                              order_no='100002')
            order.delete()
            db_conn.commit()
            order.save()
            db_conn.commit()
            # order.order_no = '100003'
            # order.update()
            # db_conn.commit()

    def test_update(self):
        with self.db_conn as db_conn:
            RiskOrder._Model__db_conn = db_conn

            order = RiskOrder.find_one_with_lock(local_order_no='123456789',
                                                 seat_id='703511')

            order.order_no = '100003'
            order.update()
            db_conn.commit()


if __name__ == "__main__":
    unittest.main()