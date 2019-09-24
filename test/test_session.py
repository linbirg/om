#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
test_dir = os.path.dirname(__abs_file__)
om_dir = os.path.dirname(test_dir)
sys.path.append(om_dir)

from yom import Model, CharField, Dao

import unittest

import cx_Oracle as cx


class RiskOrder(Model):
    __table__ = 'T_IRSK_ORDER'

    local_order_no = CharField(name='f_local_order_no',
                               primary_key=True,
                               size=16)
    seat_id = CharField(name='f_seat_id', primary_key=True, size=8)
    order_no = CharField(name='f_order_no', size=16)


class RiskOrderDao(Dao):
    def __init__(self, db_conn):
        return super().__init__(db_conn, RiskOrder)


# def out_method(name):
#     print('now in out_method:%s' % name)

# class Session(object):
#     def __init__(self, *args, **kwargs):
#         return super().__init__(*args, **kwargs)

#     def my_func(self):
#         print('in my func, ok fund.')

#     def call_agent_method(self, agent, method, *args, **kwargs):
#         agent_method = getattr(agent, method)
#         return agent_method(*args, **kwargs)

#     def __getattr__(self, method_name):
#         def _missing(*args, **kwargs):
#             print("A missing method was called.")
#             print("The object was %r, the method was %r. " %
#                   (self, method_name))
#             print("It was called with %r and %r as arguments" % (args, kwargs))

#             # out_method(*args, **kwargs)
#             RiskOrderDao.db_conn = self.db

#             if method_name in dir(RiskOrderDao):
#                 return self.call_agent_method(RiskOrderDao, method_name, *args,
#                                               **kwargs)
#             # return RiskOrderDao.method_name(*args, **kwargs)

#         return _missing


class TestCasePackageFpOrder(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @unittest.skip('')
    def test_handle_req(self):
        s = Dao()
        s.db = cx.connect('%s/%s@%s' %
                          ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))
        s.my_func()

        s.un_know(name='hello 3')

        order = s.find_one(local_order_no='81905070000009', seat_id='701211')
        print('order no:', order.order_no)

        orders = s.find(seat_id='701211')
        for o in orders:
            print(o.local_order_no, o.order_no)

    @unittest.skip('')
    def test_session_select(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        s = Dao(db_conn)

        rs = s.select('select * from t_irsk_order where f_order_no=:order_no',
                      args={'order_no': '02000013        '})
        print(rs)
        orders = [RiskOrderDao(**RiskOrderDao.row_mapper(r)) for r in rs]
        print(orders)

    @unittest.skip('')
    def test_session_update(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        s = Dao(db_conn)
        rows = s.execute(
            'update t_irsk_order set f_order_no =:order_no where f_local_order_no=:local_order_no',
            args={
                'order_no': '02000014',
                'local_order_no': '81905070000009  '
            })

        s.commit()
        print(rows)

    @unittest.skip('')
    def test_find(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        s = RiskOrderDao(db_conn)

        orders = s.find(order_no='02000014')

        print(orders)

        order = s.find_one(order_no='02000014')
        print(order)
        print(order.local_order_no)

    @unittest.skip('')
    def test_count(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        s = RiskOrderDao(db_conn)

        cnt = s.count_where()
        print('total:', cnt)

        cnt = s.count(seat_id='701211')
        print('cnt:', cnt)

    @unittest.skip('')
    def test_pid(self):
        import threading
        print('pid:', os.getpid(), ' login:', os.getlogin(), ' tid:',
              threading.get_ident())

    @unittest.skip('')
    def test_save(self):
        o = RiskOrder(local_order_no='81905070000010',
                      seat_id='703511',
                      order_no='02000012')

        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        dao = RiskOrderDao(db_conn)
        dao.delete(o)
        db_conn.commit()
        dao.save(o)
        db_conn.commit()
        o.order_no = '100001'
        dao.update(o)
        db_conn.commit()

    @unittest.skip('')
    def test_find_one_with_lock(self):
        db_conn = cx.connect(
            '%s/%s@%s' % ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        dao = RiskOrderDao(db_conn)
        order = dao.find_one_with_lock(local_order_no='81905070000009',
                                       seat_id='701211')

        db2 = cx.connect('%s/%s@%s' %
                         ('risk', 'oracle123!', '180.2.31.130:1521/interdb1'))

        dao2 = RiskOrderDao(db2)
        try:
            order2 = dao2.find_one_with_lock(nowait=True,
                                             local_order_no='81905070000009',
                                             seat_id='701211')
            print(order2)
            order2.order_no = '200200'
            db_conn.commit()
        except Exception as e:
            print(e)
            db2.rollback()
        finally:
            db2.close()

        order.order_no = '100100'
        order.update()
        db_conn.commit()


if __name__ == "__main__":
    unittest.main()