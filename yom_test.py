#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import cx_Oracle as cx
from yom import Model, StringField, DoubleField
import unittest


# fixture
# CREATE TABLE "T_ITL_RISK_CUST"
#    (	"F_ID" VARCHAR2(20 BYTE) NOT NULL ENABLE,
# 	"F_NAME" VARCHAR2(100 BYTE),
# 	"F_TYPE" CHAR(2 BYTE),
# 	"F_LEVEL" NUMBER(10,0),
# 	 CONSTRAINT "P_ITL_RISK_CUST" PRIMARY KEY ("F_ID")
#    )
# insert into t_itl_risk_cust(f_id,f_name,f_type,f_level)values('0001','gh','01',2);
# insert into t_itl_risk_cust(f_id,f_name,f_type,f_level)values('0002','gh2','01',3);
# insert into t_itl_risk_cust(f_id,f_name,f_type,f_level)values('0003','gh','01',3);
class Cust(Model):
    __table__ = 't_itl_risk_cust'
    id = StringField('f_id', True)
    name = StringField('f_name')
    custType = StringField('f_type')
    level = DoubleField('f_level')


# fixture
# CREATE TABLE "t_itl_risk_client"
#    (	"F_ID" VARCHAR2(20 BYTE) NOT NULL ENABLE,
#     "F_seat" VARCHAR2(20 BYTE) NOT NULL ENABLE,
# 	"F_NAME" VARCHAR2(100 BYTE),
# 	 CONSTRAINT "P_ITL_RISK_CLIENT" PRIMARY KEY ("F_ID","F_seat")
#    )
# insert into t_itl_risk_client(f_id,f_seat,f_name)values('001','0001','yzr');
# insert into t_itl_risk_client(f_id,f_seat,f_name)values('002','0001','sun');
# insert into t_itl_risk_client(f_id,f_seat,f_name)values('003','0002','yyb');
# insert into t_itl_risk_client(f_id,f_seat,f_name)values('003','0001','lyq');


class Client(Model):
    __table__ = 't_itl_risk_client'
    id = StringField('f_id', True)
    seat = StringField('f_seat', True)
    name = StringField('f_name')


class TestCaseYOM(unittest.TestCase):
    def setUp(self):
        self.conn = self.get_conn()
        Cust.db_conn = self.conn
        Client.db_conn = self.conn

    def tearDown(self):
        self.conn.close()

    def get_conn(self):
        # return dbutil.get_connection('itl_old_db')
        conn = cx.connect('%s/%s@%s' % ('user', 'passwd', 'ip:1521/sid'))
        return conn

    def test_find(self):
        one = Cust.find('0001')[0]
        self.assertEqual(one.id, '0001')
        self.assertEqual(one.name, 'gh')
        self.assertEqual(one.level, 2)

        alls = Cust.find(name='gh')
        self.assertEqual(2, len(alls))
        print(alls[1])

        alls = Cust.find(name='gh', level=2)
        self.assertEqual(1, len(alls))
        self.assertEqual(alls[0].id, '0001')

        one = Cust.find(id='0003')[0]
        self.assertEqual(one.level, 3)

    def test_find_all(self):
        alls = Client.find_all()
        self.assertEqual(4, len(alls))

        alls = Cust.find_all()
        self.assertEqual(3, len(alls))

    def test_mult_pks_find(self):
        one = Client.find(id='003', seat='0001')[0]
        self.assertEqual(one.name, 'lyq')

        one = Client.find(id='003', name='yyb')[0]
        self.assertEqual('0002', one.seat)

    def test_count(self):
        cnt = Cust.count()
        self.assertEqual(3, cnt)

        cnt = Client.count()
        self.assertEqual(4, cnt)

        cnt = Client.count(seat='0001')
        self.assertEqual(3, cnt)

        cnt = Client.count(seat='0002')
        self.assertEqual(1, cnt)

        cnt = Client.count(seat='0001', id='003')
        self.assertEqual(1, cnt)

        cnt = Client.count(seat='0001', name='yzr')
        self.assertEqual(1, cnt)

        cnt = Client.count(seat='0001', name='yzr', id='003')
        self.assertEqual(0, cnt)

    def test_find_one(self):
        one = Client.find_one(seat='0001', name='yzr')
        self.assertEqual(one.id, '001')

        one = Client.find_one(seat='0001', id='003')
        self.assertEqual(one.name, 'lyq')

    @unittest.expectedFailure
    def test_find_one_exception(self):
        # 查询条件为多个值时，函数返回第一条数据。具体那一条，则无法确定，可能oracle不能实现或者排序的不同，导致不同。
        # 所以在有多个条件时，不要使用这个函数。
        one = Client.find_one(seat='0001')
        print(one)

    def test_cust_save_and_del(self):
        cust = Cust.find_one('0001')
        print(cust)
        self.assertEqual(cust.level, 2)

        cust.level = 10
        cust.id = '0004'
        print('after:', cust)
        cust.save()

        self.conn.commit()

        aCust = Cust.find_one('0004')
        self.assertEqual(aCust.level, 10)
        print(aCust)
        aCust.delete()
        self.conn.commit()
        self.assertIsNone(Cust.find_one('0004'))


if __name__ == "__main__":
    unittest.main()
