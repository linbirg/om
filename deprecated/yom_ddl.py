#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import cx_Oracle as cx
from yom import Model, StringField, DoubleField, DDL
import unittest

# class Inst(Model):
#     __table__ = 'Prod_code_def'
#     id = StringField('prod_code', True)
#     # name = StringField('prod_name')
#     bourseId = StringField('BOURSE_ID')
#     currency = StringField('CURRENCY_ID')
#     marketId = StringField('MARKET_ID')
#     varietyType = StringField('VARIETY_TYPE')
#     varietyId = StringField('VARIETY_ID')
#     tick = DoubleField('TICK')


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
    id = StringField('f_id', True, ddl='char(12)')
    seat = StringField('f_seat', True, ddl='char(8)')
    name = StringField('f_name')


class TestCaseYOM(unittest.TestCase):
    def setUp(self):
        self.conn = self.get_conn()
        # Inst.db_conn = self.conn
        Cust.db_conn = self.conn
        Client.db_conn = self.conn

    def tearDown(self):
        self.conn.close()

    def get_conn(self):
        # return dbutil.get_connection('itl_old_db')
        conn = cx.connect('%s/%s@%s' % ('user', 'passwd', 'ip:1521/sid'))
        return conn

    def test_create_table(self):
        ddl = DDL(Client, 'P_ITL_RISK_CLIENT')
        ddl.drop_table()
        ddl.create_table()
        # insert into t_itl_risk_client(f_id,f_seat,f_name)values('001','0001','yzr');
        # insert into t_itl_risk_client(f_id,f_seat,f_name)values('002','0001','sun');
        # insert into t_itl_risk_client(f_id,f_seat,f_name)values('003','0002','yyb');
        # insert into t_itl_risk_client(f_id,f_seat,f_name)values('003','0001','lyq');
        clt1 = Client(id='001', seat='0001', name='yzr')
        clt2 = Client(id='002', seat='0001', name='sun')
        clt3 = Client(id='003', seat='0002', name='yyb')
        clt4 = Client(id='004', seat='0001', name='lyq')

        clt1.delete()
        clt2.delete()
        clt3.delete()
        clt4.delete()

        clt1.save()
        clt2.save()
        clt3.save()
        clt4.save()

        self.conn.commit()


if __name__ == "__main__":
    unittest.main()
