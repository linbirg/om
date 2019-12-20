#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
tool_dir = os.path.dirname(os.path.dirname(__abs_file__))
code_dir = os.path.dirname(tool_dir)
sys.path.append(code_dir)

from lib import logger
from lib.yom import Field


class RakeMigrate(object):
    def __init__(self):
        self.db_conn = None

    def execute(self, sql, args=None):
        try:
            cur = self.db_conn.cursor()
            if args is None:
                args = {}
            logger.LOG_TRACE("execute sql:%s args:%s" % (str(sql), args))
            cur.execute(sql, args)
            affected = cur.rowcount
            cur.close()
        except BaseException as e:
            raise e
        return affected

    def create_table(self, name, *columns):
        fields = []
        pkeys = []

        for c in columns:
            if isinstance(c, Field):
                if c.primary_key:
                    pkeys.append(c)
                else:
                    fields.append(c)

        fields = self.__get_fields__(columns)
        primary_keys = self.__get_sort_keys(pkeys)
        ddl_key = ''
        if len(primary_keys.strip()) != 0:
            ddl_key = ',CONSTRAINT %s PRIMARY KEY(%s)' % (
                self.__get_constraint__(name), primary_keys)
        __ddl_create_table = 'create table %s (%s %s)' % (name, fields,
                                                          ddl_key)

        self.execute(__ddl_create_table)

        self.comment_table(name, *columns)
        self.db_conn.commit()

    def comment_table(self, name, *columns, desc=''):
        # desc是对表的注释，暂时不用
        for field in columns:
            comment = "COMMENT on COLUMN %s.%s is '%s'" % (name, field.name,
                                                           field.description)
            self.execute(comment)

    def drop(self, table):

        try:
            __ddl_drop_table__ = 'drop table %s' % table
            # self.db_conn = self.db_conn
            self.execute(__ddl_drop_table__)
            self.db_conn.commit()
        except Exception as e:
            self.db_conn.rollback()
            print('drop table error:', e)
            logger.LOG_FATAL('drop table error:%s' % str(e))

    def change_table_name(self, old_name, new_name):
        __ddl_change_name = 'alter table %s rename to %s' % (old_name,
                                                             new_name)
        self.execute(__ddl_change_name)

    def add_column(self, table, *columns):
        if len(columns) == 0:
            logger.LOG_INFO('没有需要添加的列')
            return

        __ddl_add_columns__ = 'alter table %s add(%s)' % (
            table, self.__get_fields__(columns))
        self.execute(__ddl_add_columns__)

        self.comment_table(table, *columns)

    def drop_column(self, table, column_name):
        __ddl_drop_col__ = 'alter table %s drop(%s)' % (table, column_name)
        self.execute(__ddl_drop_col__)

    def rename_column(self, table, old_col_name, new_col_name):
        __ddl_change_ = 'alter table %s rename column %s to %s' % (
            table, old_col_name, new_col_name)
        self.execute(__ddl_change_)

    def modify_columns(self, table, *columns):
        if len(columns) == 0:
            logger.LOG_INFO('没有需要修改的列')
            return
        __ddl_modify_columns__ = 'alter table %s modify(%s)' % (
            table, self.__get_fields__(columns))
        self.execute(__ddl_modify_columns__)

    def __get_sort_keys(self, keys):
        return ','.join(map(lambda k: '%s' % (k.name or k), keys))

    def __get_fields__(self, columns):
        return ','.join([f.get_ddl() for f in columns])

    def __get_constraint__(self, name):
        return 'P_%s' % name
