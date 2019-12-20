# -*- coding:utf-8 -*-
# Author: yizr

# 适配pgsql的yom

# from . import yom

from .yom import Field, Model, Dao, resource, CharField, ModelMetaClass
from . import logger

import psycopg2 as pg
import psycopg2.extras


# pgsql dialect
class PgCharField(CharField):
    def __init__(self,
                 name=None,
                 primary_key=False,
                 default=None,
                 size=1,
                 desc=""):
        super().__init__(name, primary_key, default, size, desc)


class PgStringField(Field):
    def __init__(self,
                 name=None,
                 primary_key=False,
                 size=255,
                 default=None,
                 desc=""):
        super().__init__(name, "varchar(%d)" % size, primary_key, default,
                         desc)


class PgTextField(Field):
    def __init__(self, name=None, primary_key=False, default=None, desc=""):
        # 从sql标准考虑使用character varying代替text
        super().__init__(name, "character varying", primary_key, default, desc)


class PgIntField(Field):
    def __init__(self, name=None, primary_key=False, default=0, desc=""):
        super().__init__(name, "Integer", primary_key, default, desc)


class PgBigIntField(Field):
    def __init__(self, name=None, primary_key=False, default=0, desc=""):
        super().__init__(name, "bigint", primary_key, default, desc)


class PgRealField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0, desc=""):
        super().__init__(name, "real", primary_key, default, desc)


class PgDoubleField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0, desc=""):
        super().__init__(name, "double precision", primary_key, default, desc)


class PgNumericField(Field):
    def __init__(self,
                 name=None,
                 primary_key=False,
                 default=0.0,
                 size=(18, 2),
                 desc=""):
        super().__init__(name, "numeric(%d,%d)" % size, primary_key, default,
                         desc)


class PgTimeStampField(Field):
    def __init__(self,
                 name=None,
                 primary_key=False,
                 default=None,
                 column_type="TIMESTAMP(6)",
                 desc=""):
        super().__init__(name, column_type, primary_key, default, desc)


class PgModelMetaClass(ModelMetaClass):
    def __new__(cls, name, bases, attrs):
        # 因为Model类是基类，所以排除掉，如果你print(name)的话，会依次打印出Model,User,Blog，即
        # 所有的Model子类，因为这些子类通过Model间接继承元类
        if name == "PgModel":
            return type.__new__(cls, name, bases, attrs)

        # 取出表名，默认与类的名字相同
        tableName = attrs.get("__table__", None) or name
        logger.LOG_TRACE("found model: %s (table: %s)" % (name, tableName))

        mappings = dict()

        fields = []

        pkeys = []

        for k, v in attrs.items():

            if isinstance(v, Field):
                mappings[k] = v
                if v.primary_key:
                    pkeys.append(k)
                else:
                    fields.append(k)

        for k in mappings.keys():
            attrs.pop(k)

        attrs["__mappings__"] = mappings
        attrs["__table__"] = tableName
        attrs["__pKeys__"] = pkeys
        attrs["__fields__"] = fields

        def __get_sql_cols_list(cols):
            return ",".join(
                map(lambda k: "%s" % (mappings.get(k).name or k), cols))

        def __get_sql_params_list(cols):
            return ",".join(
                map(lambda k: "%%(%s)s" % (mappings.get(k).name or k), cols))

        def __get_sql_param_pairs_list(cols):
            return ",".join(
                map(
                    lambda k: "%s=%%(%s)s" %
                    (mappings.get(k).name or k, mappings.get(k).name or k),
                    cols,
                ))

        def __get_sql_where_con_pairs_list(cols):
            return " and ".join(
                map(
                    lambda k: "%s=%%(%s)s" %
                    (mappings.get(k).name or k, mappings.get(k).name or k),
                    cols,
                ))

        # 只是为了Model编写方便，放在元类里和放在Model里都可以
        attrs["__select__"] = "select %s,%s from %s " % (
            __get_sql_cols_list(pkeys),
            __get_sql_cols_list(fields),
            tableName,
        )

        attrs["__update__"] = "update %s set %s where %s" % (
            tableName,
            __get_sql_param_pairs_list(fields),
            __get_sql_where_con_pairs_list(pkeys),
        )
        attrs["__insert__"] = "insert into %s (%s,%s) values (%s,%s)" % (
            tableName,
            __get_sql_cols_list(pkeys),
            __get_sql_cols_list(fields),
            __get_sql_params_list(pkeys),
            __get_sql_params_list(fields),
        )
        attrs["__delete__"] = "delete from %s where %s " % (
            tableName,
            __get_sql_where_con_pairs_list(pkeys),
        )
        attrs["__count__"] = "select count(1) from %s " % tableName

        return type.__new__(cls, name, bases, attrs)


class PgModel(Model, metaclass=PgModelMetaClass):
    def __init__(self, **kw):
        super().__init__(**kw)

    @classmethod
    def get_sql_where_con_pairs_list(cls, cols):
        return " and ".join(
            map(
                lambda k: "%s=%%(%s)s" %
                (cls.__get_key_name__(k), cls.__get_key_name__(k)), cols))


class PgDao(Dao):
    def __init__(self, db_conn, model):
        super().__init__(db_conn, model)

    def __func_create_row__(self, cursor):
        # 列名全部小写，保持跟书写习惯一致。取列也已小写方式读取。
        cols = [d[0].lower() for d in cursor.description]

        def createrow(*args):
            return dict(zip(cols, args))

        return createrow

    def select(self, sql, args=None, db_conn=None, size=None):
        _conn = db_conn if db_conn else self._conn
        cur = _conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if args is None:
            args = {}
        # 用参数替换而非字符串拼接可以防止sql注入
        logger.LOG_DEBUG("db_uuid:%d select sql:%s args:%s" %
                         (id(_conn), sql, args))

        cur.execute(sql, args)
        rs = None
        if size:
            rs = cur.fetchmany(size)
        else:
            rs = cur.fetchall()

        return rs
