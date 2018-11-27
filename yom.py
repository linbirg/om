# !/usr/bin/python3
# -*- coding:utf8 -*-
# Author: yizr

# 1.orm 是什么
#  ORM 即Object Relational Mapping，全称对象关系映射
# 2.orm的好处
#  i.对于大部分的增删改查的表操作，orm实现表行到类和对象的一一映射，从而省区重复的、构造sql的麻烦，从而简单操作只需要调用方法，不需要sql即可完成。
#  ii.屏蔽数据库细节，提高开发效率，便于数据库迁移。
# 3.目前orm框架的缺点
#   现有sqlalchemy,peewee等太重，性能也有问题。
# 4.诉求
#   我只需要简单的om，和简单的crud的封装。raw sql有其便利性，需要保留支持。

# 这里参照了简书上的两种实现，分别为https://www.jianshu.com/p/042abf8918fc与https://www.jianshu.com/p/ac8a9bb57ec3。
# 这里主要参考后面一种，并改为同步的oracle版本，后续考虑异步版本。

import logging


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name  # 字段名
        self.column_type = column_type  # 字段数据类型
        self.primary_key = primary_key  # 是否是主键
        self.default = default  # 有无默认值

    def __str__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name)


class StringField(Field):
    def __init__(self,
                 name=None,
                 primary_key=False,
                 default=None,
                 ddl='varchar2(255)'):
        super(StringField, self).__init__(name, ddl, primary_key, default)


class CharField(Field):
    def __init__(self, name=None, primary_key=False, default=None, size=1):
        super(CharField, self).__init__(name, 'CHAR(%d)' % size, primary_key,
                                        default)


class DoubleField(Field):
    def __init__(self,
                 name=None,
                 primary_key=False,
                 default=None,
                 size=(18, 0)):
        super(DoubleField, self).__init__(name, 'NUMBER(%d,%d)' % size,
                                          primary_key, default)


# 其它字段略，一个道理，一个模式


class ModelMetaClass(type):
    # 元类必须实现__new__方法，当一个类指定通过某元类来创建，那么就会调用该元类的__new__方法
    # 该方法接收4个参数
    # cls为当前准备创建的类的对象
    # name为类的名字，创建User类，则name便是User
    # bases类继承的父类集合,创建User类，则base便是Model
    # attrs为类的属性/方法集合，创建User类，则attrs便是一个包含User类属性的dict
    def __new__(cls, name, bases, attrs):
        # 因为Model类是基类，所以排除掉，如果你print(name)的话，会依次打印出Model,User,Blog，即
        # 所有的Model子类，因为这些子类通过Model间接继承元类
        if name == "Model":
            return type.__new__(cls, name, bases, attrs)
        # 取出表名，默认与类的名字相同
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 用于存储所有的字段，以及字段值
        mappings = dict()
        # 仅用来存储非主键意外的其它字段，而且只存key
        fields = []
        # 仅保存主键的key
        # primaryKey = None
        pkeys = []
        # 注意这里attrs的key是字段名，value是字段实例，不是字段的具体值
        # 比如User类的id=StringField(...) 这个value就是这个StringField的一个实例，而不是实例化
        # 的时候传进去的具体id值
        for k, v in attrs.items():
            # attrs同时还会拿到一些其它系统提供的类属性，我们只处理自定义的类属性，所以判断一下
            # isinstance 方法用于判断v是否是一个Field
            if isinstance(v, Field):
                mappings[k] = v
                if v.primary_key:
                    # if primaryKey:
                    #     # TODO 考虑改为可以多个key
                    #     raise RuntimeError(
                    #         "Douplicate primary key for field :%s" % key)
                    # primaryKey = k
                    pkeys.append(k)
                else:
                    fields.append(k)
        # 保证了必须有一个主键
        if len(pkeys) == 0:
            raise RuntimeError("Primary key not found")
        # 这里的目的是去除类属性，为什么要去除呢，因为我想知道的信息已经记录下来了。
        # 去除之后，就访问不到类属性了
        # 记录到了mappings,fields，pkeys等变量里，而我们实例化的时候，如
        # user=User(id='10001') ，为了防止这个实例变量与类属性冲突，所以将其去掉
        for k in mappings.keys():
            attrs.pop(k)
        # 以下都是要返回的东西了，刚刚记录下的东西，如果不返回给这个类，又谈得上什么动态创建呢？
        # 到此，动态创建便比较清晰了，各个子类根据自己的字段名不同，动态创建了自己
        # 下面通过attrs返回的东西，在子类里都能通过实例拿到，如self
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__pKeys__'] = pkeys
        attrs['__fields__'] = fields

        def __get_sql_cols_list(cols):
            return ','.join(
                map(lambda k: '%s' % (mappings.get(k).name or k), cols))

        def __get_sql_params_list(cols):
            return ','.join(
                map(lambda k: ':%s' % (mappings.get(k).name or k), cols))

        def __get_sql_param_pairs_list(cols):
            return ','.join(
                map(lambda k: '%s=:%s' % (mappings.get(k).name or k, mappings.get(k).name or k), cols))

        def __get_sql_where_con_pairs_list(cols):
            return ' and '.join(
                map(lambda k: '%s=:%s' % (mappings.get(k).name or k, mappings.get(k).name or k), cols))

        # 只是为了Model编写方便，放在元类里和放在Model里都可以
        attrs['__select__'] = "select %s,%s from %s " % (
            __get_sql_cols_list(pkeys), __get_sql_cols_list(fields), tableName)

        attrs['__update__'] = "update %s set %s where %s" % (
            tableName, __get_sql_param_pairs_list(fields),
            __get_sql_where_con_pairs_list(pkeys))
        attrs['__insert__'] = "insert into %s (%s,%s) values (%s,%s)" % (
            tableName, __get_sql_cols_list(pkeys), __get_sql_cols_list(fields),
            __get_sql_params_list(pkeys), __get_sql_params_list(fields))
        attrs['__delete__'] = "delete from %s where %s " % (
            tableName, __get_sql_where_con_pairs_list(pkeys))
        attrs['__count__'] = 'select count(1) from %s ' % tableName

        # primaryKey_name = mappings.get(primaryKey).name or primaryKey
        # attrs['__select__'] = "select %s,%s from %s " % (
        #     primaryKey_name, ','.join(
        #         map(lambda f: '%s' % (mappings.get(f).name or f),
        #             fields)), tableName)
        # attrs['__update__'] = "update %s set %s where %s=:%s" % (tableName, ', '.join(
        #     map(
        #         lambda f: '%s=:%s' % (mappings.get(f).name or f, mappings.get(f).name or f),
        #         fields)), primaryKey_name, primaryKey_name)
        # attrs['__insert__'] = "insert into %s (%s,%s) values (:%s,%s)" % (
        #     tableName, primaryKey_name, ','.join(
        #         map(lambda f: '%s' % (mappings.get(f).name or f), fields)),
        #     primaryKey_name, ','.join(
        #         map(lambda k: ':%s' % (mappings.get(k).name or k), fields)))
        # attrs['__delete__'] = "delete from %s where %s=:%s " % (
        #     tableName, primaryKey_name, primaryKey_name)
        return type.__new__(cls, name, bases, attrs)


# 让Model继承dict,主要是为了具备dict所有的功能，如get方法
# metaclass指定了Model类的元类为ModelMetaClass
class Model(dict, metaclass=ModelMetaClass):
    db_conn = None

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # 实现__getattr__与__setattr__方法，可以使引用属性像引用普通字段一样  如self['id']
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    # 貌似有点多次一举
    def getValue(self, key):
        value = getattr(self, key, None)
        return value

    # 取默认值，上面字段类不是有一个默认值属性嘛，默认值也可以是函数
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(
                    field.default) else field.default
                # setattr(self, key, value) # get函数不写属性。
        return value

    def __get_args__(self, keys):
        args = {}
        for key in keys:
            if key not in self.__mappings__:
                raise RuntimeError("field not found")
        for key in keys:
            args[self.__get_key_name__(key)] = self.getValueOrDefault(key)

        return args

    # 下面 self.__mappings__,self.__insert__等变量据是根据对应表的字段不同，而动态创建
    def save(self):
        return self.execute(self.__insert__,
                            self.__get_args__(self.__mappings__.keys()))

    def delete(self):
        # args = []
        # args.append(self[self.__primaryKey__])
        # args = {
        #     self.get_primaryKey_name():
        #     self.getValueOrDefault(self.__primaryKey__)
        # }
        # print(self.__delete__)
        return self.execute(self.__delete__, self.__get_args__(self.__pKeys__))

    def update(self):
        # args = {}
        # for key in kw:
        #     if key not in self.__fields__:
        #         raise RuntimeError("field not found")
        # for key in self.__fields__:
        #     if key in kw:
        #         args[key] = kw[key]
        #         # args.append(kw[key])
        #     else:
        #         args[key] = getattr(self, key, None)
        #         # args.append(getattr(self, key, None))

        # args.append(getattr(self, self.__primaryKey__))
        # args = self.__get_args__(self.__fields__)

        return self.execute(self.__update__,
                            self.__get_args__(self.__mappings__.keys()))

    @classmethod
    def select(cls, sql, args, size=None):
        try:
            cur = cls.db_conn.cursor()
            # 用参数替换而非字符串拼接可以防止sql注入
            print("select sql:", sql, " args:", args)
            cur.execute(sql, args)
            if size:
                rs = cur.fetchmany(size)
            else:
                rs = cur.fetchall()
            cur.close()
        except BaseException as e:
            raise e
        return rs

    @classmethod
    def execute(cls, sql, args):
        try:
            cur = cls.db_conn.cursor()
            print("execute sql:", sql, "args:", args)
            cur.execute(sql, args)
            affected = cur.rowcount
            cur.close()
        except BaseException as e:
            raise e
        return affected

    @classmethod
    def tuple_2_map(cls, tp):
        obj = {}
        for i in range(len(cls.__pKeys__)):
            # 基于select时候，key都是按顺序排在前面，参考attrs['__select__']。
            obj[cls.__pKeys__[i]] = tp[i]

        j = len(cls.__pKeys__)
        for i in range(len(cls.__fields__)):
            f = cls.__fields__[i]
            obj[f] = tp[i + j]

        return obj

    @classmethod
    def __get_key_name__(cls, key):
        if key not in cls.__mappings__:
            raise RuntimeError("key not found")
        key_name = cls.__mappings__.get(key).name or key
        return key_name

    @classmethod
    def __join__(cls, format, cols, spliter=','):
        return spliter.join(
            map(lambda k: format % (cls.__get_key_name__(k)), cols))

    @classmethod
    def __get_sql_cols_list(cls, cols, spliter=','):
        # return spliter.join(
        #     map(lambda k: '%s' % (cls.__mappings__.get(k).name or k), cols))
        return cls.__join__('%s', cols)

    @classmethod
    def __get_sql_params_list(cls, cols):
        # return ','.join(
        #     map(lambda k: ':%s' % (cls.__mappings__.get(k).name or k), cols))
        return cls.__join__(':%s', cols)

    @classmethod
    def __get_sql_where_con_pairs_list(cls, cols):
        return ' and '.join(
            map(lambda k: '%s=:%s' % (cls.__get_key_name__(k), cls.__get_key_name__(k)),
                cols))

    # 类方法
    @classmethod
    def find(cls, pk=None, **pks):
        # 如果只有1个主键，可以find('ag(T+D)')或者find(id='ag(T+D)')，
        # 如果有多个key，必须使用find(id='ag(T+D)',market='02')(假设market也是主键)

        # pk与pks必须有一个为空
        if pk is not None and len(pks) > 0:
            raise RuntimeError("find：参数错误，pk与pks必须有一个为空。")

        keys = []
        args = {}
        if pk is not None:
            args = {cls.__get_key_name__(cls.__pKeys__[0]): pk}
            keys.append(cls.__pKeys__[0])

        if pks is not None or len(pks) > 0:
            for k, v in pks.items():
                args[cls.__get_key_name__(k)] = v
                keys.append(k)

        if len(keys) == 0:
            rs = cls.select(cls.__select__, args)
        else:
            rs = cls.select(
                '%s where %s' % (cls.__select__,
                                 cls.__get_sql_where_con_pairs_list(keys)),
                args)
        if len(rs) == 0:
            return None
        # print(rs)
        return [cls(**cls.tuple_2_map(r)) for r in rs]  # 返回的是一个实例对象引用

    @classmethod
    def find_one(cls, pk=None, **pks):
        rets = cls.find(pk, **pks)
        if rets is None:
            return None

        if len(rets) > 1:
            raise RuntimeError("find_one：应该返回一条数据，但是返回了多条数据。")
        return rets[0]

    @classmethod
    def find_all(cls):
        return cls.find()

    @classmethod
    def count(cls, **pks):
        args = {}
        keys = []

        # __count__ = 'select count(1) from %s ' % cls.__table__

        if pks is not None or len(pks) > 0:
            for k, v in pks.items():
                args[cls.__get_key_name__(k)] = v
                keys.append(k)

        if len(keys) == 0:
            rs = cls.select(cls.__count__, args, size=1)
        else:
            rs = cls.select(
                '%s where %s' % (cls.__count__,
                                 cls.__get_sql_where_con_pairs_list(keys)),
                args,
                size=1)
        return rs[0][0]
