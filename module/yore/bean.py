#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

from lib import dbutil
from lib import logger

# def linkp(f):
#     '''链式编程，会在函数调用完成后返回self。'''
#     def wrapper(self, *args, **kwargs):
#         f(self, *args, **kwargs)
#         return self

#     return wrapper


class RiskBaseBean():
    def __init__(self):
        self.__dbs = {}

    def _get_db(self, name):
        return dbutil.get_connection(name)

    def fetch_db(self, name):
        '''获取为name的数据库连接，并设置name属性。
           设置之后可以通过self.name来获取连接.
        eg:
          self.fetch_db('risk_db')
          print(self.risk_db)
        '''
        setattr(self, name, self._get_db(name))
        self.__dbs[name] = getattr(self, name)  # 用于释放资源

    @classmethod
    def db(cls, name):
        '''该注解在函数执行之前检查是否存在name属性(eg，如果name为risk_db，则检查self.risk_db是否存在。)或者为None。
            如果不存在或者为None，会调用一次fetch_db，该函数会设置name属性。
        '''
        def decrotor(f):
            def wrapper(self, *args, **kwargs):
                if not hasattr(self, name) or getattr(self, name) is None:
                    self.fetch_db(name)

                return f(self, *args, **kwargs)

            wrapper.__module__ = f.__module__
            wrapper.__name__ = f.__name__

            return wrapper

        return decrotor

    @classmethod
    def realsedb(cls, f):
        '''会将所有调用fetch_db获取的数据库连接都释放。
        '''
        def wraper(self, *args, **kwargs):
            ret = None

            try:
                ret = f(self, *args, **kwargs)
            except:
                raise
            finally:
                self.realse_dbs()

            return ret

        wraper.__module__ = f.__module__
        wraper.__name__ = f.__name__

        return wraper

    def realse_dbs(self):
        for db in self.__dbs:
            try:
                dbutil.release_connection(self.__dbs[db])
            except Exception as e:
                logger.LOG_WARNING('释放数据库连接时异常[%s]', str(e))
                import traceback
                logger.LOG_WARNING('stacktrace:%s', traceback.format_exc())

        self.__dbs = {}
        logger.LOG_TRACE('所有的数据库连接已处理。')

    def __del__(self):
        '''对象删除时会释放所有数据连接'''
        self.realse_dbs()


# 提供通过模块引入注解的便利
def db(name):
    '''该注解在函数执行之前检查是否存在name属性(eg，如果name为risk_db，则检查self.risk_db是否存在。)或者为None。
            如果不存在或者为None，会调用一次fetch_db，该函数会设置name属性。
    '''
    return RiskBaseBean.db(name)


def realsedb(f):
    '''会将所有调用fetch_db获取的数据库连接都释放。
        注解会对传入的参数根据f的签名进行过滤。
    '''
    return RiskBaseBean.realsedb(f)
