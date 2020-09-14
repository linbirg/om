#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

from lib import concurrent_dict

from lib.yom import Dao, Model
from lib import logger

from module.yore.exception import log_exception
from module.yore.transaction import commit


class SHMTableCache():
    def __init__(self, table_name, capacity, duration, pkeys=None):
        '''
        ### desc
        - 利用SHM实现的库表缓存。

        ### para
        - capacity:缓存大小（注：如果启动项[shmload_conf.create_dicts]配置了同名缓存，此参数不生效）
        - duration:缓存有效期，单位秒
        
        '''
        self._name = table_name
        self._pkeys = pkeys
        self._capacity = capacity
        self._duration = duration

        self._cache = self.__init_cache__(self._name, capacity)

    def __init_cache__(self, name, capacity):
        try:
            return concurrent_dict.get(self._name)
        except Exception as e:
            logger.LOG_TRACE('获取缓存[%s]异常，可能缓存未创建:%s', self._name, str(e))
            # import traceback
            # traceback.print_exc()
            logger.LOG_TRACE('尝试新建缓存...')
            return concurrent_dict.create(self._name, capacity)

    def __filter_by_pkeys(self, pks, pkeys):
        assert pkeys
        filterd_keys = [pks[k] for k in pkeys if k in pks]

        # 做trip处理，保证参数不论是否含空格，key一致
        filterd_keys = [
            k.strip() if type(k) == str else k for k in filterd_keys
        ]
        filterd_keys.sort()  # 按pkeys字母顺序以防止字典的无序性。

        return filterd_keys

    def __get_pks_str(self, **pks):
        keys = pks.values()

        if self._pkeys:
            keys = self.__filter_by_pkeys(pks, self._pkeys)

        return '_'.join(map(str, keys))

    def __get_pks_str_by_data(self, data):
        return self.__get_pks_str(**data)

    def get(self, **pks):
        pks_str = self.__get_pks_str(**pks)
        logger.LOG_DEBUG('pks_str:%s cache name:%s', pks_str, self._name)
        item = self._cache.get(pks_str)
        if item is None:
            logger.LOG_DEBUG('no data found.')
            return None

        data, stamp = item
        logger.LOG_DEBUG('find data：%s', str(data))
        logger.LOG_DEBUG('timestamp:%d', stamp)
        import time
        to_now_sec = int(time.time()) - int(stamp)  # to_sec
        logger.LOG_DEBUG('to_now_sec:%d', to_now_sec)
        if to_now_sec > self._duration:
            logger.LOG_DEBUG('data expired.')
            return None

        return data

    @log_exception
    def put(self, data):
        if data is None:
            return

        key = self.__get_pks_str_by_data(data)
        logger.LOG_DEBUG('put key:%s', key)
        logger.LOG_DEBUG('put data:%s', str(data))

        import time
        logger.LOG_DEBUG('time now:%d', time.time())
        self._cache.set(key, (data, int(time.time())))

    @log_exception
    def delete(self, data):
        if data is None:
            return

        key = self.__get_pks_str_by_data(data)
        logger.LOG_DEBUG('delete key:%s', key)
        self._cache.pop(key)


class CacheDao():
    def __init__(self, db_dao, capacity=10000, duration=1 * 60 * 60):
        assert db_dao
        self._db_dao = db_dao

        self.cache_table = SHMTableCache(self._db_dao.model.__table__,
                                         pkeys=self._db_dao.model.__pKeys__,
                                         capacity=capacity,
                                         duration=duration)

    def find_one(self, **pks):
        cache_data = None
        try:
            cache_data = self.cache_table.get(**pks)
        except Exception as e:
            logger.LOG_WARNING('读取缓存数据时异常:%s', str(e))

        if cache_data is not None:
            logger.LOG_DEBUG('find data in[%s]:%s', self.cache_table._name,
                             str(cache_data))
            return cache_data

        db_data = self._db_dao.find_one(**pks)

        self.cache_table.put(db_data)
        return db_data

    def save(self, obj):
        ret = self._db_dao.save(obj)
        # 当有数据库新增或者修改时，将会使缓存失效，在下次查询时缓存会被更新
        # 这样做的目的是为了避免不必要的类型转换还能保持缓存的数据类型与数据库一致
        self.cache_table.delete(obj)
        return ret

    def update(self, obj):
        ret = self._db_dao.update(obj)
        self.cache_table.delete(obj)
        return ret

    def delete(self, obj):
        ret = self._db_dao.delete(obj)
        self.cache_table.delete(obj)
        return ret

    def __getattr__(self, name):
        """这个方法在访问的attribute不存在的时候被调用
        the __getattr__() method is actually a fallback method
        that only gets called when an attribute is not found"""
        logger.LOG_DEBUG('CacheDao中没有改属性:%s', name)
        return getattr(self._db_dao, name)


class CacheWithPara():
    def __init__(self, capacity=10000, duration=1 * 60 * 60):
        self._capacity = capacity
        self._duration = duration

    def __call__(self, dao_cls):
        self.dao_cls = dao_cls

        def decorator(db_conn, model=None):
            if model is None:
                self.db_dao = self.dao_cls(db_conn)
            else:
                self.db_dao = self.dao_cls(db_conn, model)
            self.cache_dao = CacheDao(self.db_dao,
                                      capacity=self._capacity,
                                      duration=self._duration)
            return self.cache_dao

        return decorator


class CacheNoPara():
    def __init__(self, dao_cls):
        self.dao_cls = dao_cls
        self.db_dao = None

    def __call__(self, db_conn, model=None):
        if model is None:
            self.db_dao = self.dao_cls(db_conn)
        else:
            self.db_dao = self.dao_cls(db_conn, model)
        self.cache_dao = CacheDao(self.db_dao)
        return self.cache_dao


# 大小 过期时间
def cache(cls_dao=None, *, capacity=10000, duration=1 * 60 * 60):
    '''
    ### desc
    用于注解Dao的子类，以使Dao具有缓存功能。可带参数或者不带参数（不带参数，取默认值）。

    ### para
    - capacity:缓存大小（注：如果启动项[shmload_conf.create_dicts]配置了同名缓存，此参数不生效), 默认1万
    - duration:缓存有效期，单位秒, 默认1小时
    '''
    if cls_dao is None:
        return CacheWithPara(capacity=capacity, duration=duration)

    return CacheNoPara(cls_dao)