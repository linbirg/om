# -*- coding:utf-8 -*-
# Author: yizr

from lib import logger


def log_exception(f):
    '''
    注解不会抛出异常，只会打印出异常堆栈
    '''
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.LOG_WARNING('执行函数[%s.%s]时异常：', f.__module__, f.__name__)
            import traceback
            logger.LOG_WARNING('stacktrace:%s', traceback.format_exc())

    wrapper.__module__ = f.__module__
    wrapper.__name__ = f.__name__

    return wrapper
