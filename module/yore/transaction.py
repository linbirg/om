# -*- coding:utf-8 -*-
# Author: yizr

from contextlib import contextmanager
from lib import dbutil
from lib import logger


@contextmanager
def transcation(db):
    """
    在完成主业务后提交，如果异常会回滚并抛异常。注解会保证最终释放连接。
    """
    assert db

    try:
        yield db
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        if db:
            dbutil.release_connection(db)
            logger.LOG_TRACE("数据库连接[{id}]已经释放.".format(id=id(db)))


@contextmanager
def commit(db):
    assert db

    try:
        yield db
        db.commit()
        logger.LOG_DEBUG("数据库连接[{id}]已经提交.".format(id=id(db)))
    except:
        db.rollback()
        logger.LOG_TRACE("数据库连接[{id}]已经回滚.".format(id=id(db)))
        raise
