import sys
import os

__abs_file__ = os.path.abspath(__file__)
test_dir = os.path.dirname(__abs_file__)
om_dir = os.path.dirname(test_dir)
sys.path.append(om_dir)

import unittest

import threading
import time


class classproperty:
    def __init__(self, method):
        self.method = method

    def __get__(self, instance, owner):
        return self.method(owner)


class Billow:
    _db_conn = 0

    _lock = threading.RLock()

    @classproperty
    def lname(cls):
        return cls._db_conn

    @classmethod
    def set_db(cls):
        index = threading.get_ident()
        # print('before:', cls._db_conn)
        with cls._lock:
            cls._db_conn = index
        # print('after:', cls._db_conn)


def set_db_loop():
    while True:
        Billow.set_db()
        time.sleep(0.4)


class TestCasePackageFpOrder(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @unittest.skip('')
    def test_class_property(self):
        print(Billow.lname)

    def test_class_property_scope(self):
        t1 = threading.Thread(target=set_db_loop)
        t2 = threading.Thread(target=set_db_loop)
        t1.start()
        t2.start()

        # t1.join(60)
        # t2.join(60)
        while True:
            print(Billow.lname)
            billow = Billow()
            print(billow.lname)
            time.sleep(0.5)


if __name__ == "__main__":
    unittest.main()