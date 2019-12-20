#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
bin_dir = os.path.dirname(__abs_file__)
code_dir = os.path.dirname(bin_dir)
sys.path.append(code_dir)

from module.init.init_context_customer import InitContextCustomer

# from module.init.init_context_customer import InitContextCustomer, InitContextClnFeeRT, InitContextParDB, InitContextITLDB
# from module.init.init_context_customer import InitContextFpDB, InitContextClearRisk
# from module.init.init_context_seat import InitContextSeatCapitalAlarm
from lib import dbutil

from module.init.check_task_can_init import CanInitCheckTask

from lib import logger


class Init(object):
    def __init__(self):
        self.init_db()

    def get_conn(self, db):
        return dbutil.get_connection(db)

    def init_db(self):
        self.conn = dbutil.get_pg_connection('risk_db')
        self.itl_db = self.get_conn('itl_db')
        # self.cln_db = self.get_conn('cln_db')
        # self.par_db = self.get_conn('par_db')
        # self.bpt_db = self.get_conn('bpt_db')

    def release_db(self):
        if self.conn:
            dbutil.release_connection(self.conn)

        if self.itl_db:
            dbutil.release_connection(self.itl_db)

        # if self.cln_db:
        #     dbutil.release_connection(self.cln_db)

        # if self.par_db:
        #     dbutil.release_connection(self.par_db)

        # if self.bpt_db:
        #     dbutil.release_connection(self.bpt_db)

    def check(self):
        checker = CanInitCheckTask(self.conn, self.itl_db)
        return checker.check()

    def run_tasks(self):
        customer = InitContextCustomer(self.conn, self.itl_db)
        customer.run_task()

        # clnFeertTasks = InitContextClnFeeRT(self.conn, self.cln_db)
        # clnFeertTasks.run_task()

        # parDbTasks = InitContextParDB(self.conn, self.par_db)
        # parDbTasks.run_task()

        # itldbTasks = InitContextITLDB(self.conn, self.itl_db)
        # itldbTasks.run_task()

        # fpDbTasks = InitContextFpDB(self.conn, self.bpt_db)
        # fpDbTasks.run_task()

        # clearTask = InitContextClearRisk(self.conn)
        # clearTask.run_task()

        # seatCapitalTasks = InitContextSeatCapitalAlarm(self.conn, self.itl_db)
        # seatCapitalTasks.run_task()

    def main(self, args):
        is_force = False
        if len(args) > 1:
            arg = args[1]
            if arg == '--force':
                is_force = True

        if is_force:
            logger.LOG_INFO('将强制初始化。')

        if is_force or self.check():
            self.run_tasks()
            logger.LOG_INFO('风控完成全部初始化任务。')

        self.release_db()


if __name__ == '__main__':
    init = Init()
    init.main(sys.argv)
