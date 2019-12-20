#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import sys
import os

__abs_file__ = os.path.abspath(__file__)
tool_dir = os.path.dirname(os.path.dirname(__abs_file__))
code_dir = os.path.dirname(tool_dir)
sys.path.append(code_dir)

from lib import dbutil

from module.dao.risk.risk_notify_notify_switch import RiskCfgNotifySwitchDao, RiskCfgNotifySwitch
from module.dao.risk.risk_notify_para import RiskCfgNotifyParaDao, RiskCfgNotifyPara

from module.yore import transaction as tx

from lib import logger


def get_db_conn():
    conn = dbutil.get_pg_connection('risk_db')
    return conn


def fixture_switch():
    conn = get_db_conn()
    with tx.transcation(conn):
        switchDao = RiskCfgNotifySwitchDao(conn)

        switchDao.delete_all()
        switch = RiskCfgNotifySwitch(seat_id='$', switch='1')
        switchDao.save(switch)


def query_switch():
    conn = get_db_conn()
    with tx.transcation(conn):
        switchDao = RiskCfgNotifySwitchDao(conn)

        alls = switchDao.find_all()

        for switch in alls:
            print(switch)
            print("seat_id ", switch.seat_id)


def fixture_nty_para():

    conn = get_db_conn()

    def gen_par(seat_id, prompt_type, content, src, target, title=''):
        par = RiskCfgNotifyPara(seat_id=seat_id,
                                prompt_type=prompt_type,
                                grade_src=src,
                                grade_target=target,
                                prompt_context=content,
                                title=title)
        return par

    with tx.transcation(conn):
        paraDao = RiskCfgNotifyParaDao(conn)
        paraDao.delete_all()

        par = gen_par('$', 1, '恭喜您,您的风险等级已经从追债转到正常状态。', 61, 11, '风险解除提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从爆仓转到绝对强平状态。', 51, 41, '风险等级降低提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从爆仓转到相对强平状态。', 51, 31, '风险等级降低提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从爆仓转到告警状态。', 51, 21, '爆仓风险转为告警风险')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从爆仓转到正常状态。', 51, 11, '风险解除提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从绝对强平转到相对强平状态。', 41, 31, '风险等级降低提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从绝对强平转到告警状态。', 41, 21, '强平风险转为告警风险')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从绝对强平转到正常状态。', 41, 11, '风险解除提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从相对强平转到告警状态。', 31, 21, '强平风险转为告警风险')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从相对强平转到正常状态。', 31, 11, '风险解除提示')
        paraDao.save(par)
        par = gen_par('$', 1, '恭喜您,您的风险等级已经从告警转到正常状态。', 21, 11, '风险解除提示')
        paraDao.save(par)
        par = gen_par('$', 2, '尊敬的客户,您好,您的风险等级已经为告警状态,请及时补足资金。', 11, 21,
                      '告警通知')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从正常转到相对强平状态,请及时补足资金。', 11, 31,
                      '强平通知')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从正常转到绝对强平状态,请及时补足资金。', 11, 41,
                      '强平通知')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从正常转到爆仓状态,请及时补足资金。', 11, 51,
                      '风险转为爆仓风险')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从告警转到相对强平状态,请及时补足资金。', 21, 31,
                      '强平通知')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从告警转到绝对强平状态,请及时补足资金。', 21, 41,
                      '强平通知')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从告警转到爆仓状态,请及时补足资金。', 21, 51,
                      '风险转为爆仓风险')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从相对强平转到绝对强平状态,请及时补足资金。', 31, 41,
                      '强平通知')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从相对强平转到爆仓状态,请及时补足资金。', 31, 51,
                      '风险转为爆仓风险')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从绝对强平转到爆仓状态,请及时补足资金。', 41, 51,
                      '风险转为爆仓风险')
        paraDao.save(par)
        par = gen_par('$', 3, '尊敬的客户您好,您的风险等级已经从爆仓转到追债状态,请及时补足资金。', 51, 61,
                      '爆仓风险转为追债风险')
        paraDao.save(par)


if __name__ == '__main__':
    logger.logger.set_output_level(0)

    fixture_switch()
    query_switch()
    # fixture_nty_para()
