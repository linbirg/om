#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import os
import sys

__abs_file__ = os.path.abspath(__file__)
migrate_dir = os.path.dirname(__abs_file__)
tool_dir = os.path.dirname(migrate_dir)
code_dir = os.path.dirname(tool_dir)
sys.path.append(code_dir)

import re
import importlib
import importlib.util

# from lib.yom import DDL, Model

# from lib import dbutil as db
from tools.migrate.rake_migrate import RakeMigrate as Migrate

# class Rake(object):
#     def __init__(self):
#         self.__regx__ = r'(.*)(_)([0-9]+)(\.py)$'
#         self.__p__ = re.compile(self.__regx__)
#         return super().__init__()

#     def rake(self):
#         for task in self.tasks:
#             task.rake()


def get_current_path():
    # __abs_file__ = os.path.abspath(__file__)
    # __cur_path__ = os.path.dirname(__abs_file__)
    __cur_path__ = os.getcwd()
    return __cur_path__


def dir_file(path):
    pathDir = os.listdir(path)
    return pathDir


# 下划线[num]_.py结尾
def is_name_numberd(name):
    regx = r'^([0-9]+)(_)(.*)(\.py)$'
    ma = re.match(regx, name)
    return True if ma else False


def parse_number(name):
    regx = r'^([0-9]+)(_)(.*)(\.py)$'
    ma = re.match(regx, name)
    if ma:
        return int(ma.group(1))


def parse_module_name(file_name):
    regx = r'^(.*)(\.py)$'
    ma = re.match(regx, file_name)
    if not ma:
        return None

    return ma.group(1)


def _sort_(list_names):
    return sorted(list_names, key=lambda n: parse_number(n))


def list_all_migration_files(path):
    files = dir_file(path)
    migs = list(filter(lambda f: is_name_numberd(f), files))
    return migs


def check_module(module_name):
    """
    Checks if module can be imported without actually
    importing it
    """
    module_spec = importlib.util.find_spec(module_name)
    if module_spec is None:
        print("Module: {} not found".format(module_name))
        return None
    else:
        print("Module: {} can be imported".format(module_name))
        return module_spec


def import_module_from_spec(module_spec):
    """
    Import the module via the passed in module specification
    Returns the newly imported module
    """
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def loader(mdl_name):
    module_spec = check_module(mdl_name)
    if module_spec:
        module = import_module_from_spec(module_spec)
        return module
    return None
    # return importlib.reload(mdl_name)


def list_all_klass(module):
    if type(module) == str:
        module = loader(module)
    klass = []
    for name in dir(module):
        var = getattr(module, name)
        if isinstance(var, type):
            klass.append(name)
    return klass


def is_child_of(obj, cls):
    try:
        for i in obj.__bases__:
            if i is cls or isinstance(i, cls):
                return True
        for i in obj.__bases__:
            if is_child_of(i, cls):
                return True
    except AttributeError:
        return is_child_of(obj.__class__, cls)
    return False


def get_all_klass_type_in(module, klass=None):
    if type(module) == str:
        module = loader(module)

    if not klass:
        klass = type
    klasses = []
    for name in dir(module):
        var = getattr(module, name)
        if is_child_of(var, klass):
            klasses.append(var)

    return klasses


def max_number(path=None):
    if path is None:
        path = get_current_path()

    childs = list_all_migration_files(path)
    if len(childs) > 0:
        sorted_childs = _sort_(childs)
        num = parse_number(sorted_childs[-1])
        return num

    return 0


# def main(path=None):
#     if path is None:
#         path = get_cuurent_path()
#     # print(path)
#     childs = list_all_migration_files(path)
#     sorted_childs = _sort_(childs)
#     for f in sorted_childs:
#         mdl = loader(parse_module_name(f))
#         klasss = get_all_klass_type_in(mdl, Migrate)
#         for k in klasss:
#             # print(k)
#             k().down()


def change_to_camel(name, sep='_'):
    string_list = str(name).split(sep)  # 将字符串转化为list
    first = string_list[0].lower()
    others = string_list[1:]

    # str.capitalize():将字符串的首字母转化为大写
    others_capital = [word.capitalize() for word in others]

    others_capital[0:0] = [first]

    # 将list组合成为字符串，中间无连接符。
    hump_string = ''.join(others_capital)
    return hump_string


# 包含下划线则认为是
def is_slash_name(name):
    return '_' in name


def change_to_slash_name(name):
    if is_slash_name(name):
        return name.lower()

    listx = name[0:len(name)]
    listy = listx[0]
    for i in range(1, len(listx) - 1):
        # listx[i] 直接copy 或 先加'_'再copy
        if listx[i].isupper(
        ) and not listx[i - 1].isupper():  # 加'_',当前为大写，前一个字母为小写
            listy += '_'
            listy += listx[i]
        elif listx[i].isupper() and listx[i -
                                          1].isupper() and listx[i +
                                                                 1].islower():
            # 加'_',当前为大写，前一个字母为小写
            listy += '_'
            listy += listx[i]
        else:
            listy += listx[i]
    return listy.lower()


def generate_file(name='migrate_task', path=None):
    if not path:
        path = get_current_path()

    slash_name = change_to_slash_name(name)

    numbered_name = '%d_%s.py' % (max_number(path) + 1, slash_name)

    full_path = os.path.sep.join([path, numbered_name])

    with open(full_path, 'w') as f:
        tmps = """#!/usr/bin/python3
# -*- coding:utf-8 -*-
# Author: yizr

import os
import sys

__abs_file__ = os.path.abspath(__file__)
tool_dir = os.path.dirname(os.path.dirname(__abs_file__))
code_dir = os.path.dirname(tool_dir)
sys.path.append(code_dir)

from tools.migrate.rake_migrate import RakeMigrate

from lib import dbutil

import module.dao.base.pg_field_desc as fd


class %s(RakeMigrate):
    def __init__(self):
        super().__init__()
        self.db_conn = dbutil.get_connection('risk_db')

    def up(self):
        self.create_table('%s',*columns, fd.UpdateAtField(), fd.CreateAtField())

    def down(self):
        self.drop('%s')
"""
        class_name = change_to_camel(slash_name)
        tmps = tmps % (class_name, slash_name, slash_name)
        f.write(tmps)


def run_migrate(path=None):
    if path is None:
        path = get_current_path()

    childs = list_all_migration_files(path)
    sorted_childs = _sort_(childs)
    for f in sorted_childs:
        mdl = loader(parse_module_name(f))
        klasss = get_all_klass_type_in(mdl, Migrate)
        for k in klasss:
            obj = k()
            obj.down()
            obj.up()


def run_rollback(path=None):
    if path is None:
        path = get_current_path()

    childs = list_all_migration_files(path)
    sorted_childs = _sort_(childs)
    for f in reversed(sorted_childs):
        mdl = loader(parse_module_name(f))
        klasss = get_all_klass_type_in(mdl, Migrate)
        for k in klasss:
            obj = k()
            obj.down()


def print_usage():
    print('usage python rake.py [cmd]')
    print('[cmd]:')
    print('     g: generate content eg: g create_table_risk_order')
    print('     m: excute all migration by order.')
    print('     r: rollback by desc order.')


def console(args):
    # run_migrate(path=None)
    if len(args) <= 1:
        print_usage()
        return

    if args[1] == 'g':
        path = None
        if len(args) > 3:
            path = args[3]
        generate_file(name=args[2], path=path)

    if args[1] == 'm':
        path = None
        if len(args) > 2:
            path = args[2]
        run_migrate(path=path)

    if args[1] == 'r':
        path = None
        if len(args) > 2:
            path = args[2]
        run_rollback(path=path)


if __name__ == '__main__':
    console(sys.argv)
