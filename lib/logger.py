import sys
import time
import os

import threading

default_encoding = 'utf-8'


class _thread_idx_map:
    def __init__(self):
        self.ident_list = []
        self.ident2idx = {}
        self.lock = threading.Lock()

    def get_idx(self, ident):
        with self.lock:
            i = self.ident2idx.get(ident)
            if i is None:
                self.ident_list.append(ident)
                i = len(self.ident_list) - 1
            self.ident2idx[ident] = i
            return i

    def idx2ident(self, idx):
        with self.lock:
            if idx < 0 or idx > len(self.ident_list) - 1:
                return None
            return self.ident_list[idx]


_th_idx_map = _thread_idx_map()


class _logger:
    class LocalData(threading.local):
        initialized = False

        def __init__(self, **kw):
            if self.initialized:
                raise SystemError('LocalData: __init__ called too many times')
            self.initialized = True
            #self.tid = threading.current_thread().ident
            self.tid = _th_idx_map.get_idx(threading.current_thread().ident)
            self.__dict__.update(kw)

    def __init__(self):
        self.LEVEL_ALL = 0

        self.LEVEL_DEBUG = 1
        self.LEVEL_TRACE = 2
        self.LEVEL_INFO = 3
        self.LEVEL_ANNOUNCE = 4
        self.LEVEL_WARNING = 5
        self.LEVEL_FATAL = 6

        self.LEVEL_NONE = 7

        self.level_info = (
            ('!<', sys.stderr),
            ('DEBUG', sys.stderr),
            ('TRACE', sys.stderr),
            ('INFO', sys.stderr),
            ('ANNOUNCE', sys.stderr),
            ('WARNING', sys.stderr),
            ('FATAL', sys.stderr),
            ('>!', sys.stderr),
        )

        self.LEVEL = dict([
            (v, i)
            for i, v in enumerate(('ALL', 'DEBUG', 'TRACE', 'INFO', 'ANNOUNCE',
                                   'WARNING', 'FATAL', 'NONE'))
        ])

        self.cur_trace_no = 0

        self.thread_local = None

        self.cur_output_level = 2

    def set_log_file(self, std_file, greater_level_files={}
                     ):  # {LEVEL_ANNOUNCE : fd_a, LEVEL_WARNING : fd_wf }
        newinfo = []
        greater_levels = sorted(greater_level_files.items())
        greater_levels.insert(0, (0, std_file))
        next_greater_level = greater_levels.pop()
        for i in reversed(range(len(self.level_info))):
            while next_greater_level[0] > i:
                next_greater_level = greater_levels.pop()
            newinfo.append((self.level_info[i][0], next_greater_level[1]))

        #for i in range(self.LEVEL_NONE):
        #    newinfo.append((self.level_info[i][0], std_file))

        newinfo.reverse()
        self.level_info = tuple(newinfo)

    def set_trace_no(self, trace_no):
        if self.thread_local == None:
            self.cur_trace_no = trace_no
        else:
            self.thread_local.trace_no = trace_no

    def set_output_level(self, level):
        if isinstance(level, int) and level >= 0 and level <= self.LEVEL_NONE:
            self.cur_output_level = level
        elif isinstance(level, str) and level in self.LEVEL:
            self.cur_output_level = self.LEVEL[level]
        else:
            raise Exception('invalid log level:' + repr(level))

    def get_trace_no(self):
        return self.cur_trace_no if self.thread_local == None else self.thread_local.trace_no

    def get_tid(self):
        #return os.getpid() if self.thread_local == None else self.thread_local.tid
        return None if self.thread_local is None else self.thread_local.tid

    def get_ptid_str(self):
        return '%d.' % os.getpid() + ('' if self.thread_local == None else str(
            self.thread_local.tid))

    def set_multithread(self, enabled=True):
        if enabled:
            #self.thread_local = threading.local()
            #self.thread_init()
            self.thread_local = _logger.LocalData(trace_no=0)
            #print('[][][]ms: thread_local:%s, trace_no:%d, tid:%d' % (repr(self.thread_local), self.thread_local.trace_no, self.thread_local.tid))
        else:
            self.thread_local = None
            #print('[][][]ms: thread_local:%s, tid:%d' % (repr(self.thread_local), threading.current_thread().ident))

    def thread_init(self):
        self.thread_local.trace_no = 0
        self.thread_local.tid = threading.current_thread(
        ).ident  # TODO: better change to thread_id.

    def log_output(self,
                   code_file,
                   code_func,
                   code_line,
                   level,
                   thetime,
                   msg,
                   trace_no=None):
        trace_no = self.get_trace_no() if trace_no is None else trace_no
        #tid = self.get_tid()
        ptid_str = self.get_ptid_str()
        f_out = self.level_info[level][1]
        #out_str = '[%s][%s:%d(%s)][%s:%.05f][%d][%d]%s\n' % \
        #        (self.level_info[level][0], code_file, code_line, code_func, \
        #         time.strftime('%Y%m%d %H:%M:%S', time.localtime(thetime)), thetime, tid, trace_no, msg)
        #out_str = '[%s][%s:%d(%s)][%s][%d][%d]%s\n' % \
        #        (self.level_info[level][0], code_file, code_line, code_func, \
        #         time.strftime('%Y%m%d %H:%M:%S', time.localtime(thetime)), tid, trace_no, msg)
        out_str = '[%s][%s:%d(%s)][%s][%s][%d]%s\n' % \
                (self.level_info[level][0], code_file, code_line, code_func, \
                 time.strftime('%Y%m%d %H:%M:%S', time.localtime(thetime)), ptid_str, trace_no, msg)
        if f_out.encoding is None and isinstance(out_str, str):
            out_str = out_str.encode(default_encoding)
        #print >>f_out, out_str
        f_out.write(out_str)
        f_out.flush()

    def _LOG_(self, level, msg, args, nframe=1):
        if level < self.cur_output_level:
            return
        if len(args) > 0:
            msg = msg % args
        fr = sys._getframe(nframe)
        filename = fr.f_code.co_filename
        filename = filename.replace('.py', '')
        funcname = fr.f_code.co_name
        lineno = fr.f_lineno
        tm = time.time()
        self.log_output(filename, funcname, lineno, level, tm, msg)


logger = _logger()


def log_output(code_file,
               code_func,
               code_line,
               level,
               thetime,
               msg,
               trace_no=None):
    global logger
    return logger.log_output(code_file, code_func, code_line, level, thetime,
                             msg, trace_no)


def _LOG_(level, msg, args, nframe=1):
    global logger
    return logger._LOG_(level, msg, args, nframe + 1)


def LOG_DEBUG(msg, *args):
    global logger
    logger._LOG_(logger.LEVEL_DEBUG, msg, args, nframe=2)


def LOG_TRACE(msg, *args):
    global logger
    logger._LOG_(logger.LEVEL_TRACE, msg, args, nframe=2)


def LOG_INFO(msg, *args):
    global logger
    logger._LOG_(logger.LEVEL_INFO, msg, args, nframe=2)


def LOG_ANNOUNCE(msg, *args):
    global logger
    logger._LOG_(logger.LEVEL_ANNOUNCE, msg, args, nframe=2)


def LOG_WARNING(msg, *args):
    global logger
    logger._LOG_(logger.LEVEL_WARNING, msg, args, nframe=2)


def LOG_FATAL(msg, *args):
    logger._LOG_(logger.LEVEL_FATAL, msg, args, nframe=2)
