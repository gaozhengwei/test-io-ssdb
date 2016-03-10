#!/usr/bin/env python
#coding: utf-8

import time
import threading
import commands
import psutil
import json

from subprocess import Popen, PIPE

CLIENTS = 160
#NUM = 20000000
NUM = 200000
OK = 'ok'


from bench_conf import *

g_qps = {}
g_stat = {'clients': CLIENTS}
g_finish = ''

class SetThread(threading.Thread):
    def run(self):
        global g_finish
        global g_qps
        cmd = 'redis-benchmark  -p 8888 -t set -n %s -r 10000000000 -d 50 -c %s' % (NUM, CLIENTS)
        p = Popen(cmd, shell=True, stdout=PIPE, bufsize=1024)
        for line in iter(lambda: p.stdout.readline(), ''):
            line = str(line).strip()
            if line.endswith('second'):
                g_qps['w_qps'] = line.split()[0]
                g_finish = OK


class GetThread(threading.Thread):
    def run(self):
        global g_finish
        global g_qps
        cmd = 'redis-benchmark  -p 8888 -t get -n %s -r 10000000000 -d 50 -c %s' % (NUM, CLIENTS)
        p = Popen(cmd, shell=True, stdout=PIPE, bufsize=1024)
        for line in iter(lambda: p.stdout.readline(), ''):
            line = str(line).strip()
            if line.endswith('second'):
                g_qps['r_qps'] = line.split()[0]
                g_finish = OK


class IoStatThread(threading.Thread):
    def run(self):
        global g_finish
        global g_stat
        while True:
            g_stat = IoStatThread.call_iostat(DEV, INTERVAL)
            if g_finish == OK:
                break;

    @staticmethod
    def call_iostat(dev, interval):
        cmd = 'iostat -kxt %d 2' % interval
        out = commands.getoutput(cmd)
        lines = out.split('\n')
        lines.reverse()

        def line_to_dict(line):
            #Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await  svctm  %util
            fields = line.split()

            stat = {}
            stat['rrqm/s']   = fields[1]
            stat['wrqm/s']   = fields[2]
            stat['r/s']      = fields[3]
            stat['w/s']      = fields[4]
            stat['rkB/s']    = fields[5]
            stat['wkB/s']    = fields[6]

            stat['avgrq-sz'] = fields[7]
            stat['avqqu-sz'] = fields[8]

            stat['await']    = fields[9]
            stat['svctm']    = fields[10]
            stat['util']     = fields[11]
            return stat

        for line in lines:
            if line.startswith(dev):
                return line_to_dict(line)

def get_disk_usage(path):
    cmd = 'du -s %s' % path
    out = commands.getoutput(cmd)
    return int(out.split()[0])

def get_proc(proc_name):
    for proc in psutil.process_iter():
        if proc.name == proc_name:
            return proc

def my_json_encode(j):
    return json.dumps(j)

proc = get_proc(PROC)

def dostat():
    global g_stat
    g_stat['qps'] = g_qps
    g_stat['ts'] = time.time()
    g_stat['du'] = get_disk_usage(DATA_DIR)
    g_stat['cpu'] = proc.get_cpu_percent(interval=1)
    g_stat['mem-rss'] = proc.get_memory_info().rss
    g_stat['mem-vms'] = proc.get_memory_info().vms
    print g_stat

class StatThread(threading.Thread):
    def run(self):
        global g_finish
        while True:
            try:
                dostat()
                if g_finish == OK:
                    dostat()
                    break;
                time.sleep(INTERVAL)
            except Exception, e:
                print time.time(), 'got Exception:', e


def startup_monitor():
    IoStatThread().start()
    StatThread().start()

def test_set_cap():
    startup_monitor()
    SetThread().start()

def test_get_cap():
    startup_monitor()
    GetThread().start()

def test_set_get_cap():
    startup_monitor()
    SetThread().start()
    GetThread().start()

def main():
    """docstring for main"""
    print 'benchmark start!!!!!!!!!!!!!!!!!!!!!!!!'
    test_set_cap()

if __name__ == "__main__":
    main()
