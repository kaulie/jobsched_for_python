#-*- coding: utf8 -*-
#异步job的控制端
'''
Created on 2013-4-12

@author: gl
'''

import  os,sys

#job 运行指令
JOb_COMMAND = 0


#正常
JOB_NORMAL = 0
#启动
JOB_START = 1
#停止
JOB_STOP = 2
#重启
JOB_RESTART = 3
   
SURPORTED_COMMNDS = ['start','stop','reload','restart']

DEAMON_NAME = 'jobsched.py'

import subprocess, signal

def get_pid_by_name(pname):
    p = subprocess.Popen(['ps', '-ef'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    
    #找到sched的pid，只killsched的，不直接kill子进程
    for line in out.splitlines():
        items = line.split()
        if pname in items:
#            print line
            pid = int(items[1])
            return pid    

def do_stop():
    pid = get_pid_by_name(DEAMON_NAME)
    if pid:
        os.kill(pid, signal.SIGTERM)
        
def is_job_alive():
    pid = get_pid_by_name(DEAMON_NAME)
    return True if pid else False

def do_start():
    subprocess.Popen([sys.executable, DEAMON_NAME])
#    print 'Started jobsched'    
def do_start0():
    if not is_job_alive():
        do_start()
    
def do_restart():
    do_stop()
    do_start()    
    print 'Restarted jobsched' 

def do_reload():
    '''
        reload,重新加载配置文件
    '''
    do_restart()
    print 'Restarted jobsched'
            
def print_help():
    print 'jobschectl start|stop|reload|restart'
    
     
if __name__ == '__main__':
    
    #共享变量
    if len(sys.argv) <= 1 or sys.argv[1] not in SURPORTED_COMMNDS:
        print_help()
        sys.exit()
    if sys.argv[1] == 'start':
        do_start0()       
    elif sys.argv[1] == 'stop':
        do_stop()
    elif sys.argv[1] == 'restart':
        do_restart()
    elif sys.argv[1] == 'reload':
        do_reload()        
    else:
        print 'command not implemented!'
    
