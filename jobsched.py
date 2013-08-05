#-*- coding: utf8 -*-
'''
Created on 2013-4-12
异步job的demon
@author: gl
'''

import  os,sys
import time

BASEROOT= os.getcwd()
base_1= os.path.dirname(os.path.realpath(BASEROOT))
base  = os.path.dirname(os.path.realpath(base_1))
sys.path.append(base)

from multiprocessing.process import Process
from multiprocessing import Value
import signal
import subprocess
from framework.util.aiddplatform import DEBUG
from apps.asyn.jobconf import get_all_jobs

def logger_constructor(logger_name,filename):
    logger = logging.getLogger(logger_name)
    hdlr = logging.FileHandler(filename)
    formatter = logging.Formatter(u'[%(asctime)s]%(levelname)-8s"%(message)s"','%Y-%m-%d %a %H:%M:%S')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)

    return logger

#配置logger
j_logger = logger_constructor('jobsched_logger','../jobsched.log')

#job 运行状态码
JOB_STATUS = 0    
#工作
JOB_WORKING = 1
#休眠
JOB_SLEEPING = 2    
#重启
JOB_RESTARTING = 3

DEAMON_NAME = 'jobsched.py'

def check_pid(pid):        
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True

def register_signal_notify():
    
    #该方法会阻塞进程
    def handler(signum,frame):
        j_logger.info('Signal handler called with signal %s' % str(signum))
        #判断当前的JOB的status，如果为working，则sleep等待，否则直接退出
        #JOB_STATUS 应该lock，readonly
        retry_times = 10 #设定timeout机制,如果进程长时间没有结束，则直接杀死；以后改成可配置
        for job in get_all_jobs():
            normal_quit = 0 #0代表正常退出
            j_logger.info('trying terminate job %s !' % job.name)
            while job.run_status.value == JOB_WORKING:
                j_logger.info('job running,wait till it done!')
                time.sleep(0.5)
                retry_times -= 1
                if retry_times <= 0:
                    normal_quit = 1
                    break
            if normal_quit == 1:
                j_logger.info('job %s taken too long to finish,the scheduler will try to kill it violently!' % job.name)
            if(check_pid(job.process_id)):
                os.kill(job.process_id, signal.SIGKILL) #杀死子进程
                j_logger.info('job %s was killed !' % job.name)
            else:
                j_logger.warn('job %s was not found!(error termination)' % job.name)
        
    signal.signal(signal.SIGTERM, handler) #term信号
    signal.signal(signal.SIGHUP, handler) #hub信号
    
def job_prepare(job):
    #标识为working
    j_logger.info(u'=======job[%s] begin working========' % job.name)
    job.run_status.value = JOB_WORKING

def job_done(job):
    #标识为sleeping
    job.run_status.value = JOB_SLEEPING
    j_logger.info(u'=======job[%s] finish working========' % job.name)

def _inner_job(job):
    while True:
        job_prepare(job)
        try:
            job.fn()
        except Exception as e:
            j_logger.info(e)
        job_done(job)
        time.sleep(job.interval)
        
def start_sched():
    j_logger.info(u'starting job scheduler ...')
    jobs = get_all_jobs()
    for job in jobs:
        j_logger.info(u'starting job %s ' % job.name)
        job.run_status = Value('i', 0) #job的状态值
        try:
            p = Process(target=_inner_job, name=job.name,args=(job,))
            p.start()
            job.process_id = p.pid
            j_logger.info(u'job %s started !' % job.name)
        except Exception as e:
            j_logger.error(u'job %s fail to start,due to [%s]!' % (job.name,e))
    register_signal_notify()
    j_logger.info(u'job scheduler started !')

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

    
if __name__ == '__main__':
    start_sched()
    
    
