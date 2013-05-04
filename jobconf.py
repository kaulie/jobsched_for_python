#-*- coding: utf8 -*-
'''
Created on 2013-4-13

@author: gl
'''

MINIMUM_INTERVAL_SECONDS = 5 #最少休眠时间

#job池
class Job_Pool():
    
    def __init__(self):
        self._job_pool = []
        
    def add_job(self,job):
        self._job_pool.append(job)
        
    def get_all_jobs(self):
        return self._job_pool
        
class Base_Job(object):
    '''job 基类'''
    def __init__(self,job_name,job_fn):
        super(Base_Job, self).__init__()
        self.name = job_name
        self.fn = job_fn
        self.run_status = None #job 的状态值，供内部调度使用
        self.process_id = None
        self.logger = None #日志        

class Interval_Job(Base_Job):
    '''定时任务'''
    def __init__(self,name,fn,interval):
        assert isinstance(interval,int) and interval >= MINIMUM_INTERVAL_SECONDS
        super(Interval_Job, self).__init__(name,fn)
        self.interval = interval
        

GLOBAL_JOB_POOL = Job_Pool()


def get_all_jobs():
    return GLOBAL_JOB_POOL.get_all_jobs()

def schedule_interval_job(name,fn,interval):
    asyn_job = Interval_Job(name,fn,interval)
    GLOBAL_JOB_POOL.add_job(asyn_job)

#根据配置，动态导入
exec "from apps.asyn.asynevent import asyn_event_process"
#exec "from apps.asyn.asynsalegoods import asyn_salegoods_process"

#调度    
schedule_interval_job('asyn_event',asyn_event_process,interval=60)
#schedule_interval_job('asyn_salegoods',asyn_salegoods_process,interval=60)
