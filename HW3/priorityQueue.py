__author__ = 'snehagaikwad'

import Queue

class Job(object):
    def __init__(self, priority, description):
        self.priority = priority
        self.description = description
        print 'New job:', description
        return
    def __cmp__(self, other):
        return cmp(self.priority, other.priority)

q = Queue.PriorityQueue()

q.put( Job(3, 'Mid-level job 2') )
q.put( Job(3, 'Mid-level job') )
q.put( Job(10, 'Low-level job') )
q.put( Job(1, 'Important job') )
q.put( Job(1, 'Important job 2') )
q.put( Job(1, 'Important job 3') )
q.put( Job(10, 'Low-level job 2') )

while not q.empty():
    next_job = q.get()
    print next_job
    print 'Processing job:', next_job.description

