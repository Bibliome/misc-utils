#!/usr/bin/env python

import drmaa
import shlex
from optparse import OptionParser
from sys import stderr, stdin, exit
from datetime import datetime

def Stop(pool, jt, info):
    '''Job failure function that stops synchronization.'''
    pool.shall_stop = True
    pool.all_done = False
    pool.failed_jobs.append(jt)

def Proceed(pool, jt, info):
    '''Job failure function that proceeds with the remaining jobs.'''
    pool.all_done = False
    pool.failed_jobs.append(jt)

def Resubmit(max_tries, fail):
    '''Job failure function factory that resubmits a failed job.

    :Parameters:
    max_tries: maximum number of submissions for a job.
    fail: failure function to call if the maximum number of tries has been reached.
    '''
    def resubmit_function(pool, jt, info):
        if jt.failures >= max_tries:
            fail(pool, jt, info)
        else:
            jt.jobid = pool.session.runJob(jt)
            pool.log('job specified at ' + jt.source + ' resubmitted with id ' + jt.jobid)
            pool.current_jobs[jt.jobid] = jt
    return resubmit_function


class JobPool:
    '''
    A pool of jobs.

    :Members:
    session: DRMAA session
    logfile: file where actions and status are written
    current_jobs: jobs that have been submitted and that are not finished
    all_done: either all finished jobs were successful
    shall_stop: either this object should stop the synchronization
    '''
    def __init__(self, session, logfile):
        self.session = session
        self.logfile = logfile
        self.current_jobs = {}
        self.all_done = True
        self.shall_stop = False
        self.failed_jobs = []

    def log(self, msg=''):
        '''Logs a message'''
        d = datetime.now()
        self.logfile.write('[' + d.strftime('%Y-%m-%d %H:%M:%S') + '] ' + msg + '\n')
        self.logfile.flush()

    def createJobTemplate(self):
        '''Creates a job template (delegates to self.session)'''
        return self.session.createJobTemplate()

    def runJob(self, jt):
        '''Submits a job.

        This method delegates to self.session, then keeps track of the submitted job

        :Parameters:
        jt: job template, with a member 'source' indicating where this template was specified
        '''
        jt.jobid = self.session.runJob(jt)
        if jt.source is None:
            jt.source = jobid
        jt.failures = 0
        self.log('job specified at ' + jt.source + ' submitted with id ' + jt.jobid)
        self.current_jobs[jt.jobid] = jt
        return jt.jobid

    def waitall(self, fail=Proceed, interval=60):
        '''Waits for all submitted jobs to finish.

        :Parameters:
        fail: function called in case of failure, the function must accept 3 paremeters: this object, the JobTemplate object and the DRMAA JobInfo object.
        interval: check for job status every number of seconds.
        '''
        start = datetime.now()
        running = 0
        while self.current_jobs:
            joblist = list(self.current_jobs.keys()) # create fresh list to work around Python 3 iterator
            try:
                self.log('synchronizing %d jobs (%d running), see you in %d seconds' % (len(joblist), running, interval))
                self.session.synchronize(joblist, interval, False)
            except drmaa.errors.ExitTimeoutException:
                pass
            running = 0
            for jobid in joblist:
                status = self.session.jobStatus(jobid)
                if status == drmaa.JobState.DONE:
                    try:
                        info = self.session.wait(jobid, drmaa.Session.TIMEOUT_NO_WAIT)
                        jt = self.current_jobs[jobid]
                    except drmaa.errors.ExitTimeoutException:
                        pass
                    if info.wasAborted:
                        self.log('job specified at %s with id %s aborted' % (self.current_jobs[jobid].source, jobid))
                        self._failed(jobid, fail, info)
                    elif info.hasSignal:
                        self.log('job specified at %s with id %s aborted received signal %d' % (self.current_jobs[jobid].source, jobid, info.terminatedSignal))
                        self._failed(jobid, fail, info)
                    elif info.exitStatus != 0:
                        self.log('job specified at %s with id %s aborted exited with status %d' % (self.current_jobs[jobid].source, jobid, info.exitStatus))
                        self._failed(jobid, fail, info)
                    else:
                        self.log('job specified at %s with id %s is done' % (self.current_jobs[jobid].source, jobid))
                        del self.current_jobs[jobid]
                elif status == drmaa.JobState.FAILED:
                    self.log('job specified at %s with id %s failed somehow' % (self.current_jobs[jobid].source, jobid))
                    self._failed(jobid, fail, None)
                elif status == drmaa.JobState.RUNNING:
                    running += 1
            if self.shall_stop:
                break
        if self.all_done:
            delta = datetime.now() - start
            self.log('all jobs completed successfully in ' + str(delta) + ', you\'re welcome')
        else:
            self.log('sorry, the following jobs have failed:')
            for job in self.failed_jobs:
                self.log(job.source + ' with id ' + str(job.jobid))

    def _failed(self, jobid, fail, info):
        jt = self.current_jobs[jobid]
        jt.failures += 1
        del self.current_jobs[jobid]
        fail(self, jt, info)

    def runall(self, jobs, fail=Proceed, interval=60):
        '''Submits jobs and waits for them to finish.

        :Parameters:
        jobs: a sequence of job templates
        fail: job failure function
        interval: job status check interval in seconds

        :Return value:
        True if all jobs finished successfully, False otherwise.
        '''
        for jt in jobs:
            self.runJob(jt)
        self.waitall(fail, interval)
        return self.all_done

    def terminate(self):
        '''Terminates all remaining jobs.'''
        self.log('terminating remaining jobs')
        self.session.control(drmaa.Session.JOB_IDS_SESSION_ALL, drmaa.JobControlAction.TERMINATE)
        self.current_jobs = {}





class QSyncBase:
    def __init__(self):
        pass

    def create_jobs(self, session):
        raise NotImplemented()

    def go(self, interval=60, force_interval=False, fail=Proceed, logfile=stderr):
        if interval < 1:
            raise Exception('illegal interval: %d' % interval)
        if interval <= 10 and not force_interval:
            raise Exception('unwise interval: %d (use force interval if you want this anyway')
        session = drmaa.Session()
        session.initialize()
        jobs = self.create_jobs(session)
        pool = JobPool(session, logfile)
        try:
            r = pool.runall(jobs, fail, interval)
            if not r:
                pool.terminate()
            return r
        except BaseException as e:
            pool.log(str(e))
            pool.terminate()
            raise e
        finally:
            session.exit()


class QSync(OptionParser, QSyncBase):
    def __init__(self):
        OptionParser.__init__(self, usage='Usage: %prog [OPTIONS] [FILE...]')
        self.set_defaults(fail=Proceed)
        self.add_option('-s', '--stop-on-failure', action='store_const', const=Stop, dest='fail', help='if one job fails, stop synchronization and terminate all remaining jobs')
        self.add_option('-p', '--proceed-on-failure', action='store_const', const=Proceed, dest='fail', help='continue running jobs even if some fail (default behaviour)')
        self.add_option('-r', '--resubmit-on-failure', action='store', type='int', dest='resubmit', help='resubmit failed jobs at most N times each', metavar='N')
        self.add_option('-l', '--log-file', action='store', type='string', dest='logfile', default=None, help='write log into FILE (default: stderr)', metavar='FILE')
        self.add_option('-i', '--interval', action='store', type='int', dest='interval', default=60, help='wait T seconds before polling job status, values below 10 require --force-interval (default: %default)', metavar='T')
        self.add_option('--force-interval', action='store_true', dest='force_interval', default=False, help='accept poll intervals below 10 seconds')

    def run(self):
        options, self.filenames = self.parse_args()
        fail = options.fail
        if options.resubmit:
            if options.resubmit < 1:
                raise Exception('illegal number of resubmissions: %d' % options.resubmit)
            fail = Resubmit(options.resubmit, fail)
        logfile = stderr
        if options.logfile:
            logfile = open(options.logfile, 'w')
        self.go(interval=options.interval, force_interval=options.force_interval, fail=fail, logfile=logfile)

    @staticmethod
    def _create_job(session, filename, f):
        for n, line in enumerate(f):
            jt = session.createJobTemplate()
            b, dd, a = line.partition('--')
            if dd != '':
                jt.nativeSpecification = b
                line = a
            args = shlex.split(line)
            jt.remoteCommand = args[0]
            jt.args = args[1:]
            jt.source = '%s:%d' % (filename, n + 1)
            yield jt
        
    def create_jobs(self, session):
        if self.filenames:
            for filename in self.filenames:
                f = open(filename)
                for p in QSync._create_job(session, filename, f):
                    yield p
                f.close()
        else:
            for p in QSync._create_job(session, '<stdin>', stdin):
                yield p

if __name__ == '__main__':
    if not QSync().run():
        exit(1)
