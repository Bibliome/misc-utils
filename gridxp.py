#!/bin/env python

from sys import stderr
import itertools
from collections import OrderedDict
import os.path
import subprocess
from os import makedirs
from argparse import ArgumentParser
from datetime import datetime
#from qsync import QSync


def log(msg):
    d = datetime.now()
    stderr.write('[' + d.strftime('%Y-%m-%d %H:%M:%S') + '] ' + msg + '\n')
    stderr.flush()

class Param:
    def __init__(self, name, fmt='s', domain=None):
        self.name = name
        self.fmt = fmt
        self.named_fmt = '%%(%s)%s' % (name, fmt)
        self.order_fmt = '%%%s' % fmt
        self.values = ()
        self.current = None
        self.domain = domain

    def svalue(self):
        return self.order_fmt % self.current

    def check_value(self, value):
        if self.domain is None:
            return True
        try:
            return value in self.domain
        except TypeError:
            pass
        return self.domain(value)

    def set_value(self, value):
        if not self.check_value(value):
            raise ValueError('param %s domain error: %s' % (self.name, value))
        self.current = value
    

LOAD_PATHS = ['.']
def searchfile(filename):
    if os.path.isabs(filename):
        return filename
    for d in LOAD_PATHS:
        r = os.path.join(d, filename)
        if os.path.exists(r):
            return r
    raise ValueError('could not find \'%s\' in %s' % (filename, LOAD_PATHS))

            
class Experiment:
    def __init__(self):
        self.params = OrderedDict()
        
    def add_param(self, p):
        if not isinstance(p, Param):
            raise TypeError('is not Param: ' + p)
        if p.name in self.params:
            raise ValueError('duplicate param %s' % p.name)
        self.params[p.name] = p
            
    def set_param_values(self, name, values):
            if name in self.params:
                self.params[name].values = values
            else:
                raise ValueError('no param %s' % name)

    def cells(self):
        paramvalues = tuple(p.values for p in self.params.values())
        for p in self.params.values():
            if len(p.values) == 0:
                raise ValueError('empty values for %s' % p.name)
        for pvs in itertools.product(*paramvalues):
            for p, v in zip(self.params.values(), pvs):
                p.set_value(v)
            yield None

    def run(self, test=False):
        log('running pre-process')
        self.pre()
        for _ in self.cells():
            log('running process for ' + ', '.join(('%s=%s' % (p.name, p.svalue())) for p in self.params.values()))
            self.exe()
            if test:
                log('(test) stop')
                break
        log('running post-process')
        self.post()

    def test(self, values=None):
        self.run(test=True, values=values)

    def pre(self):
        pass

    def post(self):
        pass

    def exe(self):
        raise NotImplemented()


class ExperimentConfig(Experiment):
    def __init__(self):
        Experiment.__init__(self)
        self.cl = None
        self.output_dir = None
        self.sep = '_'
        self.shell = None
        self.cd = False
        self.pre_cl = None
        self.post_cl = None
        self.out = None
        self.err = None
        self.job_opts = None
        self.qsync_filename = 'gridxp.qsync'
        self.qsync_opts = {}
        self.executor = None
        self.dry_run = False

    def load_config(self, filename):
        found = searchfile(filename)
        log('loading experiment configuration from ' + found)
        execfile(found, self.create_locals())

    def create_locals(self):
        cl = self._attrsetter('cl')
        od = self._attrsetter('output_dir')
        cd = self._attrsetter('cd')
        pre_cl = self._attrsetter('pre_cl')
        post_cl = self._attrsetter('post_cl')
        out = self._attrsetter('out')
        err = self._attrsetter('err')
        jo = self._attrsetter('job_opts')
        qf = self._attrsetter('qsync_filename')
        param = self._paramdef()
        pv = self._paramvaluessetter()
        return dict(
            param=param, addparam=param, add_param=param,
            cl=cl, commandline=cl, cmdline=cl, command_line=cd,
            od=od, outdir=od, outputdir=od, out_dir=od, output_dir=od,
            alternate_shell=self._attrsetter('shell'),
            cd=cd, changedir=cd, change_dir=cd, change_directory=cd,
            pre_cl=pre_cl, pre_commandline=pre_cl, pre_cmdline=pre_cl, pre_command_line=pre_cl,
            post_cl=post_cl, post_commandline=post_cl, post_cmdline=post_cl, post_command_line=post_cl,
            outfile=out, out_file=out,
            errfile=out, err_file=out,
            job_opts=jo, job_options=jo,
            qf=qf, qsync_filename=qf, qsync_file=qf,
            paramvalues=pv, param_values=pv,
            qsync_opts=self._qsync_opts(),
            local=self.local_execution,
            include=(lambda filename: self.load_config(filename)),
            dry_run=self._attrsetter('dry_run')
        )

    def local_execution(self):
        self.executor = LocalExecutor

    def _attrsetter(self, name):
        return lambda value: setattr(self, name, value)

    def _paramdef(self):
        def result(name, fmt='s', domain=None):
            self.add_param(Param(name, fmt, domain))
        return result

    def _paramvaluessetter(self):
        def result(name, *values):
            self.set_param_values(name, values)
        return result

    def _qsync_opts(self):
        def result(**kwargs):
            self.qsync_file = kwargs
        return result
        
    @staticmethod
    def expand(d, s):
        return os.path.expanduser(os.path.expandvars(s % d))

    @staticmethod
    def _open_out(filename, d):
        if filename is None:
            return None
        expanded = ExperimentConfig.expand(d, filename)
        return open(expanded, 'w')

    def _param_dirname(self, p):
        return p.name + self.sep + p.svalue()

    def _dict(self):
        for name, p in self.params.items():
            yield name, p.current
            yield 's_' + name, p.svalue()
        d = self.output_dir
        if not os.path.exists(d):
            makedirs(d)
        for p in self.params.values():
            d = os.path.join(d, self._param_dirname(p))
            if not os.path.exists(d):
                makedirs(d)
            yield 'd_' + p.name, d

    def _executor(self):
        if self.executor is not None:
            return self.executor
        if QSyncExecutor.ready(self):
            log('everything in place to use qsync')
            return QSyncExecutor
        log('defaulting to local executor')
        return LocalExecutor

    def pre(self):
        self.executor = self._executor()
        self.executor.check(self)
        self.executor.pre(self)

    def post(self):
        self.executor.post(self)

    def exe(self):
        self.executor.exe(self)


class LocalExecutor:
    @staticmethod
    def pre(config):
        if config.pre_cl is None:
            return
        if config.dry_run:
            log('(dry run) ' + config.pre_cl)
        p = subprocess.Popen(config.pre_cl, shell=True, executable=config.shell)
        p.wait()
        if p.returncode != 0:
            log('preprocess has FAILED')

    @staticmethod
    def post(config):
        if config.post_cl is None:
            return
        if config.dry_run:
            log('(dry run) ' + config.post_cl)
        p = subprocess.Popen(config.post_cl, shell=True, executable=config.shell)
        p.wait()
        if p.returncode != 0:
            log('postprocess has FAILED')

    @staticmethod
    def exe(config):
        if config.cd:
            wd = config._pset_dir(self.params)
        else:
            wd = None
        d = dict(config._dict())
        out = ExperimentConfig._open_out(config.out, d)
        err = ExperimentConfig._open_out(config.err, d)
        cl = ExperimentConfig.expand(d, config.cl)
        if config.dry_run:
            log('(dry run) ' + cl)
        else:
            p = subprocess.Popen(cl, shell=True, executable=config.shell, cwd=wd, stdout=out, stderr=err, close_fds=True)
            p.wait()
            if p.returncode != 0:
                log('process has FAILED')

    @staticmethod
    def ready(config):
        if config.params is None:
            return False
        if config.cl is None:
            return False
        if config.output_dir is None:
            return False
        return True

    @staticmethod
    def check(config):
        if config.params is None:
            raise ValueError('missing pset()')
        if config.cl is None:
            raise ValueError('mising commandline()')
        if config.output_dir is None:
            raise ValueError('mising output()')



class QSyncExecutor:
    @staticmethod
    def pre(config):
        LocalExecutor.pre(config)
        config.qsync_filepath = os.path.join(config.output_dir, config.qsync_filename)
        config.qsync_file = open(config.qsync_filepath, 'w')

    @staticmethod
    def post(config):
        config.qsync_file.close()
        if config.dry_run:
            log('(dry run) skipping job submission')
        else:
            qsync = QSync()
            qsync.filenames = (config.qsync_filepath,)
            qsync.go(**config.qsync_opts)
        LocalExecutor.post(config)

    @staticmethod
    def exe(config):
        d = dict(config._dict())
        config.qsync_file.write('-V -cwd')
        QSyncExecutor._write_qsync_opt(config, d, ' ', config.job_opts)
        QSyncExecutor._write_qsync_opt(config, d, ' -o ', config.out)
        QSyncExecutor._write_qsync_opt(config, d, ' -e ', config.err)
        config.qsync_file.write(' -- ')
        config.qsync_file.write(ExperimentConfig.expand(d, config.cl))
        config.qsync_file.write('\n')

    @staticmethod
    def _write_qsync_opt(config, d, prefix, suffix):
        if suffix:
            config.qsync_file.write(prefix)
            config.qsync_file.write(ExperimentConfig.expand(d, suffix))

    @staticmethod
    def ready(config):
        if not LocalExecutor.ready(config):
            return False
        if config.job_opts is None:
            return False
        return True

    @staticmethod
    def check(config):
        LocalExecutor.check(config)
        if config.job_opts is None:
            raise ValueError('mising qopts()')


class GridXP(ArgumentParser):
    def __init__(self):
        ArgumentParser.__init__(self, description='Perform a grid experiment')
        self.add_argument('xp_filenames', metavar='XPFILE', type=str, nargs='+', default=[], help='file containing the experiment definition object')
        self.add_argument('--test', dest='test', action='store_true', default=False, help='test run (only one parameter value set)')
        self.add_argument('--load-path', metavar='PATH', dest='load_paths', action='append', type=str, default=['.'], help='add path where to search for experiment and parameter set files')
        self.add_argument('--local', dest='local', action='store_true', default=False, help='force local execution')
        self.add_argument('--dry-run', dest='dry_run', action='store_true', default=False, help='dry run')

    def go(self):
        args = self.parse_args()
        global LOAD_PATHS
        LOAD_PATHS = args.load_paths
        xp = ExperimentConfig()
        xp.dry_run = args.dry_run
        for fn in args.xp_filenames:
            xp.load_config(fn)
        if args.local:
            xp.local_execution()
        xp.run(test=args.test)


if __name__ == '__main__':
    GridXP().go()
