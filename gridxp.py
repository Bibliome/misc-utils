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
    def __init__(self, name, descr, fmt='s', domain=None):
        self.name = name
        self.descr = descr
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


class ParamSet:
    def __init__(self, name, parent=None, params=()):
        self.parent = parent
        self.name = name
        for pset in self.ancestors():
            if pset.name == name:
                raise ValueError('ParamSet has same name as ancestor: %s' % name)
        self.params = OrderedDict(() if parent is None else parent.params)
        for p in params:
            self.add_param(p)
        ParamSet.current = self

    @staticmethod
    def load(filename):
        found = searchfile(filename)
        log('loading %s as %s' % (found, ParamSet.__name__))
        f = open(found)
        s = '\n'.join(f)
        f.close()
        r = eval(s)
        if not isinstance(r, ParamSet):
            raise ValueError('expected %s, got %s' % (ParamSet, r.__class__))
        return r

    def ancestors(self, include_self=False):
        if self.parent is not None:
            for pset in self.parent.ancestors(include_self=True):
                yield pset
        if include_self:
            yield self
        
    def add_param(self, p):
        if p.name in self.params:
            raise ValueError('duplicate param %s' % p.name)
        self.params[p.name] = p
        
    def set_multi_param_values(self, paramvalues):
        for name, values in paramvalues.items():
            self.set_param_values(name, values)
            
    def set_param_values(self, name, values):
            if name in self.params:
                self.params[name].values = values
            else:
                raise ValueError('no param %s' % name)
            
    def values(self):
        return OrderedDict((name, p.current) for name, p in self.params.items())

    def cells(self):
        paramvalues = tuple(p.values for p in self.params.values())
        for p in self.params.values():
            if len(p.values) == 0:
                raise ValueError('empty values for %s' % p.name)
        for pvs in itertools.product(*paramvalues):
            for p, v in zip(self.params.values(), pvs):
                p.set_value(v)
            yield self

            
class Experiment:
    def __init__(self):
        self.pset = None

    def run(self, test=False):
        log('running pre-process')
        self.pre()
        for _ in self.pset.cells():
            log('running process for ' + ', '.join(('%s=%s' % (n, p.svalue())) for (n, p) in self.pset.params.items()))
            self.exe()
            if test:
                log('test run, stop')
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

    def reset_values(self, values):
        self.pset.set_multi_param_values(values)
        return self


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

    def load_config(self, filename):
        found = searchfile(filename)
        log('loading experiment configuration from ' + found)
        execfile(found, self.create_locals())

    def create_locals(self):
        pset = self._attrsetter('pset')
        cl = self._attrsetter('cl')
        od = self._attrsetter('output_dir')
        cd = self._attrsetter('cd')
        pre_cl = self._attrsetter('pre_cl')
        post_cl = self._attrsetter('post_cl')
        out = self._attrsetter('out')
        err = self._attrsetter('err')
        jo = self._attrsetter('job_opts')
        qf = self._attrsetter('qsync_filename')
        pv = self._paramvaluessetter()
        return dict(
            pset=pset, param_set=pset, params=pset,
            ParamSet=ParamSet,
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
            include=(lambda filename: self.load_config(filename))
        )

    def local_execution(self):
        self.executor = LocalExecutor

    def _attrsetter(self, name):
        return lambda value: setattr(self, name, value)

    def _paramvaluessetter(self):
        def result(name, *values):
            self.pset.set_param_values(name, values)
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

    def _param_dir(self, p):
        return p.name + self.sep + p.svalue()

    def _pset_dir(self, pset):
        d = tuple(self._param_dir(p) for p in pset.params.values())
        return os.path.join(self.output_dir, *d)

    def _dict(self):
        for name, p in self.pset.params.items():
            yield name, p.current
            yield 's_' + name, p.svalue()
        for pset in self.pset.ancestors(include_self=True):
            d = self._pset_dir(pset)
            if not os.path.exists(d):
                makedirs(d)
            yield 'd_' + pset.name, d

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

    def local(self):
        self.executor = LocalExecutor
        return self

    def qsync(self):
        self.executor = QSyncExecutor
        return self


class LocalExecutor:
    @staticmethod
    def pre(config):
        if config.pre_cl is not None:
            p = subprocess.Popen(config.pre_cl, shell=True, executable=config.shell)
            p.wait()
            if p.returncode != 0:
                log('preprocess has FAILED')

    @staticmethod
    def post(config):
        if config.post_cl is not None:
            p = subprocess.Popen(config.post_cl, shell=True, executable=config.shell)
            p.wait()
            if p.returncode != 0:
                log('postprocess has FAILED')

    @staticmethod
    def exe(config):
        if config.cd:
            wd = config._pset_dir(self.pset)
        else:
            wd = None
        d = dict(config._dict())
        out = ExperimentConfig._open_out(config.out, d)
        err = ExperimentConfig._open_out(config.err, d)
        cl = ExperimentConfig.expand(d, config.cl)
        p = subprocess.Popen(cl, shell=True, executable=config.shell, cwd=wd, stdout=out, stderr=err, close_fds=True)
        p.wait()
        if p.returncode != 0:
            log('process has FAILED')

    @staticmethod
    def ready(config):
        if config.cl is None:
            return False
        if config.output_dir is None:
            return False
        return True

    @staticmethod
    def check(config):
        if config.pset is None:
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

    def go(self):
        args = self.parse_args()
        global LOAD_PATHS
        LOAD_PATHS = args.load_paths
        xp = ExperimentConfig()
        for fn in args.xp_filenames:
            xp.load_config(fn)
        if args.local:
            xp.local_execution()
        xp.run(test=args.test)


if __name__ == '__main__':
    GridXP().go()
