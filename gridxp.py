#!/usr/bin/python

from sys import stderr
import itertools
from collections import OrderedDict
import os.path
import subprocess
from os import makedirs
from argparse import ArgumentParser
from datetime import datetime
from qsync import QSync

class Param:
    def __init__(self, name, descr, fmt='s', values=()):
        self.name = name
        self.descr = descr
        self.fmt = fmt
        self.named_fmt = '%%(%s)%s' % (name, fmt)
        self.order_fmt = '%%%s' % fmt
        self.values = values
        self.current = None

    def svalue(self):
        return self.order_fmt % self.current


class Loadable:
    search_paths = ('.',)

    @classmethod
    def load(cls, filename):
        found = cls.searchfile(filename)
        stderr.write('loading %s as %s\n' % (found, str(cls)))
        f = open(found)
        s = '\n'.join(f)
        f.close()
        r = eval(s)
        if not isinstance(r, cls):
            raise ValueError('expected %s, got %s' % (cls, r.__class__))
        return r

    @classmethod
    def searchfile(cls, filename):
        if os.path.isabs(filename):
            return filename
        for d in cls.search_paths:
            r = os.path.join(d, filename)
            if os.path.exists(r):
                return r
        raise ValueError('could not find \'%s\' in %s' % (filename, str(cls.search_paths)))

    
class ParamSet(Loadable):
    search_paths = ('.',)

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
        
    def set_values(self, paramvalues):
        for name, values in paramvalues.items():
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
                p.current = v
            yield self

            
class Experiment(Loadable):
    search_paths = ('.',)

    def __init__(self, pset):
        self.pset = pset

    def run(self, test=False, values=None):
        if values is not None:
            self.log('setting parameter values')
            self.pset.set_values(values)
        self.log('running pre-process')
        self.pre()
        for _ in self.pset.cells():
            self.log('running process for ' + ', '.join(('%s=%s' % (n, p.svalue())) for (n, p) in self.pset.params.items()))
            self.exe()
            if test:
                self.log('test run, stop')
                break
        self.log('running post-process')
        self.post()

    def test(self, values=None):
        self.run(test=True, values=values)

    def pre(self):
        pass

    def post(self):
        pass

    def exe(self):
        raise NotImplemented()

    def log(self, msg):
        d = datetime.now()
        stderr.write('[' + d.strftime('%Y-%m-%d %H:%M:%S') + '] ' + msg + '\n')
        stderr.flush()


class QSyncExperiment(Experiment):
    def __init__(self, clxp, job_opts, qsync_filename, qsync_opts={}):
        Experiment.__init__(self, clxp.pset, values=values)
        self.clxp = clxp
        self.job_opts = job_opts
        self.qsync_filename = qsync_filename
        self.qsync_opts = qsync_opts

    def pre(self):
        self.clxp.pre()
        self.qsync_file = open(self.qsync_filename, 'w')

    def exe(self):
        d = dict(self.clxp._dict())
        self.qsync_file.write('-V -cwd')
        if self.job_opts:
            self.qsync_file.write(' ')
            self.qsync_file.write(CommandlineExperiment.expandstring(d, self.job_opts))
        if self.clxp.out is not None:
            self.qsync_file.write(' -o ')
            self.qsync_file.write(CommandlineExperiment.expandstring(d, self.clxp.out))
        if self.clxp.err is not None:
            self.qsync_file.write(' -e ')
            self.qsync_file.write(CommandlineExperiment.expandstring(d, self.clxp.err))
        self.qsync_file.write(' -- ')
        self.qsync_file.write(CommandlineExperiment.expandstring(d, self.clxp.cl))
        self.qsync_file.write('\n')

    def post(self):
        self.qsync_file.close()
        qsync = QSync()
        qsync.filenames = (self.qsync_filename,)
        qsync.go(**self.qsync_opts)
        self.clxp.post()

        
class CommandlineExperiment(Experiment):
    def __init__(self, pset, cl, output_path, sep='_', shell=None, cd=False, pre_cl=None, post_cl=None, out=None, err=None, values={}):
        Experiment.__init__(self, pset, values=values)
        self.cl = cl
        self.output_path = output_path
        self.sep = sep
        self.shell = shell
        self.cd = cd
        self.pre_cl = pre_cl
        self.post_cl = post_cl
        self.out = out
        self.err = err

    def _param_dir(self, p):
        return p.name + self.sep + p.svalue()

    def _pset_dir(self, pset):
        d = tuple(self._param_dir(p) for p in pset.params.values())
        return os.path.join(self.output_path, *d)

    def _dict(self):
        for name, p in self.pset.params.items():
            yield name, p.current
            yield 's_' + name, p.svalue()
        for pset in self.pset.ancestors(include_self=True):
            d = self._pset_dir(pset)
            if not os.path.exists(d):
                makedirs(d)
            yield 'd_' + pset.name, d

    @staticmethod
    def expandstring(d, s):
        return os.path.expanduser(os.path.expandvars(s % d))

    @staticmethod
    def _open_out(filename, d):
        if filename is None:
            return None
        expanded = CommandlineExperiment.expandstring(d, filename)
        return open(expanded, 'w')

    def exe(self):
        if self.cd:
            wd = self._pset_dir(self.pset)
        else:
            wd = None
        d = dict(self._dict())
        out = CommandlineExperiment._open_out(self.out, d)
        err = CommandlineExperiment._open_out(self.err, d)
        cl = CommandlineExperiment.expandstring(d, self.cl)
        p = subprocess.Popen(cl, shell=True, executable=self.shell, cwd=wd, stdout=out, stderr=err, close_fds=True)
        p.wait()
        if p.returncode != 0:
            self.log('process has FAILED')

    def pre(self):
        if self.pre_cl is not None:
            p = subprocess.Popen(self.pre_cl, shell=True, executable=self.shell)
            p.wait()
            if p.returncode != 0:
                self.log('preprocess has FAILED')

    def post(self):
        if self.post_cl is not None:
            p = subprocess.Popen(self.post_cl, shell=True, executable=self.shell)
            p.wait()
            if p.returncode != 0:
                self.log('postprocess has FAILED')


class GridXP(ArgumentParser):
    def __init__(self):
        ArgumentParser.__init__(self, description='Perform a grid experiment')
        self.add_argument('xp_filename', metavar='XPFILE', type=str, nargs=1, help='file containing the experiment definition object')
        self.add_argument('--test', dest='test', action='store_true', default=False, help='test run (only one parameter value set)')
        self.add_argument('--xp-path', metavar='PATH', dest='xp_paths', action='append', type=str, default=['.'], help='add path where to search for experiment file')
        self.add_argument('--pset-path', metavar='PATH', dest='pset_paths', action='append', type=str, default=['.'], help='add path where to search for parameter set files')

    def go(self):
        args = self.parse_args()
        Experiment.search_paths = args.xp_paths
        ParamSet.search_paths = args.pset_paths
        xp = Experiment.load(args.xp_filename[0])
        xp.run(test=args.test)


if __name__ == '__main__':
    GridXP().go()
