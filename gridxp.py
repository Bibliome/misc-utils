#!/usr/bin/python

import itertools
from collections import OrderedDict
import os.path
import subprocess
from os import makedirs
from argparse import ArgumentParser

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
    @classmethod
    def load(cls, filename):
        f = open(filename)
        s = '\n'.join(f)
        f.close()
        r = eval(s)
        if not isinstance(r, cls):
            raise ValueError('expected %s, got %s' % (cls, r.__class__))
        return r

    
class ParamSet(Loadable):
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
    def __init__(self, pset):
        self.pset = pset

    def run(self, test=False, values=None):
        if values is not None:
            self.pset.set_values(values)
        self.pre()
        for _ in self.pset.cells():
            self.exe()
            if test:
                break
        self.post()

    def test(self, values=None):
        self.run(test=True, values=values)

    def pre(self):
        pass

    def post(self):
        pass

    def exe(self):
        raise NotImplemented()


class CommandlineExperiment(Experiment):
    def __init__(self, pset, cl, sep='=', shell=None, cd=False):
        Experiment.__init__(self, pset)
        self.cl = cl
        self.sep = sep
        self.shell = shell
        self.cd = cd

    def _param_dir(self, p):
        return p.name + self.sep + p.svalue()

    def _pset_dir(self, pset):
        d = tuple(self._param_dir(p) for p in pset.params.values())
        return os.path.join(*d)
        
    def _env(self):
        for p in self.pset.params.values():
            yield ('p_' + p.name, str(p.current))
            yield ('s_' + p.name, p.svalue())
        for pset in self.pset.ancestors(include_self=True):
            d = self._pset_dir(pset)
            if not os.path.exists(d):
                makedirs(d)
            yield ('d_' + pset.name, d)

    def exe(self):
        env = dict(self._env())
        if self.cd:
            wd = self._pset_dir(self.pset)
        else:
            wd = None
        p = subprocess.Popen(self.cl, shell=True, env=env, executable=self.shell, cwd=wd)
        p.wait()




class GridXP(ArgumentParser):
    def __init__(self):
        ArgumentParser.__init__(self, description='Perform a grid experiment')
        self.add_argument('xp_filename', metavar='XPFILE', type=str, nargs=1, help='file containing the experiment definition object')
        self.add_argument('--test', dest='test', action='store_true', default=False, help='test run (only one parameter value set)')

    def go(self):
        args = self.parse_args()
        print args.xp_filename
        xp = Experiment.load(args.xp_filename[0])
        xp.run(test=args.test)


if __name__ == '__main__':
    GridXP().go()
