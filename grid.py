#!/usr/bin/env python

import itertools
from ConfigParser import RawConfigParser
from sys import argv
from string import Template

class Experiment:
    def __init__(self):
        self.parameters = {}
        self.default_order = []
        self.constraints = []

    def add_constraint(self, c):
        self.constraints.append(c)

    def add_param(self, name, values):
        if name in self.parameters:
            raise ValueError('duplicate parameter "%s"' % name)
        if not values:
            raise ValueError('empty value set for parameter "%s"' % name)
        self.parameters[name] = tuple(values)
        self.default_order.append(name)

    def check_param(self, name):
        if name not in self.parameters:
            raise ValueError('undefined parameter "%s"' % param)

    def check_value(self, name, value):
        self.check_param(name)
        if value not in self.parameters[name]:
            raise ValueError('undefined value "%s" for parameter "%s"' % (value, name))

    def accept_run(self, run):
        for c in self.constraints:
            if not c(run):
                return False
        return True

    def check_run(self, run):
        for param, value in run.iteritems():
            self.check_value(param, value)

    def stack_run(self, runs, param, value):
        self.check_value(param, value)
        for run in runs:
            self.check_run(run)
            if param in run and self.accept_run(run):
                yield run
            new_run = dict(run.iteritems())
            new_run[param] = value
            if self.accept_run(new_run):
                yield new_run

    def _complete_run(self, run):
        self.check_run(run)
        unset = tuple(param for param in self.parameters if param not in run)
        if unset:
            all_values = tuple(self.parameters[param] for param in unset)
            for values in itertools.product(*all_values):
                new_run = dict(run.iteritems())
                for param, value in zip(unset, values):
                    new_run[param] = value
                if self.accept_run(new_run):
                    yield new_run
        elif self.accept_run(run):
            yield run

    def complete(self, runs=None):
        if runs is None:
            runs = ({},)
        for run in runs:
            for new_run in self._complete_run(run):
                yield new_run



class Grid:
    def __init__(self):
        self.xp = Experiment()
        self.runs = ({},)
        self.output_base = 'output'
        self.output_commands = []
        self.output_order = None

    def read(self, *files):
        self.config = RawConfigParser(allow_no_value=True)
        self.config.read(*files)
        self._read_config()

    def read_stdin(self):
        self.config = RawConfigParser(allow_no_value=True)
        self.config.read_fp(stdin)
        self._read_config()

    def _read_config(self):
        self._read_config_params()
        self._read_config_seeds()
        self._read_config_output()
        
    def _read_config_params(self):
        for param, svalues in self.config.items('parameters'):
            values = svalues.split()
            self.xp.add_param(param, values)

    def _read_config_seeds(self):
        if self.config.has_section('seeds'):
            self.runs = ({},)
            for seed, _ in self.config.items('seeds'):
                try:
                    param, value = seed.split('.')
                except ValueError:
                    raise ValueError('seeds must be in the form "param.value": ' + seed)
                self.runs = tuple(self.xp.stack_run(self.runs, param, value))

    def _read_config_output(self):
        commands = []
        for opt, val in self.config.items('output'):
            if opt == 'base':
                self.output_base = val
            elif opt.startswith('cmd'):
                n = int('0' + opt[3:])
                commands.append((n, val))
            elif opt == 'order':
                self.output_order = val.split()
                for param in self.output_order:
                    self.xp.check_param(param)
                for param in xp.parameters:
                    if param not in self.output_order:
                        self.output_order.append(param)
        if commands:
            self.output_commands = tuple(Template(cmd) for (n, cmd) in sorted(commands))

    def output(self):
        if self.output_order is None:
            order = self.xp.default_order
        else:
            order = self.output_order
        if not self.output_commands:
            raise ValueError('no command specified')
        for run in self.xp.complete(self.runs):
            rundir = self.output_base + '/' + '/'.join(run[param] for param in order)
            for cmd in self.output_commands:
                print cmd.substitute(run, base=self.output_base, rundir=rundir)



if __name__ == '__main__':
    grid = Grid()
    grid.read(argv[1:])
    grid.output()

