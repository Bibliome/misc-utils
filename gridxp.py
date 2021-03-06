#!/usr/bin/env python3

from sys import stderr, stdout
import itertools
from collections import OrderedDict
import os.path
import subprocess
from os import makedirs, listdir, remove
from argparse import ArgumentParser
from datetime import datetime
import shutil
import traceback
try:
    from qsync import QSync
except ImportError:
    QSync = None


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



class ObjectDict(dict):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(name)

        
class Experiment:
    def __init__(self):
        self.params = OrderedDict()
        self.accept = []
        
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

    def accept_params(self, pvs):
        paramdict = ObjectDict(zip(self.params.keys(), pvs))
        for f in self.accept:
            if not f(paramdict):
                return False
        return True
    
    def cells(self):
        for p in self.params.values():
            if len(p.values) == 0:
                raise ValueError('empty values for %s' % p.name)
        paramvalues = tuple(p.values for p in self.params.values())
        for pvs in itertools.product(*paramvalues):
            if self.accept_params(pvs):        
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
        self.fun = None
        self.output_dir = None
        self.sep = '_'
        self.shell = None
        self.cd = False
        self.pre_cl = None
        self.pre_fun = None
        self.post_cl = None
        self.post_fun = None
        self.out = None
        self.err = None
        self.job_opts = None
        self.qsync_filename = 'gridxp.qsync'
        self.qsync_opts = {}
        self.executor = None
        self.dry_run = False
        self.properties = {}
        self.update = False
        self.delete_out = False

    def load_config(self, filename):
        found = searchfile(filename)
        log('loading experiment configuration from ' + found)
        #execfile(found, self.create_locals())
        exec(open(found).read(), self.create_locals())

    def create_locals(self):
        cl = self._attrsetter('cl')
        fun = self._attrsetter('fun')
        od = self._attrsetter('output_dir')
        cd = self._attrsetter('cd')
        pre_cl = self._attrsetter('pre_cl')
        pre_fun = self._attrsetter('pre_fun')
        post_cl = self._attrsetter('post_cl')
        post_fun = self._attrsetter('post_fun')
        out = self._attrsetter('out')
        err = self._attrsetter('err')
        jo = self._attrsetter('job_opts')
        qf = self._attrsetter('qsync_filename')
        delo = self._attrsetter('delete_out')
        param = self._paramdef()
        pv = self._paramvaluessetter()
        pa = self._paramaccept()
        prop = self._propdef()
        return dict(
            param=param, addparam=param, add_param=param,
            cl=cl, commandline=cl, cmdline=cl, command_line=cd,
            fun=fun, function=fun,
            od=od, outdir=od, outputdir=od, out_dir=od, output_dir=od,
            alternate_shell=self._attrsetter('shell'),
            cd=cd, changedir=cd, change_dir=cd, change_directory=cd,
            pre_cl=pre_cl, pre_commandline=pre_cl, pre_cmdline=pre_cl, pre_command_line=pre_cl,
            pre_fun=pre_fun, pre_function=pre_fun,
            post_cl=post_cl, post_commandline=post_cl, post_cmdline=post_cl, post_command_line=post_cl,
            post_fun=post_fun, post_function=post_fun,
            outfile=out, out_file=out,
            errfile=err, err_file=err,
            job_opts=jo, job_options=jo,
            qf=qf, qsync_filename=qf, qsync_file=qf,
            delete_out=delo, delete_output=delo,
            paramvalues=pv, param_values=pv,
            paramaccept=pa, param_accept=pa,
            prop=prop, property=prop,
            qsync_opts=self._qsync_opts(),
            local=self.local_execution,
            include=(lambda filename: self.load_config(filename)),
            dry_run=self._attrsetter('dry_run'),
        )

    def local_execution(self):
        self.executor = LocalExecutor

    def _attrsetter(self, name):
        return lambda value: setattr(self, name, value)

    def _propdef(self):
        def result(name, fun):
            self.properties[name] = fun
        return result

    def _paramaccept(self):
        def result(fun):
            self.accept.append(fun)
        return result

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
            self.qsync_opts = kwargs
        return result

    def _delete_output_files(self, d):
        self._delete_output_file(d, self.out)
        self._delete_output_file(d, self.err)

    def _delete_output_file(self, d, filename):
        if self.delete_out and filename is not None:
            xfn = ExperimentConfig.expand(d, filename)
            log('deleting ' + xfn)
            remove(xfn)
            
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

    def _dict(self, props):
        params = {}
        for name, p in self.params.items():
            yield name, p.current
            yield 's_' + name, p.svalue()
            params[name] = p.current
        d = self.output_dir
        if not os.path.exists(d):
            makedirs(d)
        for p in self.params.values():
            d = os.path.join(d, self._param_dirname(p))
            if not os.path.exists(d):
                makedirs(d)
            yield 'd_' + p.name, d
        if props:
            for name, fun in self.properties.items():
                if hasattr(fun, '__call__'):
                    yield name, fun(params)
                else:
                    yield name, fun

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

    def test_exe(self, d):
        if not self.update:
            return True
        if self.out is None:
            return True
        expanded = ExperimentConfig.expand(d, self.out)
        return not os.path.exists(expanded)

    def exe(self):
        d = dict(self._dict(True))
        if self.test_exe(d):
            self.executor.exe(self, d)
        else:
            log('(update) output file exists, skip')
            

    def insert_param_dir(self, name):
        self.params = self._sub_params_to(name)
        lastp = self.params[name]
        if len(lastp.values) > 1:
            raise Exception('can only insert parameter with a single value')
        for _ in self.cells():
            d = dict(self._dict(False))
            paramdir = d['d_'+name]
            parentdir, dirname = os.path.split(paramdir)
            for fobj in listdir(parentdir):
                if fobj != dirname:
                    if self.dry_run:
                        log('(dry run) move %s to %s' % (os.path.join(parentdir, fobj), paramdir))
                    else:
                        shutil.move(os.path.join(parentdir, fobj), paramdir)

    def _sub_params_to(self, name):
        params = OrderedDict()
        for k, p in self.params.items():
            params[k] = p
            if k == name:
                return params
        raise Exception('Parameter not found: %s' % name)
        


class LocalExecutor:
    @staticmethod
    def pre(config):
        if config.pre_cl is not None:
            p = subprocess.Popen(config.pre_cl, shell=True, executable=config.shell)
            p.wait()
            if p.returncode != 0:
                log('preprocess has FAILED')
        elif config.pre_fun is not None:
            try:
                config.pre_fun()
            except:
                log('pre-function has FAILED')
                traceback.print_exc()
                
    @staticmethod
    def post(config):
        if config.post_cl is not None:
            p = subprocess.Popen(config.post_cl, shell=True, executable=config.shell)
            p.wait()
            if p.returncode != 0:
                log('postprocess has FAILED')
        elif config.post_fun is not None:
            try:
                config.post_fun()
            except:
                log('post-function has FAILED')
                traceback.print_exc()

    @staticmethod
    def exe(config, d):
        out = ExperimentConfig._open_out(config.out, d)
        err = ExperimentConfig._open_out(config.err, d)
        if config.cl is not None:
            cl = ExperimentConfig.expand(d, config.cl)
            if config.dry_run:
                log('(dry run) ' + cl)
            else:
                config._delete_output_files(d)
                p = subprocess.Popen(cl, shell=True, executable=config.shell, stdout=out, stderr=err, close_fds=True)
                p.wait()
                if p.returncode != 0:
                    log('process has FAILED')
        else:
            if config.dry_run:
                log('(dry run)' )
            else:
                config._delete_output_files(d)
                try:
                    config.fun(tuple(config.params), d, out, err)
                except:
                    log('function has FAILED')
                    traceback.print_exc()
                    

    @staticmethod
    def ready(config):
        if config.params is None:
            return False
        if config.cl is None and config.fun is None:
            return False
        if config.cl is not None and config.fun is not None:
            return False
        if config.pre_cl is not None and config.pre_fun is not None:
            return False
        if config.post_cl is not None and config.post_fun is not None:
            return False
        if config.output_dir is None:
            return False
        return True

    @staticmethod
    def check(config):
        if config.params is None or len(config.params) == 0:
            raise ValueError('no parameters specified')
        if config.cl is None and config.fun is None:
            raise ValueError('no command line or function specified')
        if config.cl is not None and config.fun is not None:
            raise ValueError('both command line and function specified')
        if config.pre_cl is not None and config.pre_fun is not None:
            raise ValueError('both pre command line and pre function specified')
        if config.post_cl is not None and config.post_fun is not None:
            return ValueError('both post command line and post function specified')
        if config.output_dir is None:
            raise ValueError('no output directory specified')



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
    def exe(config, d):
        config.qsync_file.write('-V -cwd')
        QSyncExecutor._write_qsync_opt(config, d, ' ', config.job_opts)
        QSyncExecutor._write_qsync_opt(config, d, ' -o ', config.out)
        QSyncExecutor._write_qsync_opt(config, d, ' -e ', config.err)
        config.qsync_file.write(' -- ')
        config.qsync_file.write(ExperimentConfig.expand(d, config.cl))
        config.qsync_file.write('\n')
        if not config.dry_run:
            config._delete_output_files(d)

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
            log('no job options specified')
            return False
        if QSync is None:
            log('qsync not imported')
            return False
        return True

    @staticmethod
    def check(config):
        LocalExecutor.check(config)
        if config.job_opts is None:
            raise ValueError('mising qopts()')


class GridXP(ArgumentParser):
    def __init__(self):
        ArgumentParser.__init__(self, description='Perform a grid experiment', epilog='For detailed documentation on experiment definition, see https://github.com/Bibliome/misc-utils/edit/master/gridxp.md')
        self.add_argument('xp_filenames', metavar='XPFILE', type=str, nargs='+', default=[], help='file containing the experiment definition object')
        self.add_argument('--test', dest='test', action='store_true', default=False, help='test run, launch only one parameter value set and exit')
        self.add_argument('--load-path', metavar='PATH', dest='load_paths', action='append', type=str, default=['.'], help='add PATH to the search paths for experiments files; this option can be specified several times')
        self.add_argument('--local', dest='local', action='store_true', default=False, help='force local execution, do not submit jobs to the Grid Engine')
        self.add_argument('--dry-run', dest='dry_run', action='store_true', default=False, help='do not actually run commands, just print them; if using a GE, then generates a specification file for qsync.py but do not submit jobs')
        self.add_argument('--update', dest='update', action='store_true', default=False, help='only execute each command if the corresponding output file does not exist')
        self.add_argument('--delete-out', dest='delo', action='store_true', default=False, help='delete output and error files before running (will not delete if dry run or updating).')
        self.add_argument('--param-values', metavar=('NAME', 'VALUES'), dest='param_values', nargs=2, action='append', type=str, default=[], help='set the values of parameter PARAM; VALUES must be a valid Python expression that returns a collection')
        self.add_argument('--insert-param-dir', metavar='PARAM', action='store', type=str, dest='insert_param_dir', default=None, help='insert parameter directory for PARAM in existing directory structure, instead of running the experiment')
        self.add_argument('--list-params', action='store_true', dest='list_params', default=False, help='list parameters, instead of running the experiment')

    def go(self):
        args = self.parse_args()
        global LOAD_PATHS
        LOAD_PATHS = args.load_paths
        xp = ExperimentConfig()
        xp.dry_run = args.dry_run
        xp.update = args.update
        for fn in args.xp_filenames:
            xp.load_config(fn)
        if args.local:
            xp.local_execution()
        for name, svalues in args.param_values:
            xp.set_param_values(name, eval(svalues))
        if args.delo:
            xp.delete_out = True
        if args.insert_param_dir is not None:
            if args.list_params:
                raise Exception('--insert-param-dir and --list-params are mutually exclusive')
            xp.insert_param_dir(args.insert_param_dir)
        elif args.list_params:
            for p in xp.params.values():
                stdout.write('%s\n' % p.name)
                for v in p.values:
                    stdout.write('  %s\n' % p.order_fmt % v)
        else:
            xp.run(test=args.test)

if __name__ == '__main__':
    GridXP().go()
