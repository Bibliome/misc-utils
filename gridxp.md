# `gridxp.py`

## A parameter screening experiment

`gridxp.py` runs a command for each combination of several specified parameters.
`gridxp.py` helps to handle separate directories for each parameter value set, runs commands on the spot or submits to a Grid Engine.

Screening experiments are configured through files using a Python syntax.

## Prerequisites

* Python
* [`qsync.py`](https://github.com/Bibliome/misc-utils/blob/master/qsync.md) to run on a cluster

## Usage

```
gridxp.py [OPTIONS] [XPFILES...]
```

## Options

| **Option** | **Description** |
|------------|-----------------|
| `--help` | show brief help message and exit |
| `--test` | test run, launch only one parameter value set and exit |
| `--load-path PATH` | add *PATH* to the search paths for experiments files; this option can be specified several times |
| `--local` | force local execution, do not submit jobs to the Grid Engine |
| `--dry-run` | do not actually run commands, just print them; if using a GE, then generates a specification file for `qsync.py` but does not submit jobs |
| `--update` | only execute each command if the corresponding output file does not exist |
| `--param-values NAME VALUES` | set the values of parameter *PARAM*; *VALUES* must be a valid Python expression that returns a collection |
| `--insert-param-dir` | insert parameter directory for *PARAM* in existing directory structure, instead of running the experiment |

## Experiment files

Experiment files specify parameter, parameter values, and operational configuration of an experiment.
An experiment file is a sequence of Python statements with a limited amount of functions in the scope.

#### `param(NAME, [fmt=FMT], [domain=DOMAIN])`

Creates a parameter named *NAME*. *NAME* must be a Python string.
The parameter name is the basis for created directories, so it is recommended to use short names that contain only regular characters.

*FMT* is a format specifier for a [Python string interpolation](https://docs.python.org/3/library/stdtypes.html#printf-style-string-formatting). By default *FMT* is `s`.
For numeric parameters, a fixed-width format is recommended (`02d` or `6.2f`).

*DOMAIN* specifies the domain of possible values for the parameter. It must be a valid Python collection.
When the parameter is set to a value `gridxp.py` checks that the value is in *DOMAIN*.
By default *DOMAIN* is `None` and no check is performed.

The order of declaration of parameters is remembered and important. `gridxp.py` creates a tree structure which depends on the parameter order and value.

Aliases: `addparam`, `add_param`.

#### `commandline(CL)`

Specifies the command line to run for each parameter value set. *CL* is a Python string that is modified:
* Tilde (`~`) is expanded as the current user home directory.
* Environment variables (`$NAME`) are expanded with the current environment.
* The string is interpolated with a dictionary with the following keys:
  * `PARAM`: the current value of parameter *PARAM*; it is up to you to chose an appropriate formatter specification.
  * `s_PARAM`: the current value of parameter *PARAM* formatted with [this parameter *fmt* option](#paramname-fmtfmt-domaindomain); the formatter specification should be `s`.
  * `d_PARAM`: the path to a directory which is unique for the current value of *PARAM* and of each one of its predecessor parameters.
  * `PROP`: the current value of property *PROP*; it is up to you to chose an appropriate formatter specification.

Aliases: `cl`, `cmdline`, `command_line`.

#### `outdir(DIR)`

Specifies the path to the base directory of the parameter directory tree. *DIR* must be a Python string.

Aliases: `od`, `outputdir`, `out_dir`, `output_dir`.

#### `pre_cl(CL)`

Specifies a command line to run at the start of the experiment. This command is executed before all commands specified with [`commandline()`](#commandlinecl).

*CL* is a Python string. Tilde (`~`) and environment variable expansion are performed.

Aliases: `pre_commandline`, `pre_cmdline`, `pre_command_line`.

#### `post_cl(CL)`

Specifies a command line to run at the end of the experiment. This command is executed after all commands specified with [`commandline()`](#commandlinecl).

*CL* is a Python string. Tilde (`~`) and environment variable expansion are performed.

Aliases: `post_commandline`, `post_cmdline`, `post_command_line`.

#### `outfile(PATH)`

Specifies the file where to redirect the standard output of the command [`commandline()`](#commandlinecl).
The file path is relative to the last parameter directory.

*PATH* is a Python string expanded in the same way as [`commandline()`](#commandlinecl).

Aliases: `out_file`.

#### `errfile(PATH)`

Specifies the file where to redirect the standard error of the command [`commandline()`](#commandlinecl).
The file path is relative to the last parameter directory.

*PATH* is a Python string expanded in the same way as [`commandline()`](#commandlinecl).

Aliases: `err_file`.

#### `job_options(OPTS)`

Specifies options to pass to `qsub` when the experiment is executed on a Grid Engine.

*OPTS* is a Python string expanded in the same way as [`commandline()`](#commandlinecl).

Aliases: `job_opts`.

#### `qsync_file(FILE)`

When executing commands through a cluster, `gridxp.py` creates a job list file for `qsync.py`.
`qsync_file()` specifies the path to this file.

*FILE* is a Pyhton string expanded for tilde (`~`) and environment variables.

Aliases: `qf`, `qsync_filename`.

#### `qsync_opts(**OPTS)`

Specifies options to pass to `QSync#go()`.

#### `paramvalues(PARAM, *VALUES)`

Specifies all the values for a parameter.
*PARAM* is a Python string and must be the name of a parameter declared with [`param()`](#paramname-fmtfmt-domaindomain).

*VALUES* is a Python collection and each element will be checked against the parameter domain.

Aliases: `param_values`.

#### `param_accept(PRED)`

Adds a constraint on parameter values.
By default `gridxp.py` will run the command for all combinations of parameter values.
Use this directive to exclude certain combinations that would not make sense.

*PRED* is a Python callable object that takes a single parameter.
This parameter is a dictionary object that contains the current value for each parameter.
This dictionary also has convenience attributes named after each parameter.

For each combination of parameter values `gridxp.py` calls all functions specified with `param_accept()`.
If one of these returns a false value, then the combination is rejected.

#### `property(NAME, VALUE)`

Declares a property named *NAME* with value *VALUE*.
The property can be used in expanded strings as in commandline(), outfile(), errfile(), job_options().

*VALUE* is either a Python string or a callable object.
If *VALUE* is a string, then this property value is a constant.
The string value is not expanded.

If *VALUE* is a callable object, it must accept a single parameter.
The `gridxp.py` will call it for each parameter value combination passing them as a dictionary.
The callable object must return a string value that will set the value for the property.

#### `local()`

Force local execution, do not submit jobs to the Grid Engine.
Equivalent to `--local`.

#### `dry_run()`

Do not actually run commands, just print them; if using a GE, then generates a specification file for `qsync.py` but does not submit jobs.
Equivalent to `--dry-run`.

#### `include(FILE)`

Includes directives contained in *FILE*.
The file will be searched in directores specified by [`--load-path`](#options).

## Simple example

Paste the following directives into a file, let's call it `test.xp`.
```
param('a', fmt='02d')
param('b', fmt='02d')

outdir('gridxp_example')
commandline('echo %(a)d+%(b)d | bc')
outfile('%(d_b)s/result.txt')

paramvalues('a', 1, 2, 3, 4)
paramvalues('b', 3, 5, 7, 11)
```

Now run the command:

```
gridxp.py test.xp
```

It will output something similar to this:

```
[2018-10-16 19:45:02] loading experiment configuration from ./foo.xp
[2018-10-16 19:45:02] running pre-process
[2018-10-16 19:45:02] defaulting to local executor
[2018-10-16 19:45:02] running process for a=01, b=03
[2018-10-16 19:45:02] running process for a=01, b=05
[2018-10-16 19:45:02] running process for a=01, b=07
[2018-10-16 19:45:02] running process for a=01, b=11
[2018-10-16 19:45:02] running process for a=02, b=03
[2018-10-16 19:45:02] running process for a=02, b=05
[2018-10-16 19:45:02] running process for a=02, b=07
[2018-10-16 19:45:02] running process for a=02, b=11
[2018-10-16 19:45:02] running process for a=03, b=03
[2018-10-16 19:45:02] running process for a=03, b=05
[2018-10-16 19:45:02] running process for a=03, b=07
[2018-10-16 19:45:02] running process for a=03, b=11
[2018-10-16 19:45:02] running process for a=04, b=03
[2018-10-16 19:45:02] running process for a=04, b=05
[2018-10-16 19:45:02] running process for a=04, b=07
[2018-10-16 19:45:02] running process for a=04, b=11
[2018-10-16 19:45:02] running post-process
```

`gridxp.py` has created a directory structure in `gridxp_example` like this:

```
gridxp_example
├── a_01
│   ├── b_03
│   │   └── result.txt
│   ├── b_05
│   │   └── result.txt
│   ├── b_07
│   │   └── result.txt
│   └── b_11
│       └── result.txt
├── a_02
│   ├── b_03
│   │   └── result.txt
│   ├── b_05
│   │   └── result.txt
│   ├── b_07
│   │   └── result.txt
│   └── b_11
│       └── result.txt
├── a_03
│   ├── b_03
│   │   └── result.txt
│   ├── b_05
│   │   └── result.txt
│   ├── b_07
│   │   └── result.txt
│   └── b_11
│       └── result.txt
└── a_04
    ├── b_03
    │   └── result.txt
    ├── b_05
    │   └── result.txt
    ├── b_07
    │   └── result.txt
    └── b_11
        └── result.txt
```
