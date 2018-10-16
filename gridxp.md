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
| `--load-path PATH` | add path where to search for experiments files; this option can be speciied multiple times |
| `--local` | force local execution, do not submit jobs to the Grid Engine |
| `--dry-run` | do not actually run commands, just print them; if using a GE, then generates a specification file for `qsync.py` but do not submit jobs |
| `--param-values NAME VALUES` | set the values of a parameter; `VALUES` must be a valid Python expression that returns a collection |

## Experiment files

Experiment files specify parameter, parameter values, and operational configuration of an experiment.
An experiment file is a sequence of Python statements with a limited amount of functions in the scope.

#### `param(NAME, [fmt=FMT], [domain=DOMAIN])`

Creates a parameter named `NAME`. `NAME` must be a Python string.
The parameter name is the basis for created directories, so it is recommended to use short names that contains only regular characters.

`FMT` is a format specifier for a Python string interpolation. By default `FMT` is `s`.
For numeric parameters, a fixed-width format is recommended (`02d` or `6.2f`).

`DOMAIN` specifies the domain of possible values for the parameter. It must be a valid Python collection.
When the parameter is set to a value `gridxp.py` checks that the value is in `DOMAIN`.
By default `DOMAIN` is `None` and no check is performed.

The order of declaration of parameters is remembered and important. `gridxp.py` creates a tree structure which depends on the parameter order and values.

Aliases: `addparam`, `add_param`.

#### `commandline(CL)`

`CL` specifies the command line to run for each parameter value set. `CL` is a Python string that is modified:
* Tilde (`~`) is expanded as the current user home directory.
* Environment variables (`$NAME`) are expanded with the current environment.
* The string is interpolated with a dictionary with the following keys
  * `PARAM`: the current value of parameter `PARAM`, it is up to the user to chose an appropriate formatter specification.
  * `s_PARAM`: the current value of parameter `PARAM` formatted with this parameter `fmt` option, the formatter specification should be `s`.
  * `d_PARAM`: the path to a directory which is unique for the current values of `PARAM` and each parameter declared before `PARAM`.

