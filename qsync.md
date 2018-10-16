# `qsync.py`

## A batch job submission script for the Grid Engine

`qsync.py` reads a list of job specifications for the [Grid Engine scheduler](http://gridscheduler.sourceforge.net/),
submits the job to the scheduler, and polls for their status.

One can specify several behaviours in the event that a job fails: cancel every other job, resubmit job (with a count limit), or proceed.

## Prerequisites

* A deployed Grid Engine
* The environment variable `SGE_ROOT` set properly
* Python
* the [`drmaa` Python library](http://drmaa-python.github.io/)

## Usage

```
qsync.py [OPTIONS] [FILE...]
```

### Options

| **Short** | **Long** | **Description** |
|------|----------|----------------------------------|
| `-h` | `--help` | show brief help message and exit |
| `-l FILE` | `--log-file FILE` | write log into `FILE` (default is to `stderr`) |
| `-i T` | `--interval T | wait `T` seconds before polling job status, values lower than 10 require `--force-interval` (default is 60s) |
| | `--force-interval` | accept poll intervals lower than 10 seconds |
| `-s` | `--stop-on-failure` | if one job fails, cancel queued jobs, terminate running jobs, and return with non-zero exit status |
| `-p` | `--proceed-on-failure` | continue running jobs even if some fail (default behaviour) |
| `-r N` | `--resubmit-on-failure=N` | resubmit failed jobs, a job will be submitted at most `N` times |

### Job specifications file

Each job is specified in a single line.
The specification line contains two parts separated by a double dash (`--`).

`qsync.py` interprets options before `--` as options for GE.
These options are passed as they are to `qsub`.

The remainder after `--` is the command-line to execute on the cluster.
The first token is thus the executable.

#### Example

```
-V -cwd -o out.txt -e err.txt -- java -jar heavy-stuff.jar
```
