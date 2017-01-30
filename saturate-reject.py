#!/bin/env python

import re
from sys import stdin
from collections import defaultdict
from optparse import OptionParser, OptionGroup

def curly_replace(fmt, matcher):
    groups = list(matcher.groups())
    groups.insert(0, matcher.group())
    return fmt.format(*groups, **matcher.groupdict())

def backref_replace(fmt, matcher):
    return matcher.expand(fmt)

class Saturate:
    def __init__(self, expr, name, formats, replace_fun):
        if isinstance(expr, unicode):
            self.expr = re.compile(expr, re.U)
        elif isinstance(expr, str):
            self.expr = re.compile(expr)
        else:
            self.expr = expr
        self.name = str(name)
        self.formats = tuple(formats)
        self.replace_fun = replace_fun

    def _match(self, s):
        result = self.expr.match(s)
        if result is None:
            return None
        if result.end() < len(s):
            return None
        return result

    def matches(self, s):
        return self._match(s) is not None

    def replacements(self, s):
        matcher = self._match(s)
        if matcher is not None:
            for rep in self.formats:
                yield self.replace_fun(rep, matcher)

def _load_saturate_line(line, replace_fun):
    cols = line.split('\t')
    return Saturate(cols[0], cols[1], cols[2:], replace_fun)

def load_saturate(f, replace_fun):
    return tuple(_load_saturate_line(line.strip(), replace_fun) for line in f)

def all_saturate(count, sats, s):
    yield s
    for sat in sats:
        seen = False
        for r in sat.replacements(s):
            seen = True
            yield r
        if seen:
            count[sat.name] += 1

def saturate(count, sats, f, column, separator):
    for line in f:
        line = line.strip()
        if column is None:
            cols = None
            s = line
        else:
            cols = line.split(separator)
            s = cols[column]
        for r in all_saturate(count, sats, s):
            if column is None:
                print r
            else:
                cols[column] = r
                print '\t'.join(cols)

if __name__ == '__main__':
    parser = OptionParser(
        usage='usage: %prog [options] [files]',
        description='Saturate dictionaries with regular expression patterns',
        epilog='Saturation patterns file format: one per line with 3 or more tab-separated columns: 1. Regular expression (must match the whole entry) 2. Pattern name                      3+. Replacement patterns'
        )
    parser.add_option('-c',
                      '--column',
                      action='store',
                      type='int',
                      dest='column',
                      default=None,
                      help='saturate the COLth column (start at 0, by default saturate the whole line)',
                      metavar='COL'
                      )
    parser.add_option('-S',
                      '--separator',
                      action='store',
                      type='string',
                      dest='separator',
                      default='\t',
                      help='column separator (default: tab)',
                      metavar='SEP'
                      )
    parser.add_option('-s',
                      '--saturation-file',
                      action='append',
                      type='string',
                      dest='saturation_file_list',
                      default=[],
                      help='file containing saturation patterns',
                      metavar='FILE'
                      )
    parser.add_option('-B',
                      '--backref-format',
                      action='store_const',
                      dest='replace_fun',
                      const=backref_replace,
                      default=curly_replace,
                      help='replacement templates in regular expression backreference syntax (backslash instead of curly braces)'
                      )
    
    options, args = parser.parse_args()

    saturates = []
    for fn in options.saturation_file_list:
        f = open(fn)
        saturates.extend(load_saturate(f, options.replace_fun))
        f.close()

    count = defaultdict(int)
    if args:
        for fn in args:
            f = open(fn)
            saturate(count, saturates, f, options.column, options.separator)
            f.close()
    else:
        saturate(count, saturates, stdin, options.column, options.separator)
