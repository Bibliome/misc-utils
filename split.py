#!/usr/bin/env python

from sys import stdin, stderr
from optparse import OptionParser, OptionGroup
import re

def log(msg, *args):
    stderr.write(msg % args)
    stderr.write('\n')

class Splitter:
    def __init__(self, options):
        self.options = options
        self.entries = 0
        self.current = 0
        self.fout = None
        self.inentry = False
        self.confirmed = (options.filter is None)
        self.buf = []
        self.next_fout()
        self.total_entries = 0
        self.rejected = 0
        self.unknown = 0

    def close_fout(self):
        if self.fout is not None:
            self.fout.write(self.options.footer)
            self.fout.close()

    def next_fout(self):
        self.close_fout()
        self.entries = 0
        self.current += 1
        fn = self.options.pattern % self.current
        log('writing to %s', fn)
        self.fout = open(fn, 'w')
        self.fout.write(self.options.header)

    def write_entry(self):
        if self.confirmed:
            if self.entries >= self.options.entries:
                self.next_fout()
            for line in self.buf:
                self.fout.write(line)
        else:
            self.unknown += 1
        self.buf = []
        self.entries += 1
        self.total_entries += 1

    def read_line(self, line):
        if self.inentry:
            self.buf.append(line)
            if self.options.end.search(line):
                self.write_entry()
                self.inentry = False
            elif self.options.filter is not None:
                m = self.options.filter.search(line)
                if m is not None:
                    if m.group(1) in self.options.dictionary:
                        self.confirmed = True
                    else:
                        self.buf = []
                        self.confirmed = False
                        self.inentry = False
                        self.rejected += 1
        elif self.options.begin.search(line):
            self.buf.append(line)
            self.inentry = True
            self.confirmed = (self.options.filter is None)

    def read_file(self, f):
        for line in f:
            self.read_line(line)




class Split(OptionParser):
    def __init__(self):
        OptionParser.__init__(self, usage='Usage: %prog [OPTIONS] [FILE...]')
        self.add_option('-b', '--begin', action='store', type='string', dest='begin', help='pattern to match the first line of an entry')
        self.add_option('-e', '--end', action='store', type='string', dest='end', help='pattern to match the last line of an entry')
        self.add_option('-H', '--header', action='store', type='string', dest='header', default='', help='content to add at the beginning of each file')
        self.add_option('-F', '--footer', action='store', type='string', dest='footer', default='', help='content to add at the end of each file')
        self.add_option('-n', '--entries', action='store', type='int', dest='entries', default=1000, help='number of entries in each file')
        self.add_option('-p', '--pattern', action='store', type='string', dest='pattern', default='split_%06d', help='pattern for output files')
        self.add_option('-f', '--filter', action='store', type='string', dest='filter', default=None, help='filter by identifier')
        self.add_option('-d', '--dictionary', action='store', type='string', dest='dictionary', default=None, help='identifier dictionary')

    def run(self):
        options, args = self.parse_args()
        if options.begin is None:
            raise Exception('missing --begin')
        if options.end is None:
            raise Exception('missing --end')
        options.begin = re.compile(options.begin)
        options.end = re.compile(options.end)
        if options.filter is not None:
            if options.dictionary is None:
                raise Exception('missing --dictionary')
            options.filter = re.compile(options.filter)
            f = open(options.dictionary)
            options.dictionary = set(line.strip() for line in f)
            f.close()
        elif options.dictionary is not None:
            raise Exception('missing --dictionary')   
        splitter = Splitter(options)
        if len(args) == 0:
            splitter.read_file(stdin)
        else:
            for fn in args:
                f = open(fn)
                splitter.read_file(f)
                f.close()
        splitter.close_fout()
        log('files: %d', splitter.current)
        log('entries: %d', splitter.total_entries)
        if options.filter is not None:
            log('rejected: %d', splitter.rejected)
            log('unknown: %d', splitter.unknown)

if __name__ == '__main__':
    Split().run()
