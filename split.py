#!/usr/bin/env python

from sys import stdin, stderr
from optparse import OptionParser, OptionGroup
import re

class Splitter:
    def __init__(self, options):
        self.options = options
        self.entries = 0
        self.current = 0
        self.fout = None
        self.inentry = False
        self.next_fout()

    def close_fout(self):
        if self.fout is not None:
            self.fout.write(self.options.footer)
            self.fout.close()

    def next_fout(self):
        self.close_fout()
        self.entries = 0
        self.current += 1
        fn = self.options.pattern % self.current
        self.fout = open(fn, 'w')
        self.fout.write(self.options.header)

    def read_line(self, line):
        if self.inentry:
            self.fout.write(line)
            if re.search(self.options.end, line):
                self.inentry = False
        elif re.search(self.options.begin, line):
            if self.entries >= self.options.entries:
                self.next_fout()
            self.entries += 1
            self.fout.write(line)
            self.inentry = True
            
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

    def run(self):
        options, args = self.parse_args()
        if options.begin is None:
            raise Exception('missing --begin')
        if options.end is None:
            raise Exception('missing --end')
        splitter = Splitter(options)
        if len(args) == 0:
            splitter.read_file(stdin)
        else:
            for fn in args:
                f = open(fn)
                splitter.read_file(f)
                f.close()
        splitter.close_fout()

if __name__ == '__main__':
    Split().run()
