#!/usr/bin/env python

from sys import argv
from optparse import OptionParser

class Filter(OptionParser):
    def __init__(self):
        OptionParser.__init__(self, usage='Usage: %prog [OPTIONS] [FILE...]')
        self.add_option('-f', '--file', action='append', type='string', dest='files', help='pattern list file')
        self.add_option('-k', '--column', action='store', type='int', dest='column', default=0, help='column to match')

    def run(self):
        options, args = self.parse_args()
        patterns = set()
        for fn in options.files:
            f = open(fn)
            for line in f:
                line = line.strip()
                patterns.add(line)
            f.close()
        for fn in args:
            f = open(fn)
            for line in f:
                line = line.strip()
                cols = line.split('\t')
                if cols[options.column] in patterns:
                    print line
            f.close()


if __name__ == '__main__':
    Filter().run();
