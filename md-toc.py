#!/usr/bin/env python

from sys import argv, stdin, stdout
import re
from shutil import copy

def writeln(f, line):
    f.write(line)
    f.write('\n')

class Line:
    def __init__(self, value):
        self.value = value

    def echo(self, f):
        writeln(f, self.value)

    def strip(self, f):
        raise NotImplemented()

    def add(self, f):
        raise NotImplemented()

    def min_level(self, n):
        return n

    def toc(self, f, min_level):
        pass

class Plain(Line):
    def __init__(self, value):
        Line.__init__(self, value)

    def strip(self, f):
        writeln(f, self.value)

    def add(self, f):
        writeln(f, self.value)
        
class Header(Line):
    def __init__(self, value, level, title, toc_title, anchor_name, generate_anchor):
        Line.__init__(self, value)
        self.level = level
        self.title = title
        self.toc_title = toc_title
        self.anchor_name = anchor_name
        self.generate_anchor = generate_anchor

    def strip(self, f):
        writeln(f, self.value)

    def add(self, f):
        if self.generate_anchor:
            f.write('<a name="%s" />\n\n' % self.anchor_name)
        writeln(f, self.value)

    def min_level(self, n):
        if n is None or n > self.level:
            return self.level
        return n

    def toc(self, f, min_level):
        f.write('> %s* [%s](#%s)\n' % (('  ' * (self.level - min_level)), self.toc_title, self.anchor_name))

class Silent(Line):
    def __init__(self, value):
        Line.__init__(self, value)

    def strip(self, f):
        pass

    def add(self, f):
        pass
    
class Anchor(Line):
    def __init__(self, value, name):
        Line.__init__(self, value)
        self.name = name

    def strip(self, f):
        pass

    def add(self, f):
        writeln(f, self.value)
        
TOC_HEADER = '# Table of Contents'
TOC_END = '</toc>'
HEADER = re.compile('^(?P<level>[#]+)\s+(?P<title>.*)$')
ANCHOR = re.compile('^<a name="(?P<name>.*)" />$')


def read_file(f):
    in_toc = False
    anchor_name = None
    anchors = {}
    for line in f:
        line = line.rstrip()
        if in_toc:
            if line == TOC_END:
                in_toc = False
            yield Silent(line)
            continue
        if line.endswith(TOC_HEADER):
            in_toc = True
            yield Silent(line)
            continue
        m = ANCHOR.match(line)
        if m is not None:
            anchor_name = m.group('name')
            anchors[anchor_name] = None
            yield Anchor(line, anchor_name)
            continue
        m = HEADER.match(line)
        if m is not None:
            level = len(m.group('level'))
            title = m.group('title')
            toc_title = title.split('(')[0].strip()
            if anchor_name is None:
                anchor_name_root = toc_title.replace(' ', '-').replace('`', '')
                for n in xrange(1, 10):
                    anchor_name = '%s-%d' % (anchor_name_root, n)
                    if anchor_name not in anchors:
                        anchors[anchor_name] = None
                        break
                generate_anchor = True
            else:
                generate_anchor = False
            yield Header(line, level, title, toc_title, anchor_name, generate_anchor)
            anchor_name = None
            continue
        yield Plain(line)

def read_filename(fn):
    f = open(fn)
    for l in read_file(f):
        yield l


def echo(fin, fout):
    for l in read_file(fin):
        l.echo(fout)

def strip(fin, fout):
    for l in read_file(fin):
        l.strip(fout)

def add(fin, fout):
    lines = list(read_file(fin))
    min_level = None
    for l in lines:
        min_level = l.min_level(min_level)
    fout.write('%s%s\n\n' % ('#' * (min_level - 1), TOC_HEADER))
    for l in lines:
        l.toc(fout, min_level)
    writeln(fout, TOC_END)
    for l in lines:
        l.add(fout)

def dispatch(action, fin, fout):
    if action == 'echo':
        echo(fin, fout)
    elif action == 'strip':
        strip(fin, fout)
    elif action == 'toc':
        add(fin, fout)
    else:
        raise Exception('unknown action: %s' % action)

if len(argv) < 2:
    raise Exception('Usage: %s echo|strip|toc [files...]' % argv[0])

ACTION = argv[1]

if len(argv) == 2:
    dispatch(ACTION, stdin, stdout)
else:
    for fn in argv[2:]:
        fn2 = '.%s.save' % fn
        copy(fn, fn2)
        fin = open(fn2)
        fout = open(fn, 'w')
        dispatch(ACTION, fin, fout)
        fout.close()
        fin.close()

