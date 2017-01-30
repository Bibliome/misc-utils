#!/bin/env python

from sys import argv, stdout, stderr
from os.path import dirname, exists
from os import makedirs

class CommentParser:
    def __init__(self, exts):
        self.exts = set(exts)

    def parse(self, f):
        raise NotImplementedError()

    def write_comment(self, f, lines):
        raise NotImplementedError()


EMPTY = 0
PREAMBLE = 1
COMMENT = 2
CONTENT = 3


class NullCommentParser(CommentParser):
    def __init__(self):
        CommentParser.__init__(self, [])

    def parse(self, f):
        for n, line in enumerate(f):
            yield n, PREAMBLE, line

    def write_comment(self, f, lines):
        pass


class SingleLineCommentParser(CommentParser):
    def __init__(self, exts, open_comment):
        CommentParser.__init__(self, exts)
        self.open_comment = open_comment

    def _is_comment_line(self, line):
        return line.lstrip().startswith(self.open_comment)

    def _is_empty_line(self, line):
        return line.strip() == ''

    def parse(self, f):
        for n, line in enumerate(f):
            if self._is_empty_line(line):
                yield n, EMPTY, line
            elif self._is_comment_line(line):
                yield n, COMMENT, line
            else:
                yield n, CONTENT, line
    
    def write_comment(self, f, lines):
        for line in lines:
            f.write(self.open_comment)
            f.write(' ')
            f.write(line)


class ShellScriptCommentParser(CommentParser):
    def __init__(self, exts):
        CommentParser.__init__(self, exts)
        self.open_comment = '#'

    def _is_comment_line(self, line):
        return line.lstrip().startswith(self.open_comment)

    def _is_empty_line(self, line):
        return line.strip() == ''

    def _is_preamble_line(self, line):
        return line.startswith('#!')

    def parse(self, f):
        for n, line in enumerate(f):
            if self._is_preamble_line(line):
                yield n, PREAMBLE, line
            elif self._is_empty_line(line):
                yield n, EMPTY, line
            elif self._is_comment_line(line):
                yield n, COMMENT, line
            else:
                yield n, CONTENT, line
    
    def write_comment(self, f, lines):
        for line in lines:
            f.write(self.open_comment)
            f.write(' ')
            f.write(line)


class MultiLineCommentParser(CommentParser):
    def __init__(self, exts, open_comment, close_comment):
        CommentParser.__init__(self, exts)
        self.open_comment = open_comment
        self.close_comment = close_comment

    def _is_open_comment_line(self, line):
        return line.lstrip().startswith(self.open_comment)

    def _is_close_comment_line(self, line):
        return line.rstrip().endswith(self.close_comment)

    def _is_empty_line(self, line):
        return line.strip() == ''

    def parse(self, f):
        in_comment = False
        for n, line in enumerate(f):
            if in_comment or self._is_open_comment_line(line):
                yield n, COMMENT, line
                in_comment = not self._is_close_comment_line(line)
            elif self._is_empty_line(line):
                yield n, EMPTY, line
            else:
                yield n, CONTENT, line
    
    def write_comment(self, f, lines):
        f.write(self.open_comment)
        f.write('\n')
        for line in lines:
            f.write(line)
        f.write(self.close_comment)
        f.write('\n')


class XMLCommentParser(CommentParser):
    def __init__(self, exts):
        CommentParser.__init__(self, exts)
        self.open_comment = '<!--'
        self.close_comment = '-->'

    def _is_open_comment_line(self, line):
        return line.lstrip().startswith(self.open_comment)

    def _is_close_comment_line(self, line):
        return line.rstrip().endswith(self.close_comment)

    def _is_empty_line(self, line):
        return line.strip() == ''

    def _is_preamble_line(self, line):
        return line.lstrip().startswith('<?xml')

    def parse(self, f):
        in_comment = False
        for n, line in enumerate(f):
            if self._is_preamble_line(line):
                yield n, PREAMBLE, line
            elif in_comment or self._is_open_comment_line(line):
                yield n, COMMENT, line
                in_comment = not self._is_close_comment_line(line)
            elif self._is_empty_line(line):
                yield n, EMPTY, line
            else:
                yield n, CONTENT, line
    
    def write_comment(self, f, lines):
        f.write(self.open_comment)
        f.write('\n')
        for line in lines:
            f.write(line)
        f.write(self.close_comment)
        f.write('\n')


class HeaderParser:
    def __init__(self):
        pass

    def start_file(self, filename):
        raise NotImplementedError()

    def end_file(self):
        raise NotImplementedError()

    def parse(self, f, comment_parser):
        empty_lines = []
        header_start = False
        header_end = False
        for n, t, line in comment_parser.parse(f):
            if header_end:
                self.content(line)
                continue
            if t == PREAMBLE:
                self.content(line)
                continue
            if t == EMPTY:
                empty_lines.append(line)
                continue
            if t == CONTENT:
                for el in empty_lines:
                    self.content(el)
                empty_lines = []
                if not header_start:
                    self.start_header(comment_parser)
                header_end = True
                self.content(line)
                continue
            if t == COMMENT:
                if header_start:
                    for el in empty_lines:
                        self.header(n, el)
                else:
                    self.start_header(comment_parser)
                empty_lines = []
                header_start = True
                self.header(n, line)

        def content(self, line):
            raise NotImplementedError()

        def header(self, line):
            raise NotImplementedError()

        def start_header(self, comment_parser):
            raise NotImplementedError()


class ExtractHeader(HeaderParser):
    def __init__(self):
        HeaderParser.__init__(self)
        self.current_filename = None
    
    def start_file(self, filename):
        self.current_filename = filename

    def end_file(self):
        self.current_filename = None

    def content(self, line):
        pass

    def header(self, n, line):
        stdout.write('%s:%d: %s' % (self.current_filename, n, line))

    def start_header(self, comment_parser):
        pass


class ChangeHeader(HeaderParser):
    def __init__(self, target_dir):
        HeaderParser.__init__(self)
        self.target_dir = target_dir
        self.current_out = None

    def start_file(self, filename):
        out_filename = self.target_dir + '/' + filename
        out_dir = dirname(out_filename)
        if not exists(out_dir):
            makedirs(out_dir)
        self.current_out = open(out_filename, 'w+')
    
    def end_file(self):
        self.current_out.close()
        self.current_out = None

    def content(self, line):
        self.current_out.write(line)


class RemoveHeader(ChangeHeader):
    def __init__(self, target_dir):
        ChangeHeader.__init__(self, target_dir)

    def start_header(self, comment_parser):
        pass

    def header(self, n, line):
        pass


class AddHeader(ChangeHeader):
    def __init__(self, target_dir, header_lines):
        ChangeHeader.__init__(self, target_dir)
        self.header_lines = header_lines

    def start_header(self, comment_parser):
        comment_parser.write_comment(self.current_out, self.header_lines)
        self.current_out.write('\n')

    def header(self, n, line):
        self.current_out.write(line)


class ReplaceHeader(ChangeHeader):
    def __init__(self, target_dir, header_lines):
        ChangeHeader.__init__(self, target_dir)
        self.header_lines = header_lines

    def start_header(self, comment_parser):
        comment_parser.write_comment(self.current_out, self.header_lines)
        self.current_out.write('\n')

    def header(self, n, line):
        pass


COMMENT_PARSERS = (
    MultiLineCommentParser(('java', 'css', 'jj'), '/*', '*/'),
    SingleLineCommentParser(('properties',), '#'),
    ShellScriptCommentParser(('sh', 'py')),
    SingleLineCommentParser(('tex',), '%'),
    XMLCommentParser(('xml', 'xslt', 'dtd', 'html', 'plan')),
)


def get_comment_parser(filename):
    _, _, ext = filename.rpartition('.')
    for p in COMMENT_PARSERS:
        if ext in p.exts:
            return p
    stderr.write('Could not find a comment parser for ' + filename + '\n')
    return NullCommentParser()


def get_header_lines(filename):
    f = open(filename)
    result = list(f)
    f.close()
    return result



if __name__ == '__main__':
    exe = argv.pop(0)
    command = argv.pop(0)
    if command == 'help':
        print '%s help' % exe
        print '%s extract FILES' % exe
        print '%s remove OUTDIR FILES' % exe
        print '%s add OUTDIR HFILE FILES' % exe
        print '%s replace OUTDIR HFILE FILES' % exe
        exit(0)
    if command == 'extract':
        header_parser = ExtractHeader()
    elif command == 'remove':
        header_parser = RemoveHeader(argv.pop(0))
    elif command == 'add':
        header_parser = AddHeader(argv.pop(0), get_header_lines(argv.pop(0)))
    elif command == 'replace':
        header_parser = ReplaceHeader(argv.pop(0), get_header_lines(argv.pop(0)))
    else:
        raise RuntimeError('unknown command ' + command)
    for filename in argv:
        comment_parser = get_comment_parser(filename)
        f = open(filename)
        header_parser.start_file(filename)
        header_parser.parse(f, comment_parser)
        header_parser.end_file()
        f.close()
