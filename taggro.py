#!/usr/bin/env python

from sys import stdin, stdout, stderr
from collections import defaultdict, OrderedDict
from argparse import ArgumentParser


class Aggregator:
    def __init__(self, init_value):
        self.value = init_value

    def add_value(self, value):
        raise NotImplementedError()

    def get_value(self, sep):
        return self.value

    @staticmethod
    def numeric_options(args):
        number_type = int
        strict = False
        for a in args:
            if a == 'float' or a == 'f':
                number_type = float
            elif a == 'strict':
                strict = True
            else:
                raise ValueError('unknown aggregator option %s' % args[0])
        return (number_type, strict)

    @staticmethod
    def create(ctor, args):
        if ctor == 'ignore' or ctor == '-':
            if len(args) != 0:
                raise ValueError('unknown aggregator option %s' % args[0])
            return Ignore
        if ctor == 'first':
            if len(args) != 0:
                raise ValueError('unknown aggregator option %s' % args[0])
            return First
        if ctor == 'last':
            if len(args) != 0:
                raise ValueError('unknown aggregator option %s' % args[0])
            return Last
        if ctor == 'group':
            if len(args) != 0:
                raise ValueError('unknown aggregator option %s' % args[0])
            return Group
        if ctor == 'count':
            if len(args) != 0:
                raise ValueError('unknown aggregator option %s' % args[0])
            return Count
        if ctor == 'sum':
            number_type, strict = Aggregator.numeric_options(args)
            return Sum.Type(number_type, strict)
        if ctor == 'mean':
            number_type, strict = Aggregator.numeric_options(args)
            return Mean.Type(number_type, strict)
        if ctor == 'collect' or ctor == 'set':
            if len(args) != 0:
                raise ValueError('unknown aggregator option %s' % args[0])
            return Values
        raise ValueError('unknown aggregator %s' % ctor)

    @staticmethod
    def parse_token(token):
        args = token.split(':')
        ctor = args.pop(0)
        return Aggregator.create(ctor, args)


class Ignore(Aggregator):
    def __init__(self):
        Aggregator.__init__(self, None)

    def add_value(self, value):
        pass


class First(Aggregator):
    def __init__(self):
        Aggregator.__init__(self, None)

    def add_value(self, value):
        if self.value is None:
            self.value = value


class Last(Aggregator):
    def __init__(self):
        Aggregator.__init__(self, None)

    def add_value(self, value):
        self.value = value


class Group(First):
    def __init__(self):
        First.__init__(self)


class Count(Aggregator):
    def __init__(self):
        Aggregator.__init__(self, 0)

    def add_value(self, value):
        self.value += 1


class NumericAggregator(Aggregator):
    def __init__(self, number_type, strict=False):
        Aggregator.__init__(self, number_type(0))
        self.number_type = number_type
        self.strict = strict

    def add_value(self, value):
        try:
            self.add_number(self.number_type(value))
        except ValueError as e:
            if self.strict:
                raise e
            self.add_missing()

    def add_number(self, value):
        raise NotImplementedError()

    def add_missing(self):
        raise NotImplementedError()

    @classmethod
    def Type(cls, number_type, strict=False):
        return lambda: cls(number_type, strict)


class Sum(NumericAggregator):
    def __init__(self, number_type, strict=False):
        NumericAggregator.__init__(self, number_type, strict)

    def add_number(self, value):
        self.value += value

    def add_missing(self):
        pass


class Mean(NumericAggregator):
    def __init__(self, number_type, strict=False):
        NumericAggregator.__init__(self, number_type, strict)
        self.count = 0

    def add_number(self, value):
        self.value += value
        self.count += 1

    def add_missing(self):
        pass

    def get_value(self, sep):
        return float(self.value) / self.count


class Values(Aggregator):
    def __init__(self):
        Aggregator.__init__(self, OrderedDict())

    def add_value(self, value):
        self.value[value] = None

    def get_value(self, sep):
        return sep.join(str(x) for x in self.value)


class TAggro(ArgumentParser):
    def __init__(self):
        ArgumentParser.__init__(self, description='aggregate columns in a table')
        self.add_argument('aggregators', metavar='AGG', type=str, nargs='+', default=[], help='aggregators')
        self.add_argument('-i', '--input', metavar='FILE', type=str, nargs=1, action='append', dest='input', default=[], help='input file')
        self.add_argument('-s', '--separator', metavar='CHAR', type=str, action='store', dest='separator', default='\t', help='column separator character (default: tab)')
        self.add_argument('-l', '--list-separator', metavar='SEP', type=str, action='store', dest='list_separator', default=', ', help='list separator (default: comma)')

    def run(self):
        args = self.parse_args()
        self.aggregator_types = tuple(Aggregator.parse_token(a) for a in args.aggregators)
        self.group_indexes = tuple(i for (i, at) in enumerate(self.aggregator_types) if at is Group)
        self.result = defaultdict(lambda: tuple(at() for at in self.aggregator_types))
        if len(args.input) == 0:
            self.read_file(stdin, args.separator)
        else:
            for a in args.input:
                self.read_filename(a[0], args.separator)
        for cols in self.result.values():
            stdout.write(args.separator.join(str(a.get_value(args.list_separator)) for a in cols if not isinstance(a, Ignore)))
            stdout.write('\n')

    def read_line(self, cols):
        group = tuple(cols[i] for i in self.group_indexes)
        aggregators = self.result[group]
        for agg, val in zip(aggregators, cols):
            agg.add_value(val)

    def read_file(self, f, sep='\t'):
        stderr.write('separator: %s\n' % str(sep))
        for line in f:
            if line[-1] == '\n':
                line = line[:-1]
            cols = line.split(sep)
            self.read_line(cols)

    def read_filename(self, filename, sep='\t'):
        with open(filename) as f:
            self.read_file(f, sep)


if __name__ == '__main__':
    TAggro().run()
