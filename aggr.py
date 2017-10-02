#!/usr/bin/env python

from sys import maxint, stdin, stderr
from collections import defaultdict
from optparse import OptionParser, OptionGroup
from operator import setitem, add


class Source:
    '''Objects used to store the location of an information.

    :Members
    filename: name of the source file.
    lineno: line number where the information was read.
    '''
    def __init__(self, filename, lineno):
        self.filename = filename
        self.lineno = lineno

    def __str__(self):
        return str(self.filename) + ':' + str(self.lineno)


class AggregatorException(Exception):
    '''Aggregator failure.

    :Members
    source: location that caused the failure.
    msg: error message.
    *args, **kwargs
    '''
    def __init__(self, source, msg, *args, **kwargs):
        Exception.__init__(self, source, msg, *args, **kwargs)
        self.source = source
        self.msg = msg

class NumericAggregatorFunction:
    '''Function that aggregate numbers.

    :Members
    fun: function [ like + or * ].
    name: name of the function.
    help: help string.
    start: initial value.
    forgiving: either to forgive if a value cannot be converted into a number.
    '''
    side_effect = False

    def __init__(self, fun, name, help, start, forgiving):
        self.fun = fun
        self.name = name
        self.help = help
        self.start = start
        self.forgiving = forgiving
        
    def init(self):
        return self.start

    def aggregate(self, source, old, new):
        try:
            return self.fun(old, int(new))
        except ValueError:
            if self.forgiving:
                return old
            raise AggregatorException(source, 'not a number')

    def column(self, value, **kwargs):
        return str(value)

class CollectionAggregateFunction:
    '''Base class for function that aggregates values into a collection.'''
    side_effect = True

    @staticmethod
    def column(value, **kwargs):
        if kwargs['sort_values']:
            value = sorted(value)
        return kwargs['value_separator'].join(value)

class ListAggregateFunction(CollectionAggregateFunction):
    '''Function that collects values into a list (keeps duplicates, preserve order).'''
    name = 'list'
    help = 'store the values in a list'

    @staticmethod
    def init():
        return []

    @staticmethod
    def aggregate(old, new):
        old.append(new)

class SetAggregateFunction(CollectionAggregateFunction):
    '''Function that collects values into a set (no duplicates, do not preserve order).'''
    name = 'set'
    help = 'store the values in a set (no duplicates)'

    @staticmethod
    def init():
        return set()

    @staticmethod
    def aggregate(old, new):
        old.add(new)

class ComplementSetAggregateFunction:
    '''Function that collects missing values into a set.'''
    side_effect = True
    name = 'complement'
    help = 'store the values that are not found in the column'

    def __init__(self):
        self.all_values = set()

    def init(self):
        return set()

    def aggregate(self, old, new):
        old.add(new)
        self.all_values.add(new)

    def column(self, value, **kwargs):
        value = set(v for v in self.all_values if v not in value)
        if kwargs['sort_values']:
            value = sorted(value)
        return kwargs['value_separator'].join(value)

class CountAggregateFunction:
    '''Function that counts values.'''
    side_effect = True
    name = 'count'
    help = 'count the values'

    @staticmethod
    def init():
        return defaultdict(int)

    @staticmethod
    def aggregate(old, new):
        old[new] += 1

    @staticmethod
    def column(value, **kwargs):
        if kwargs['sort_values']:
            keys = sorted(value.iterkeys())
        else:
            keys = value.iterkeys()
        return kwargs['value_separator'].join(str(v) + kwargs['count_separator'] + str(value[v]) for v in keys)

class ComplementCountAggregateFunction:
    '''Function that counts missing values.'''
    side_effect = True
    name = 'complement-count'
    help = 'count the values and show zero for values that are not found in the column'

    def __init__(self):
        self.all_values = set()

    def init(self):
        return defaultdict(int)

    def aggregate(self, old, new):
        old[new] += 1
        self.all_values.add(new)

    def column(self, value, **kwargs):
        if kwargs['sort_values']:
            keys = sorted(self.all_values)
        else:
            keys = self.all_values
        return kwargs['value_separator'].join(str(v) + kwargs['count_separator'] + str(value[v]) for v in keys)


StandardAggregatorFunctions = {
    'sum': lambda: NumericAggregatorFunction(add, 'sum', 'sum the values, abort if a value is not a number', 0, False),
    'max': lambda: NumericAggregatorFunction(max, 'max', 'keep the highest value, abort if a value is not a number', 0, False),
    'min': lambda: NumericAggregatorFunction(min, 'min', 'keep the lowest value, abort if a value is not a number', maxint, False),
    'xsum': lambda: NumericAggregatorFunction(add, 'xsum', 'sum the values, ignore if a value is not a number', 0, True),
    'xmax': lambda: NumericAggregatorFunction(max, 'xmax', 'keep the highest value, ignore if a value is not a numbe', 0, True),
    'xmin': lambda: NumericAggregatorFunction(min, 'xmin', 'keep the lowest value, ignore if a value is not a number', maxint, True),
    'list': lambda: ListAggregateFunction,
    'set': lambda: SetAggregateFunction,
    'complement': lambda: ComplementSetAggregateFunction(),
    'count': lambda: CountAggregateFunction,
    'complement-count': lambda: ComplementCountAggregateFunction()
    }


class Aggregator:
    group_complement = object()
    aggregator_complement = object()

    def __init__(self, group_by=(), column_names=(), aggregator_functions=((group_complement,StandardAggregatorFunctions['sum']()),), separator='\t'):
        self.group_by = set(group_by)
        self.column_names = dict(column_names)
        self.aggregator_functions = dict(aggregator_functions)
        self.separator = separator
        self.line_size = 0

    def _iter_keys(self, line, keys):
        if isinstance(keys, int):
            yield keys
        elif isinstance(keys, str):
            yield self.column_names[keys]
        elif isinstance(keys, slice):
            for k in xrange(keys.start, keys.stop):
                yield k
        elif keys is Aggregator.group_complement:
            for k in self._iter_line_complement(line, self._iter_group_keys(line)):
                yield k
        elif keys is Aggregator.aggregator_complement:
            for k in self._iter_line_complement(line, self._iter_aggregator_keys(line)):
                yield k
        elif keys is Aggregator.aggregator_complement:
            aggregator_keys = set(self._iter_aggregator_keys(line))
            for k in xrange(len(line)):
                if k not in group_keys:
                    yield k            
        else:
            for key in keys:
                for k in self._iter_keys(line, key):
                    yield k

    def _iter_line_complement(self, line, it):
        keys = set(it)
        for k in xrange(len(line)):
            if k not in keys:
                yield k

    def _aggregate_line(self, source, aggregates, line):
        self.line_size = max(self.line_size, len(line))
        group_aggregate = self._get_group_aggregate(aggregates, line)
        for keys, aggfun in self.aggregator_functions.iteritems():
            for k in self._iter_keys(line, keys):
                value = line[k]
                old_value = group_aggregate[k]
                if aggfun.side_effect:
                    aggfun.aggregate(old_value, value)
                else:
                    group_aggregate[k] = aggfun.aggregate(source, old_value, value)

    def _get_group_aggregate(self, aggregates, line):
        group = tuple(self._get_group(line))
        if group not in aggregates:
            group_aggregate = {}
            for keys, aggfun in self.aggregator_functions.iteritems():
                for k in self._iter_keys(line, keys):
                    group_aggregate[k] = aggfun.init()
            aggregates[group] = group_aggregate
        return aggregates[group]

    def _iter_group_keys(self, line):
        for k in self._iter_keys(line, self.group_by):
            yield k

    def _iter_aggregator_keys(self, line):
        for k in self._iter_keys(line, self.aggregator_functions.iterkeys()):
            yield k

    def _get_group(self, line):
        for k in self._iter_group_keys(line):
            yield line[k]

    def _iter_values(self, keys, line):
        for k in self._iter_keys(line, keys):
            yield line[k]

    def aggregate_file(self, filename, f, first_line_as_column_names=False, aggregates=None):
        read_column_names = first_line_as_column_names
        if aggregates is None:
            aggregates = {}
        source = Source(filename, 0)
        for lineno, line in enumerate(f):
            source.lineno = lineno
            line = [c.strip() for c in line.split(self.separator)]
            if not line:
                continue
            if read_column_names:
                self.column_names = dict((name, index) for index, name in enumerate(line))
                read_column_names = False
                continue
            self._aggregate_line(source, aggregates, line)
        return aggregates



if __name__ == '__main__':
    aggregator = Aggregator()

    def _parse_key(key):
        keys = key.split(',')
        if len(keys) == 1:
            if key == '%':
                return Aggregator.group_complement
            begin, dash, end = key.partition('-')
            if dash:
                begin = int(begin)
                end = int(end) + 1
                return slice(begin,end)
            try:
                return int(key)
            except ValueError:
                return key.strip()
        return tuple(_parse_key(k) for k in keys)

    def _parse_callback_group(option, opt_str, value, parser):
        for key in value.split(','):
            aggregator.group_by.add(_parse_key(key))
    
    def _parse_callback_separator(option, opt_str, value, parser):
        aggregator.separator = value

    def _parse_callback_header_name(option, opt_str, value, parser):
        index, name = value
        aggregator.column_names[name] = int(index)

    def _parse_callback_aggregator_function(option, opt_str, value, parser, default_spec='sum'):
        if not hasattr(option, 'custom_functions'):
            option.custom_functions = True
            aggregator.aggregator_functions = {}
        try:
            key, spec = value
        except ValueError:
            key = value
            spec = default_spec
        aggfun = StandardAggregatorFunctions.get(spec)()
        aggregator.aggregator_functions[_parse_key(key)]

    parser = OptionParser(
        usage='usage: %prog [options] [files]',
        description='Aggregate a tabular file. Grouping, aggregated columns and aggregator functions may be specified (-g and --FUN options).',
        epilog='COLS: number (column number, starting at 0, -1 is last), string (column name, should be set with -n or -H), n-m (columns from n to m), % (not grouped/aggregated), a,b,... (columns a and columns b).'
        )

    aggregate_optgrp = OptionGroup(parser, 'Aggregation options')
    parser.add_option_group(aggregate_optgrp)
    aggregate_optgrp.add_option('-g',
                                '--group',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_group,
                                help='grouping columns specification (default: none)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('-f',
                                '--function',
                                action='callback',
                                nargs=2,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                help='set the aggregator function for a column specification (default: \'% sum\')',
                                metavar='COLS FUN'
                                )
    aggregate_optgrp.add_option('--sum',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('sum',),
                                help='sum the specified colums (equivalent to -f COLS sum)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--max',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('max',),
                                help='max of the specified colums (equivalent to -f COLS max)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--min',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('min',),
                                help='min of the specified colums (equivalent to -f COLS min)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--xsum',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('xsum',),
                                help='sum the specified colums (forgiving, equivalent to -f COLS xsum)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--xmax',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('xmax',),
                                help='max of the specified colums (forgiving, equivalent to -f COLS xmax)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--xmin',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('xmin',),
                                help='min of the specified colums (forgiving, equivalent to -f COLS xmin)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--list',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('list',),
                                help='list the specified colums (equivalent to -f COLS list)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--set',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('set',),
                                help='set the specified colums (equivalent to -f COLS set)',
                                metavar='COLS'
                                )
    aggregate_optgrp.add_option('--count',
                                action='callback',
                                nargs=1,
                                type='string',
                                callback=_parse_callback_aggregator_function,
                                callback_args=('count',),
                                help='count the specified colums (equivalent to -f COLS count)',
                                metavar='COLS'
                                )

    input_optgrp = OptionGroup(parser, 'Input options')
    parser.add_option_group(input_optgrp)
    input_optgrp.add_option(
        '-s',
        '--separator',
        action='callback',
        nargs=1, type='string',
        callback=_parse_callback_separator,
        help='input and output column separator (default: \'\\t\')',
        metavar='SEP'
        )
    input_optgrp.add_option(
        '-n',
        '--column-name',
        action='callback',
        nargs=2,
        type='string',
        callback=_parse_callback_header_name,
        help='set the name for a column (default: no name set)',
        metavar='INDEX NAME'
        )
    input_optgrp.add_option(
        '-H',
        '--header-line',
        action='store_true',
        dest='first_line_as_column_names',
        default=False,
        help='do not aggregate the first line, use columns as column names'
        )

    output_optgrp = OptionGroup(parser, 'Output options')
    parser.add_option_group(output_optgrp)
    output_optgrp.add_option(
        '-S',
        '--value-separator',
        action='store',
        type='string',
        dest='value_separator',
        default=',',
        help='separator between values for aggregator functions "list", "set" and "count" (default: \',\')',
        metavar='SEP'
        )
    output_optgrp.add_option(
        '-c',
        '--count-separator',
        action='store',
        type='string',
        dest='count_separator',
        default=':',
        help='separator between the value and the count for aggregator function "count" (default: \':\')'
        )
    output_optgrp.add_option(
        '-r',
        '--remove-empty',
        action='store_true',
        dest='remove_empty',
        default=False,
        help='remove empty columns from the output'
        )
    output_optgrp.add_option(
        '--sort',
        action='store_true',
        dest='sort',
        default=False,
        help='sort the output by group (default: do not sort)'
        )
    output_optgrp.add_option(
        '--sort-values',
        action='store_true',
        dest='sort_values',
        default=False,
        help='sort the values for aggregator functions "list", "set" and "count"'
        )

    options, args = parser.parse_args()
    
    aggregates = {}
    if args:
        for fn in args:
            f = open(fn)
            aggregator.aggregate_file(fn, f, options.first_line_as_column_names, aggregates)
            f.close()
    else:
        aggregator.aggregate_file('<<stdin>>', stdin, options.first_line_as_column_names, aggregates)

    dummy_line = [None]*aggregator.line_size
    group_keys = list(aggregator._iter_group_keys(dummy_line))
    for group, aggregate in aggregates.iteritems():
        line = [''] * aggregator.line_size
        for i, k in enumerate(group_keys):
            line[k] = group[i]
        for keys, aggfun in aggregator.aggregator_functions.iteritems():
            for k in aggregator._iter_keys(line, keys):
                line[k] = aggfun.column(aggregate[k], value_separator=options.value_separator, count_separator=options.count_separator)
        if options.remove_empty:
            print aggregator.separator.join(v for v in line if v != '')
        else:
            aggregator.aggregate_file('<stdin>', stdin, options.first_line_as_column_names, aggregates)
            
        dummy_line = [None]*aggregator.line_size
        group_keys = list(aggregator._iter_group_keys(dummy_line))
        groups = aggregates.keys()
        if options.sort:
            groups.sort()
        for group in groups:
            aggregate = aggregates[group]
            line = [''] * aggregator.line_size
            for i, k in enumerate(group_keys):
                line[k] = group[i]
            for keys, aggfun in aggregator.aggregator_functions.iteritems():
                for k in aggregator._iter_keys(line, keys):
                    line[k] = aggfun.column(aggregate[k],
                                            value_separator=options.value_separator.decode('string_escape'),
                                            count_separator=options.count_separator,
                                            sort_values=options.sort_values)
            if options.remove_empty:
                print aggregator.separator.join(v for v in line if v != '')
            else:
                print aggregator.separator.join(line)
