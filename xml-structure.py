#!/bin/env python

import xml.etree.ElementTree as ET
import sys
import collections
import argparse


class Card:
    NONE = 0
    ONE = 1
    OPTIONAL = 2
    ZERO_MANY = 3
    MANY = 4

    def __init__(self):
        raise RuntimeError()

    @staticmethod
    def merge(card1, card2):
        card1, card2 = sorted([card1, card2])
        if card1 == Card.NONE:
            if card2 == Card.NONE:
                return Card.NONE
            if card2 == Card.ONE:
                return Card.OPTIONAL
            if card2 == Card.OPTIONAL:
                return Card.OPTIONAL
            if card2 == Card.ZERO_MANY:
                return Card.ZERO_MANY
            if card2 == Card.MANY:
                return Card.ZERO_MANY
        if card1 == Card.ONE:
            return card2
        if card2 == Card.OPTIONAL:
            if card2 == Card.MANY:
                return Card.ZERO_MANY
            return card2
        if card1 == Card.ZERO_MANY:
            return Card.ZERO_MANY
        return Card.MANY


class LightNode:
    def __init__(self, source, tag, children=None, cardinality=Card.ONE, values=None):
        self.sources = set([source])
        self.tag = tag
        if values is None:
            self.values = set()
        elif type(values) == str:
            self.values = set([values])
        else:
            self.values = set(values)
        self.cardinality = cardinality
        if children is None:
            self.children = []
        else:
            self.children = list(children)
        self.flattened = False

    def __str__(self):
        return 'LightNode(%s, %s, %d children)' % (self._tag, self._pp_more(), len(self._children))

    def get_child(self, tag):
        for child in self.children:
            if child.tag == tag:
                return child
        return None

    def lookup(self, path):
        if type(path) == str:
            yield from self.lookup(path.split('.'))
        elif len(path) == 0:
            yield self
        else:
            tag = path[0]
            tail = path[1:]
            if tag == '*':
                for child in self.children:
                    yield from child.lookup(tail)
            elif tag == '@*':
                for child in self.children:
                    if child.tag.startswith('@'):
                        yield from child.lookup(tail)
            elif tag == '**':
                for child in self.children:
                    yield from child.lookup(path)
                    yield from child.lookup(tail)
            else:
                child = self.get_child(tag)
                if child is not None:
                    yield from child.lookup(tail)

    def merge_node(self, other, card=None):
        if other is None:
            if card is None:
                self.cardinality = Card.merge(self.cardinality, Card.NONE)
            else:
                self.cardinality = card
            return
        if self.tag != other.tag:
            raise RuntimeError()
        for child in self.children:
            child.merge_node(other.get_child(child.tag))
        for child in other.children:
            if self.get_child(child.tag) is None:
                child.merge_node(None)
                self.children.append(child)
        if card is None:
            self.cardinality = Card.merge(self.cardinality, other.cardinality)
        else:
            self.cardinality = card
        self.values |= other.values
        self.sources |= other.sources

    def squelch_children(self):
        child_map = collections.defaultdict(list)
        for child in self.children:
            child.squelch_children()
            child_map[child.tag].append(child)
        new_children = []
        for tag, children in child_map.items():
            merged = children.pop()
            for child in children:
                merged.merge_node(child, Card.MANY)
            new_children.append(merged)
        self.children = new_children

    def _descendents(self, depth=0):
        if len(self.values) == 0:
            yield depth, self
            depth += 1
            for child in self.children:
                yield from child._descendents(depth)

    def flatten(self):
        descmap = collections.defaultdict(list)
        for depth, desc in self._descendents():
            descmap[desc.tag].append((depth, desc))
        for nodelist in descmap.values():
            nodelist.sort(key=lambda y: y[0])
            _, head = nodelist.pop(0)
            for _, node in nodelist:
                head.merge_node(node, head.cardinality)
                node.flattened = True


class XMLStructure(argparse.ArgumentParser):
    URL_VALUE = '<<URL>>'
    TEXT_VALUE = '<<TEXT>>'

    def __init__(self):
        argparse.ArgumentParser.__init__(self, description='examines the structure of XML files')
        self.add_argument('files', metavar='FILES', type=str, nargs='+', default=[], help='XML files')
        self.add_argument('--no-squelch', action='store_true', default=False, dest='no_squelch', help='do not squelch multiple children with the same tag')
        self.add_argument('--flatten', metavar='PATH', type=str, action='append', default=[], dest='flatten', help='flatten descendents of the specified nodes')
        self.add_argument('--indent', metavar='STR', type=str, action='store', default='|--', dest='indent_step', help='indentation (%(default)s)')
        self.add_argument('--max-values', metavar='N', type=int, action='store', default=6, dest='max_values', help='maximum number of values to displpay (%(default)s)')
        self.add_argument('--max-sources', metavar='N', type=int, action='store', default=3, dest='max_sources', help='maximum number of sources to displpay (%(default)s)')
        self.add_argument('--max-value-size', metavar='N', type=int, action='store', default=20, dest='max_value_size', help='maximum size in character of displayed values (%(default)s)')
        self.add_argument('--all-values', metavar='PATH', type=str, action='append', default=[], dest='all_values', help='display all values of specified nodes')

    def run(self):
        self.args = self.parse_args()
        tree = self._parse_all()
        self._flatten(tree)
        self._all_values_nodes(tree)
        self._pp(sys.stdout, tree)

    def _all_values_nodes(self, tree):
        self.args.all_values_nodes = set()
        for path in self.args.all_values:
            self.args.all_values_nodes |= set(tree.lookup(path))

    def _parse_all(self):
        result = None
        for filename in self.args.files:
            light = self._parse(filename)
            if result is None:
                result = light
            else:
                result.merge_node(light)
        return result

    def _parse(self, filename):
        with open(filename) as f:
            tree = ET.parse(f)
        root = tree.getroot()
        light = self._node_from_ET(filename, root)
        if not self.args.no_squelch:
            light.squelch_children()
        return light

    def _node_from_ET(self, source, element):
        tag = self._get_ET_tag(element)
        children = list(self._attribute(source, k, v) for k, v in element.attrib.items())
        txt = element.text
        if txt is not None and txt.strip() != '':
            children.append(self._text(source, txt))
        children.extend(self._node_from_ET(source, child) for child in element)
        return LightNode(source, tag, children)

    def _get_ET_tag(self, element):
        nsi = element.tag.index('}')
        if nsi >= 0:
            return element.tag[(nsi + 1):]
        return element.tag

    def _attribute(self, source, key, value):
        return LightNode(source, '@' + key, values=self._value(value))

    def _text(self, source, value):
        return LightNode(source, '#text', values=self._value(value))

    def _value(self, value):
        value = value.strip().replace('\n', ' ')
        if value.startswith('https://') or value.startswith('http://'):
            return XMLStructure.URL_VALUE
        if self.args.max_value_size >= 0 and len(value) >= self.args.max_value_size:
            return XMLStructure.TEXT_VALUE
        return value

    def _flatten(self, tree):
        for path in self.args.flatten:
            nodes = list(tree.lookup(path))
            if len(nodes) == 0:
                sys.stderr.write('could not find ' + path + '\n')
            else:
                for node in nodes:
                    node.flatten()

    def _pp(self, f, node, indent=''):
        if node.cardinality == Card.NONE:
            return
        f.write('%s%s%s%s%s%s\n' % (indent, node.tag, self._pp_cardinality(node), self._pp_values(node), self._pp_flattened(node), self._pp_sources(node)))
        if not node.flattened:
            indent = indent + self.args.indent_step
            for child in node.children:
                self._pp(f, child, indent)

    def _pp_cardinality(self, node):
        if node.cardinality == Card.ONE:
            return ''
        elif node.cardinality == Card.OPTIONAL:
            return ' [?]'
        elif node.cardinality == Card.ZERO_MANY:
            return ' [*]'
        elif node.cardinality == Card.MANY:
            return ' [+]'
        else:
            raise RuntimeError()

    def _pp_list(self, lst, max_n, left, right):
        if max_n == 0:
            return ''
        if max_n >= 1 and len(lst) > max_n:
            return ' ' + left + ', '.join(('\'%s\'' % e) for e in list(lst)[:max_n]) + ', ...' + right
        return ' ' + left + ', '.join(('\'%s\'' % e) for e in lst) + right

    def _pp_values(self, node):
        if len(node.values) == 0:
            return ''
        if node in self.args.all_values_nodes:
            return self._pp_list(node.values, sys.maxsize, '(', ')')
        if XMLStructure.TEXT_VALUE in node.values:
            return ''
        return self._pp_list(node.values, self.args.max_values, '(', ')')

    def _pp_flattened(self, node):
        if node.flattened:
            return ' <>'
        return ''

    def _pp_sources(self, node):
        if node.cardinality == Card.ONE:
            return ''
        return self._pp_list(node.sources, self.args.max_sources, '{', '}')


if __name__ == '__main__':
    XMLStructure().run()
