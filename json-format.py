#!/bin/env python

import sys
import json
import collections


j = json.load(sys.stdin)
props = collections.defaultdict(int)
for o in j:
    for k in o:
        props[k] += 1
sys.stderr.write('docs: %d\n' % len(j))
for k, n in props.items():
    sys.stderr.write('%s: %d\n' % (k, n))
json.dump(j, sys.stdout, indent=4)
