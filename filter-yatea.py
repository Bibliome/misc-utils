#!/usr/bin/python

# MIT License

# Copyright (c) 2017 Bibliome

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from sys import argv, stderr
from re import compile

YATEA = argv[2:]

tfn = argv[1]
f = open(tfn)
TERMS = set(t.strip() for t in f)
f.close()

start_term = compile('\s*<TERM_CANDIDATE')
form_decl = compile('\s*<FORM>(.+)</FORM>')
end_term = compile('\s*</TERM_CANDIDATE>')

def filter_file(fn):
    f = open(fn)
    buf = None
    has_form = False
    for l in f:
        if start_term.match(l):
            buf = l
            has_form = False
            continue
        if buf is None:
            continue
        buf += l
        if end_term.match(l):
            print buf.rstrip()
            continue
        if not has_form:
            m = form_decl.match(l)
            if m:
                has_form = True
                form = m.group(1).replace('&gt;', '>').replace('&lt;', '<').replace('&amp;', '&').replace('&apos;', '\'')
                if form in TERMS:
                    TERMS.remove(form)
                else:
                    buf = None
    f.close()


print '''<?xml version="1.0" encoding="UTF-8"?>
<TERM_EXTRACTION_RESULTS>
  <LIST_TERM_CANDIDATES>'''

for fn in YATEA:
    filter_file(fn)

print '''  </LIST_TERM_CANDIDATES>
</TERM_EXTRACTION_RESULTS>'''

for t in TERMS:
    stderr.write('missed: ' + t + '\n')

