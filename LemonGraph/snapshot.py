from __future__ import print_function
import sys

from . import Graph

try:
    db = sys.argv[1]
except:
    print("Usage: python -mLemonGraph.snapshot path/to/src.db > dst.db", file=sys.stderr)
    sys.exit(1)

# snapshot blocks are binary - on Python 3 sys.stdout is a text stream, so write to its underlying buffer
out = getattr(sys.stdout, 'buffer', sys.stdout)
with Graph(db, create=False) as g:
    for block in g.snapshot():
        out.write(block)
sys.stdout.close()
