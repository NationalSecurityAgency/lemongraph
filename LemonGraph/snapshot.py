from __future__ import print_function
import sys

from . import Graph

try:
    db = sys.argv[1]
except:
    print("Usage: python -mLemonGraph.snapshot path/to/src.db > dst.db", file=sys.stderr)
    sys.exit(1)

with Graph(db, create=False) as g:
    for block in g.snapshot():
        sys.stdout.write(block)
sys.stdout.close()
