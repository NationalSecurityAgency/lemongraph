import sys

from . import Graph

try:
    db = sys.argv[1]
except:
    print >>sys.stderr, "Usage: python -mLemonGraph.snapshot path/to/src.db > dst.db"
    sys.exit(1)

with Graph(db, create=False) as g:
    for block in g.snapshot():
        sys.stdout.write(block)
sys.stdout.close()
