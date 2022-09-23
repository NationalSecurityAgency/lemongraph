from __future__ import print_function
import re
from six import iteritems
import sys
import itertools

from collections import defaultdict, deque

SQ = r'''(?:'(?:[^'\\]|\\['"\\])*')'''
DQ = r'''(?:"(?:[^"\\]|\\['"\\])*")'''
BW = '(?:(?:(?![0-9])\w)\w*)'

WILD = re.compile(r'\*')
STR = re.compile('(?:%s|%s)' % (DQ, SQ), re.UNICODE)
WHITE = re.compile(r'\s+', re.UNICODE)
KEY = re.compile(r'(?:%s|%s|%s)' % (BW, SQ, DQ), re.IGNORECASE|re.UNICODE)
DOT = re.compile(r'(?:\.)', re.UNICODE)
NULL = re.compile(r'(?:None|null)', re.IGNORECASE|re.UNICODE)
TRUE = re.compile(r'(?:true)', re.IGNORECASE|re.UNICODE)
FALSE = re.compile(r'(?:false)', re.IGNORECASE|re.UNICODE)
TYPES = re.compile(r'(?:boolean|string|number|array|object)', re.UNICODE)
OCT = re.compile(r'(?:-?0[0-7]+)', re.UNICODE)
HEX = re.compile(r'(?:-?0x[0-9a-f]+)', re.IGNORECASE|re.UNICODE)
NUM = re.compile(r'(?:[0-9.e+-]+)', re.IGNORECASE|re.UNICODE)
#REGEX = re.compile(r'(?:/((?:[^\/]|\\.)*)/([ilmsxu]*))', re.UNICODE)
REGEX = re.compile(r'(?:/((?:[^/]|\\.)*)/([imsx]*))', re.UNICODE)
LIST_BEGIN = re.compile(r'\[', re.UNICODE)
LIST_END = re.compile(r'\]', re.UNICODE)
COMMA = re.compile(r',[\s,]*', re.UNICODE)
OBJ_BEGIN = re.compile(r'([@]*)\b([NE])(?::(%s(?:,%s)*?))?\(' % (BW, BW), re.IGNORECASE|re.UNICODE)
OBJ_END = re.compile(r'\)', re.UNICODE)
LINK_UNIQ = re.compile(r'(?:<?->?)', re.UNICODE)
CLEANER = re.compile(r'\\(.)', re.UNICODE)
END = re.compile(r'$', re.UNICODE)
OP = re.compile(r'(?:[<>]=?|!?[=~:])', re.UNICODE)
# aliases are positive integers or barewords
ALIAS = re.compile(r'((?:[1-9][0-9]*)|%s)\(' % BW, re.UNICODE)

RANGE = (STR, OCT, NUM, HEX)
OP_NEXT_BEGIN = {
    ':'  : (TYPES, LIST_BEGIN),
    '!:' : (TYPES, LIST_BEGIN),
    '='  : (STR, OCT, NUM, HEX, TRUE, FALSE, NULL, LIST_BEGIN, WILD),
    '!=' : (STR, OCT, NUM, HEX, TRUE, FALSE, NULL, LIST_BEGIN),
    '~'  : (REGEX, LIST_BEGIN),
    '!~' : (REGEX, LIST_BEGIN),
    '<'  : RANGE,
    '<=' : RANGE,
    '>'  : RANGE,
    '>=' : RANGE,
}

OP_NEXT_END = {
    ':'  : (TYPES, LIST_END),
    '!:' : (TYPES, LIST_END),
    '='  : (STR, OCT, NUM, HEX, TRUE, FALSE, NULL, LIST_END),
    '!=' : (STR, OCT, NUM, HEX, TRUE, FALSE, NULL, LIST_END),
    '~'  : (REGEX, LIST_END),
    '!~' : (REGEX, LIST_END),
}

OTHERTYPE = {
    'E': 'N',
    'N': 'E',
}

RE_FLAGS = {
    'i': re.IGNORECASE,
    'l': re.LOCALE,
    'm': re.MULTILINE,
    's': re.DOTALL,
    'x': re.VERBOSE,
    'u': re.UNICODE,
}

QUOTES = "\'\""
REVERSE = {
    'both' : 'both',
    'in'   : 'out',
    'out'  : 'in',
}

MERGE = {
    '=':  lambda a, b: a.intersection(b),
    '!=': lambda a, b: a.union(b),
    '~':  lambda a, b: a.intersection(b),
    '!~': lambda a, b: a.union(b),
    ':':  lambda a, b: a.intersection(b),
    '!:': lambda a, b: a.union(b),
}

RANGE_OP = {
    '>': lambda a, b: a > b,
    '<': lambda a, b: a < b,
    '>=': lambda a, b: a >= b,
    '<=': lambda a, b: a <= b,
}

# sigh - bools are a subclass of int
def is_type(val, types):
    if isinstance(val, bool) and bool not in types:
        return False
    return isinstance(val, tuple(types))

FILTER_OPS = ('!=', ':', '!:', '~', '!~')
FILTER = {
    '!=' : lambda d1, d2: set(v for v in d1 if v not in d2),
    ':'  : lambda d1, d2: set(v for v in d1 if is_type(v, d2)),
    '!:' : lambda d1, d2: set(v for v in d1 if not is_type(v, d2)),
    '~'  : lambda d1, d2: set(v for v in d1 if _match_at_least_one(v, d2)),
    '!~' : lambda d1, d2: set(v for v in d1 if not _match_at_least_one(v, d2)),
}

# tests are (key-list, op, vals)
TEST_EVAL = {
    '='      : lambda val, vals: val in vals,
    '!='     : lambda val, vals: val not in vals,
    '~'      : lambda val, vals: _match_at_least_one(val, vals),
    '!~'     : lambda val, vals: not _match_at_least_one(val, vals),

    # resolving the key already succeeded - that's all we need
    'exists' : lambda val, vals: True,
    'any'    : lambda val, vals: True,

    # range operators are guaranteed to have exactly one value
    '>'      : lambda val, vals: val >  vals[0],
    '>='     : lambda val, vals: val >= vals[0],
    '<'      : lambda val, vals: val <  vals[0],
    '<='     : lambda val, vals: val <= vals[0],

    # type operators
    ':'      : lambda val, vals: is_type(val, vals),
    '!:'     : lambda val, vals: not is_type(val, vals),
}

def _clean_num(m, val, _):
    try:
        val = float(val)
        val = int(val)
    except ValueError:
        # we only have a problem if the first conversion failed
        if isinstance(val, str):
            raise ValueError(m.start())
    return val

def _clean_regex(m, val, cache):
    flags = re.UNICODE
    for f in m.group(2):
        flags |= RE_FLAGS[f]
    try:
        ret = cache[(m.group(1), flags)]
    except KeyError:
        ret = cache[(m.group(1), flags)] = re.compile(m.group(1), flags)
    return ret

TYPES_MAP = {
    'boolean': bool,
    'string': str,
    'number': (float, int),
    'array': list,
    'object': dict,
}

CLEAN_VAL = {
    STR: lambda m, val, _: CLEANER.sub(r'\1', val[1:-1]),
    OCT: lambda m, val, _: int(val, 8),
    HEX: lambda m, val, _: int(val, 16),
    NUM: _clean_num,
    TRUE: lambda m, val, _: True,
    FALSE: lambda m, val, _: False,
    NULL: lambda m, val, _: None,
    REGEX: _clean_regex,
    TYPES: lambda m, val, _: TYPES_MAP[val],
}

def _match_at_least_one(val, rgxs):
    try:
        for rgx in rgxs:
            if rgx.search(val):
                return True
    except TypeError:
        pass
    return False

class QueryCannotMatch(Exception):
    def __init__(self, query):
        self.query = query

    def __str__(self):
        return 'Query cannot match: %s' % self.query

    def __repr__(self):
        return 'QueryCannotMatch(%s)' % repr(self.query)

class QuerySyntaxError(Exception):
    def __init__(self, query, pos, message):
        self.query = query
        self.pos = int(pos)
        self.message = message

    def __str__(self):
        return 'Query syntax error - %s at index %d: %s' % (self.message, self.pos, self.query)

    def __repr__(self):
        return 'QuerySyntaxError(%s, %s, %s)' % tuple(
            repr(arg) for arg in (self.query, self.pos, self.message))


class MatchLGQL(object):
    suppress_node_fields = ()
    suppress_edge_fields = ()

    def __init__(self, filter, cache=None):
        self.filter = filter
        self.pos = 0
        self.depth = 0
        self.end = len(filter)
        self.cache = {} if cache is None else cache
        self.matches = deque()
        self.best = None
        self.toc = {}
        self.required_filters = set()
        self.n = 0

        # parse a node/edge
        info = self.parse_obj()
        self.matches.append(info)

        while self.pos <= self.end:
            m, reg = self.token(LINK_UNIQ, COMMA, END)
            if reg is END:
                return self.finalize()
            elif reg is COMMA:
                return self.trailer()

            arrow = m.group(0)
            if len(arrow) == 2:
                link, rlink = ('out','in') if arrow[-1] == '>' else ('in', 'out')
            else:
                link = rlink = dir = 'both'

            info['next'] = link

            # parse another node/edge
            info = self.parse_obj()
            info['prev'] = rlink

            if info['type'] == self.matches[-1]['type']:
                inferred = {
                    'type': OTHERTYPE[info['type']],
                    'tests' : [],
                    'next': link,
                    'prev': rlink,
                    'keep': False,
                    'uniq': self.matches[-1]['uniq'] or info['uniq'],
                    'accel': {},
                }
                inferred['rank'] = 6 if inferred['type'] == 'N' else 7

                self.matches.append(inferred)
            self.matches.append(info)
        raise self.syntax_error("query not closed properly")

    def syntax_error(self, message, pos=None):
        if pos is None:
            pos = self.pos
        raise QuerySyntaxError(self.filter, pos, message)

    def finalize(self):
        if self.required_filters:
            raise self.syntax_error('missing required additional filter[s]: %s' % ', '.join(self.required_filters))

        self.matches[0]['prev'] = self.matches[-1]['next'] = ()
        self.matches = tuple(self.munge_obj(info) for info in self.matches)

        min = None
        for i, test in enumerate(self.matches):
            if min is None or test['rank'] < min['rank'] or (test['rank'] == min['rank'] and test['rank2'] < min['rank2']):
                min = test
                self.best = i

        self.keep = tuple(i for i, m in enumerate(self.matches) if m['keep'])

        return

    @staticmethod
    def normalize_alias(alias):
        try:
            return int(alias)
        except ValueError:
            return alias.lower()

    def add_filter(self, alias):
        alias = str(alias)
        normalized = self.normalize_alias(alias)
        required = (normalized != alias)
        try:
            infos = self.toc[normalized]
        except KeyError:
            if required:
                raise self.syntax_error('missing required alias: %s' % normalized)
            # minimum info obj
            infos = ({ 'tests': deque() },)
        self.required_filters.discard(normalized)
        pos = self.pos
        for info in infos:
            self.pos = pos
            self.parse_guts(info)

    def trailer(self):
        m, reg = self.token(ALIAS, END)
        if reg is END:
            return self.finalize()

        self.add_filter(m.group(1))

        while self.pos <= self.end:
            m, reg = self.token(COMMA, END)
            if reg is END:
                return self.finalize()

            m, reg = self.token(ALIAS, END)
            if reg is END:
                return self.finalize()

            self.add_filter(m.group(1))

        raise self.syntax_error('ran off end')

    def token(self, *regs):
        # always eat leading whitespace
        m = WHITE.match(self.filter, self.pos)
        if m is not None:
            self.pos = m.end()

        # use the first match
        for reg in regs:
            m = reg.match(self.filter, self.pos)
            if m is not None:
                self.pos = m.end()
                return m, reg

        raise self.syntax_error('unexpected sequence')

    def parse_list(self, op):
        lst = set()
        next = OP_NEXT_END[op]
        while self.pos < self.end:
            m, reg = self.token(*next)
            if reg is LIST_END:
                return tuple(sorted(lst))
            try:
                lst.add(CLEAN_VAL[reg](m, m.group(0), self.cache))
            except ValueError as e:
                raise self.syntax_error("bad value", pos=e.message)
            m, reg = self.token(COMMA, LIST_END)
            if reg is LIST_END:
                return tuple(sorted(lst))

        raise self.syntax_error('ran off end')

    def parse_obj(self):
        m, _ = self.token(OBJ_BEGIN)
        info = {
            'tests': deque(),
            'type': m.group(2).upper(),
            'keep': '@' not in m.group(1),
            'uniq': m.group(2) in 'ne',
        }
        if m.group(3) is not None:
            for alias in m.group(3).split(','):
                normalized = self.normalize_alias(alias)
                try:
                    self.toc[normalized].append(info)
                except KeyError:
                    self.toc[normalized] = [info]
                if alias != normalized:
                    self.required_filters.add(normalized)
        self.n += 1
        self.toc[self.n] = [info]
        return self.parse_guts(info)

    def parse_guts(self, info):
        matches = info['tests']
        while self.pos < self.end:
            m, reg = self.token(KEY, OBJ_END)
            if reg is OBJ_END:
                return info

            keys = []
            key = m.group(0)
            if key[0] in QUOTES:
                key = CLEANER.sub(r'\1', key[1:-1])
            keys.append(key)

            while True:
                m, reg = self.token(DOT, COMMA, OP, OBJ_END)
                if DOT != reg:
                    break
                m, reg = self.token(KEY)
                key = m.group(0)
                if key[0] in QUOTES:
                    key = CLEANER.sub(r'\1', key[1:-1])
                keys.append(key)

            key = tuple(keys)

            if reg in (COMMA, OBJ_END):
                matches.append((key, 'exists'))
                if reg is OBJ_END:
                    return info
                continue

            # else it is an op
            op = m.group(0)

            # value is required now
            m, reg = self.token(*OP_NEXT_BEGIN[op])

            if reg is LIST_BEGIN:
                val = self.parse_list(op)
            else:
                try:
                    val = (CLEAN_VAL[reg](m, m.group(0), self.cache),)
                except ValueError as e:
                    raise self.syntax_error("bad value", pos=e.message)
                except KeyError:
                    if reg is not WILD:
                        raise
            if reg is WILD:
                matches.append((key, 'any'))
            elif '=' == op:
                matches.appendleft((key, op, val))
            else:
                matches.append((key, op, val))
            _, reg = self.token(COMMA, OBJ_END)
            if reg is OBJ_END:
                return info

        raise self.syntax_error('ran off end')

    def munge_obj(self, info):
        d = {}
        keys = set()
        special = {}
        any    = special['any']    = set()
        exists = special['exists'] = set()
        tests_range = deque()
        for test in info['tests']:
            try:
                special[test[1]].add(test[0])
                continue
            except KeyError:
                pass

            keys.add(test[0])
            k = test[0:2]
            try:
                merge = MERGE[test[1]]
            except KeyError:
                tests_range.append(test)
                continue

            try:
                d[k] = merge(d[k], test[2])
            except KeyError:
                d[k] = set(test[2])

            if not d[k] and test[1] in ('=', '~', ':'):
                raise QueryCannotMatch(self.filter)

        ranges = deque()
        # filter '=' matches by range queries
        for test in tests_range:
            exists.discard(test[0])
            cmp = RANGE_OP[test[1]]
            key_op = (test[0], '=')
            try:
                d[key_op] = set(v for v in d[key_op] if cmp(v, test[2][0]))
                if not d[key_op]:
                    raise QueryCannotMatch(self.filter)
            except KeyError:
                ranges.append(test)

        # filter '=' matches by !=, :, !:, ~, and !~
        for key in keys:
            for i in range(1, len(keys)):
                exists.discard(key[0:i])
            exists.discard(key)
            key_op_eq = (key, '=')
            if key_op_eq not in d:
                continue

            for op in FILTER_OPS:
                key_op = (key, op)
                try:
                    d[key_op_eq] = FILTER[op](d[key_op_eq], d[key_op])
                    if not d[key_op_eq]:
                        raise QueryCannotMatch(self.filter)
                    del d[key_op]
                except KeyError:
                    pass

        tests_by_type = defaultdict(deque)
        tests_by_type['ranges'] = ranges
        tests_by_type['any'] = deque((key, 'any', ()) for key in any)
        tests_by_type['exists'] = deque((key, 'exists', ()) for key in exists)
        for key_op, vals in iteritems(d):
            tests_by_type[key_op[1]].append(key_op + (tuple(vals),))

        tests_new = deque()
        for types in ('any', 'exists', ':', '!:', '=', '!=', 'ranges', '~', '!~'):
            tests_new.extend(tests_by_type[types])

        # add type/value accelerator info
        # rank from best to worst (how specific, how many adjacent items, how many total)
        #   0: edge ID
        #   1: node ID
        #   2: node type/value
        #   3: edge type/value
        #   4: node type
        #   5: edge type
        #   6: node
        #   7: edge
        # assuming we have more edges than nodes
        accel = {}
        info['rank'] = 6 if info['type'] == 'N' else 7
        info['rank2'] = 0
        try:
            offset = 0 if info['type'] == 'N' else 1
            accel['type']  = tuple(d[(('type',), '=')])
            info['rank'] = 4 + offset
            info['rank2'] = len(accel['type'])
            # value is only useful if type is there
            accel['value'] = tuple(d[(('value',),'=')])
            info['rank'] = 2 + offset
            info['rank2'] *= len(accel['value'])
        except KeyError:
            pass

        # add id accelerator
        try:
            accel['ID']  = tuple(d[(('ID',), '=')])
            info['rank'] = 0 if info['type'] == 'E' else 1
            info['rank2'] = len(accel['ID'])
        except KeyError:
            pass

        # if there are no tests, add fudge trigger for new node/edge
        fudged = 0
        if len(tests_new) == 0:
            tests_new.append((('type',), 'exists', ()))
            fudged = 1

        info['tests'] = tuple(tests_new)
        info['accel'] = accel
        info['fudged'] = fudged

        return info

    def seeds(self, txn, beforeID=None):
        test = self.matches[self.best]
        rank = test['rank']
        accel = test['accel']
        funcs = (txn.nodes, txn.edges)
        if rank in (0,1):
            funcs = (txn.edge, txn.node)
            for ID in accel['ID']:
                try:
                    yield funcs[rank](ID=ID, beforeID=beforeID)
                except TypeError:
                    pass
        elif rank in (2,):
            for t,v in itertools.product(accel['type'], accel['value']):
                seed = txn.node(type=t, value=v, query=True, beforeID=beforeID)
                if seed:
                    yield seed
        elif rank in (3,):
            for t,v in itertools.product(accel['type'], accel['value']):
                for seed in txn.edges(type=t, value=v, beforeID=beforeID):
                    yield seed
        elif rank in (4,5):
            for t in accel['type']:
                for seed in funcs[rank % 2](type=t, beforeID=beforeID):
                    yield seed
        else:
            for seed in funcs[rank % 2](beforeID=beforeID):
                yield seed

    def dump(self, fh=sys.stdout):
        print('[', file=fh)
        for p in self.matches:
            pre = dict( (key, val) for key, val in iteritems(p) if key != 'tests' )
            if p['tests']:
                print('\t%s:[' % pre, file=fh)
                for test in p['tests']:
                    print("\t\t", test, ",", file=fh)
                print("\t],", file=fh)
            else:
                print('\t%s:[],' % pre, file=fh)
        print(']', file=fh)

    def is_valid(self, obj, idx=0, skip_fudged=False):
        match = self.matches[idx]
        n = match['fudged'] if skip_fudged else 0
        for test in match['tests'][n:]:
            if not eval_test(obj, test):
                return False
        return True
#        for key, op, vals in self.matches[idx]['tests']:
#            val = obj
#            try:
#                for k in key:
#                    val = val[k]
#            except Exception:
#                return False
#            if not TEST_EVAL[op](val, vals):
#                return False
#        return True

    @property
    def signature(self):
        return ''.join(m['type'] for m in self.matches if m['keep'])

    @property
    def reduce(self):
        arrows = {
            'in': '->',
            'out': '<-',
            'both': '-',
        }
        ret = []
        for m in self.matches:
            ret.append(arrows.get(m['prev'], None))
            fields = set()
            for t in m['tests'][m['fudged']:]:
                fields.add(t[0])
            fields.difference_update(self.suppress_node_fields if m['type'] == 'N' else self.suppress_edge_fields)
            ret.append({
                'type': m['type'].lower(),
                'select': m['keep'],
                'fields': sorted(fields),
            })
        return ret[1:]

def differ(a, b):
    t = type(a)
    if t != type(b):
        return True
    if t is list:
        if len(a) != len(b):
            return True
        for x, y in zip(sorted(a), sorted(b)):
            if differ(x, y):
                return True
    elif t is dict:
        if len(a) != len(b):
            return True
        for k,v in iteritems(a):
            if k not in b or differ(v, b[k]):
                return True
    else:
        return a != b
    return False

def eval_test(obj, test, prev=None):
    target = obj
    try:
        for k in test[0]:
            target = target[k]
    except:
        return False
    t = test[1]
    if not TEST_EVAL[t](target, test[2]):
        return False
    elif prev is None:
        return True
    try:
        prev = prev()
        for k in test[0]:
            prev = prev[k]
    except:
        return True
    return differ(target, prev) if t == 'any' else not TEST_EVAL[t](prev, test[2])

class MatchCTX(object):
    link = (None, 'next', 'prev')

    def __init__(self, match):
        self.len = len(match.matches)
        self.last = self.len - 1
        self.next = next
        self.match = match
        self.chain = deque()
        self.uniq = tuple(i for i,x in enumerate(match.matches) if x['uniq'])
        self.seen = deque()

    def push(self, target, delta):
        self.chain.append(target) if 1 == delta else self.chain.appendleft(target)

    def pop(self, delta):
        self.chain.pop() if 1 == delta else self.chain.popleft()

    def matches(self, target, idx=0, beforeID=None):
        self.chain.clear()
        self.seen.clear()

        if 0 == idx:
            deltas = (1,)
            stop = (self.last,)
        elif self.last == idx:
            deltas = (-1,)
            stop = (0,)
        else:
            left = idx + 1
            right = self.len - idx
            if left > right:
                deltas = (-1, 1)
                stop = (0, self.last)
            else:
                deltas = (1, -1)
                stop = (self.last, 0)
        if 2 == len(deltas):
            # get link/filter info for stage two up here, as it will not change
            link = self.match.matches[idx][self.link[deltas[1]]]
            # do stage one
            for _ in self._recurse(target, idx, deltas[0], stop[0], self.match.matches[idx]['uniq']):
                # do stage two
                for _ in self._next(target, link, idx, deltas[1], stop[1]):
                    if beforeID is None:
                        yield self.result(self.chain)
                    else:
                        snapped = tuple(x.clone(beforeID=beforeID) for x in self.chain)
                        if self.validate(snapped):
                            yield self.result(snapped)
        else:
            # we only have a stage one
            for _ in self._recurse(target, idx, deltas[0], stop[0], self.match.matches[idx]['uniq']):
                if beforeID is None:
                    yield self.result(self.chain)
                else:
                    snapped = tuple(x.clone(beforeID=beforeID) for x in self.chain)
                    if self.validate(snapped):
                        yield self.result(snapped)

    def validate(self, chain):
        for idx, target in enumerate(chain):
            if not self.match.is_valid(target, idx=idx):
                return False
        return True

    def result(self, chain):
        return tuple(chain[i] for i in self.match.keep)

    def _next(self, target, dir, idx, delta, stop):
        idx += delta
        if self.match.matches[idx]['uniq']:
            filter = self.seen
            do_seen = True
        else:
            do_seen = filter = None
        filter = self.seen if self.match.matches[idx]['uniq'] else None
        for t2 in target.iterlinks(filterIDs=filter, dir=dir):
            for _ in self._recurse(t2, idx, delta, stop, do_seen):
                yield

    def _recurse(self, target, idx, delta, stop, do_seen):
        if not self.match.is_valid(target, idx=idx):
            return
        self.push(target, delta)
        if do_seen:
            self.seen.append(target.ID)
        if idx == stop:
            yield
        else:
            link = self.match.matches[idx][self.link[delta]]
            for _ in self._next(target, link, idx, delta, stop):
                yield
        if do_seen:
            self.seen.pop()
        self.pop(delta)
