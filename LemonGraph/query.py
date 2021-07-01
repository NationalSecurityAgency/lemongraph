from collections import deque, defaultdict
import itertools
from six import iteritems, iterkeys

from . import Node, Edge
from .MatchLGQL import MatchLGQL, MatchCTX, QueryCannotMatch, eval_test

def noop_scanner(entry):
    pass

class Once:
    __slots__ = 'func', 'ret'

    def __init__(self, func):
        self.func = func

    def __call__(self):
        if self.func is not None:
            self.ret = self.func()
            self.func = None
        return self.ret

class Query(object):
    magic = {
        'N': {
            'B': Node.reserved_both,
            'S': Node.reserved_src_only,
            'T': Node.reserved_tgt_only,
            'N': Node.reserved_internal,
        },
        'E': {
            'E': Edge.reserved,
        },
    }

    def __init__(self, patterns, cache=None):
        self.cache = {} if cache is None else cache
        self.patterns = patterns = tuple(patterns)
        self.handler_init = False
        self.handlers = None

        try:
            self.compiled = self.cache[('c',) + patterns]
            return
        except KeyError:
            pass
        compiled = deque()
        for p_idx, p in enumerate(patterns):
            try:
                c = self.cache[('p',p)]
            except KeyError:
                try:
                    c = self.cache[('p',p)] = MatchLGQL(p, cache=self.cache)
                except QueryCannotMatch:
                    c = self.cache[('p',p)] = None
            compiled.append(c)
        self.compiled = self.cache[('c',) + patterns] = tuple(compiled)


    def _gen_handlers(self):
        if self.handlers is not None:
            return

        try:
            self.handlers = self.cache[('h',) + self.patterns]
            return
        except KeyError:
            pass

        setdict = lambda: defaultdict(set)
        triggers = defaultdict(setdict)
        triggers['n'] = defaultdict(setdict)
        triggers['e'] = defaultdict(setdict)

        for p_idx, c in enumerate(self.compiled):
            if c is None:
                continue
            for idx, match in enumerate(c.matches):
                target_type = match['type'] # N or E
                jump = self.magic[target_type]
                toc = (p_idx, idx)
                for test in match['tests']:
                    key0 = test[0][0]
                    handled = False
                    for code, reserved in iteritems(jump):
                        if key0 in reserved:
                            triggers[code][test].add(toc)
                            handled = True
                            break
                    if not handled:
                        triggers[target_type.lower()][key0][test].add(toc)

        # squish down to regular dicts and frozensets
        for code in iterkeys(triggers):
            if code in 'ne':
                triggers[code] = dict( (k0, dict( (test, frozenset(tocs)) for test, tocs in iteritems(d) )) for k0, d in iteritems(triggers[code]) )
            else:
                triggers[code] = dict( (test, frozenset(tocs)) for test, tocs in iteritems(triggers[code]) )
        triggers = dict( (k, v) for k, v in iteritems(triggers) if v )

        edge_funcs = deque()
        if 'E' in triggers:
            E_trig = triggers['E']
            edge_funcs.append(lambda target, seen: self._scan_static(E_trig, target, seen))
        if 'B' in triggers:
            B_trig = triggers['B']
            edge_funcs.append(lambda target, seen: self._scan_mutable(B_trig, target.src, seen, target.ID))
            edge_funcs.append(lambda target, seen: self._scan_mutable(B_trig, target.tgt, seen, target.ID))
        if 'S' in triggers:
            S_trig = triggers['S']
            edge_funcs.append(lambda target, seen: self._scan_mutable(S_trig, target.src, seen, target.ID))
        if 'T' in triggers:
            T_trig = triggers['T']
            edge_funcs.append(lambda target, seen: self._scan_mutable(T_trig, target.tgt, seen, target.ID))

        handlers = {}

        if edge_funcs:
            edge_funcs = tuple(edge_funcs)
            def _edge_handler(target):
                seen = set()
                for func in edge_funcs:
                    for ret in func(target, seen):
                        yield ret
            def _edge_handler2(target):
                seen = set()
                h = deque()
                try:
                    h.append(self._scan_static(triggers['E'], target, set()))
                except KeyError:
                    pass
                try:
                    h.append(self._scan_mutable(triggers['B'], target.src, seen, target.ID))
                except KeyError:
                    pass
                try:
                    h.append(self._scan_mutable(triggers['B'], target.tgt, seen, target.ID))
                except KeyError:
                    pass
                try:
                    h.append(self._scan_mutable(triggers['S'], target.src, seen, target.ID))
                except KeyError:
                    pass
                try:
                    h.append(self._scan_mutable(triggers['T'], target.tgt, seen, target.ID))
                except KeyError:
                    pass
                return itertools.chain.from_iterable(h)
            handlers['E'] = _edge_handler

        if 'N' in triggers:
            N_trig = triggers['N']
            handlers['N'] = lambda target: self._scan_static(N_trig, target, set())
        if 'n' in triggers:
            n_trig = triggers['n']
            handlers['n'] = lambda target: self._scan_prop(n_trig, target)
        if 'e' in triggers:
            e_trig = triggers['e']
            handlers['e'] = lambda target: self._scan_prop(e_trig, target)

        self.handlers = self.cache[('h',) + self.patterns] = handlers

    def _adhoc(self, txn, stop=0, snap=False):
        for p_idx, c in enumerate(self.compiled):
            if c is None:
                continue
            ctx = MatchCTX(c)
            p = self.patterns[p_idx]
            if snap:
                beforeID = txn._snap(stop)
            else:
                beforeID = (stop + 1) if stop else None
            for seed in c.seeds(txn, beforeID=beforeID):
                yield (p, ctx.matches(seed, idx=c.best))

    def validate(self, *chains):
        ctxs_by_len = {}
        for p_idx, c in enumerate(self.compiled):
            if c is None:
                continue
            p = self.patterns[p_idx]
            try:
                ctxs_by_len[len(c.keep)][p] = MatchCTX(c)
            except KeyError:
                ctxs_by_len[len(c.keep)] = { p: MatchCTX(c) }

        for chain in chains:
            if not isinstance(chain, tuple):
                chain = tuple(chain)
            try:
                ctxs = ctxs_by_len[len(chain)]
            except KeyError:
                continue
            for p, ctx in iteritems(ctxs):
                valid = True
                for target, idx in zip(chain, ctx.match.keep):
                    if not ctx.match.is_valid(target, idx=idx, skip_fudged=True):
                        valid = False
                        break
                if valid:
                    yield p, chain

    def _starts(self, txn, scanner=noop_scanner, **kwargs):
        handlers = self.handlers
        for entry in txn.scan(**kwargs):
            if scanner(entry):
                return
            try:
                for target, tocs in handlers[entry.code](entry):
                    yield entry.ID, target, tocs
            except KeyError:
                pass

    def _update_seen(self, matches, seen, sk):
        for m in matches:
            seen.add(sk)
            yield m

    def _streaming(self, txn, snap=False, **kwargs):
        self._gen_handlers()
        ctxs = {}
        beforeID = 1 if snap else None
        seen = set() if snap else None
        for ID, target, tocs in self._starts(txn, **kwargs):
            if snap and ID >= beforeID:
                beforeID = txn._snap(ID)
                seen.clear()
            for p_idx, idx in tocs:
                if snap:
                    sk = target.ID, p_idx, idx
                    if sk in seen:
                        continue
                p = self.patterns[p_idx]
                try:
                    ctx = ctxs[p]
                except KeyError:
                    ctx = ctxs[p] = MatchCTX(self.compiled[p_idx])
                matches = ctx.matches(target, idx=idx, beforeID=beforeID)
                if snap:
                    matches = self._update_seen(matches, seen, sk)
                yield (p, matches)

    def _exec(self, pat_matches):
        for p, matches in pat_matches:
            for chain in matches:
                yield (p, chain)

    def _exec_limit(self, pat_matches, limit):
        for p, matches in pat_matches:
            for chain in matches:
                yield (p, chain)
                if 1 < limit:
                    limit -= 1
                else:
                    return

    def execute(self, txn, start=0, limit=0, **kwargs):
        limit = int(limit)
        pat_matches = self._streaming(txn, start=start, **kwargs) if start else self._adhoc(txn, **kwargs)
        return self._exec_limit(pat_matches, limit) if limit else self._exec(pat_matches)

    def _scan_static(self, trigs, entry, seen):
        for test, tocs in iteritems(trigs):
            if eval_test(entry, test):
                unseen = tocs - seen
                if unseen:
                    seen |= unseen
                    yield (entry, unseen)

    def _scan_mutable(self, trigs, entry, seen, beforeID):
        prev = Once(lambda: entry.clone(beforeID=beforeID))
        for test, tocs in iteritems(trigs):
            if eval_test(entry, test, prev=prev):
                unseen = tocs - seen
                if unseen:
                    seen |= unseen
                    yield (entry, unseen)

    def _scan_prop(self, trigs, entry):
        try:
            triggers = trigs[entry.key]
        except KeyError:
            return
        prev = Once(lambda: entry.parent.clone(beforeID=entry.ID))
        for test, tocs in iteritems(triggers):
            if eval_test(entry.parent, test, prev=prev):
                yield (entry.parent, tocs)
