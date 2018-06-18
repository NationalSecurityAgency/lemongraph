import itertools
from collections import deque, defaultdict
from itertools import chain

from . import Node, Edge
from .MatchLGQL import MatchLGQL, MatchCTX, QueryCannotMatch, eval_test

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
        except KeyError:
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
                    for code, reserved in jump.iteritems():
                        if key0 in reserved:
                            triggers[code][test].add(toc)
                            handled = True
                            break
                    if not handled:
                        triggers[target_type.lower()][key0][test].add(toc)

        # squish down to regular dicts and frozensets
        for code in triggers.iterkeys():
            if code in 'ne':
                triggers[code] = dict( (k0, dict( (test, frozenset(tocs)) for test, tocs in d.iteritems() )) for k0, d in triggers[code].iteritems() )
            else:
                triggers[code] = dict( (test, frozenset(tocs)) for test, tocs in triggers[code].iteritems() )
        triggers = dict( (k, v) for k, v in triggers.iteritems() if v )

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
                return chain.from_iterable(h)
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

    def _starts(self, txn, start, stop, scanner=None):
        handlers = self.handlers
        for entry in txn.scan(start=start, stop=stop):
            if scanner is not None:
                scanner(entry)
            try:
                for x in handlers[entry.code](entry):
                    yield x
            except KeyError:
                pass

    def _adhoc(self, txn, stop, limit):
        for p_idx, c in enumerate(self.compiled):
            if c is None:
                continue
            ctx = MatchCTX(c)
            p = self.patterns[p_idx]
            for seed in c.seeds(txn, beforeID=(stop+1) if stop else None):
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
            for p, ctx in ctxs.iteritems():
                valid = True
                for target, idx in zip(chain, ctx.match.keep):
                    if not ctx.match.is_valid(target, idx=idx, skip_fudged=True):
                        valid = False
                        break
                if valid:
                    yield p, chain

    def _streaming(self, txn, start, stop, limit, scanner):
        self._gen_handlers()
        ctxs = {}
        for target, tocs in self._starts(txn, start=start, stop=stop, scanner=scanner):
            for p_idx, idx in tocs:
                p = self.patterns[p_idx]
                try:
                    ctx = ctxs[p]
                except KeyError:
                    ctx = ctxs[p] = MatchCTX(self.compiled[p_idx])
                yield (p, ctx.matches(target, idx=idx))

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

    def execute(self, txn, start=0, stop=0, limit=0, scanner=None):
        limit = int(limit)
        pat_matches = self._streaming(txn, start, stop, limit, scanner) if start else self._adhoc(txn, stop, limit)
        return self._exec_limit(pat_matches, limit) if limit else self._exec(pat_matches)

    def _scan_static(self, trigs, entry, seen):
        for test, tocs in trigs.iteritems():
            if eval_test(entry, test):
                unseen = tocs - seen
                if unseen:
                    seen |= unseen
                    yield (entry, unseen)

    def _scan_mutable(self, trigs, entry, seen, beforeID):
        for test, tocs in trigs.iteritems():
            if eval_test(entry, test) and not eval_test(entry.clone(beforeID=beforeID), test):
                unseen = tocs - seen
                if unseen:
                    seen |= unseen
                    yield (entry, unseen)

    def _scan_prop(self, trigs, entry):
        try:
            triggers = trigs[entry.key]
        except KeyError:
            return
        for test, tocs in triggers.iteritems():
            if eval_test(entry.parent, test) and not eval_test(entry.parent.clone(beforeID=entry.ID), test):
                yield (entry.parent, tocs)
