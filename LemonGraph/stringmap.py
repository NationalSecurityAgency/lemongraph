native_unicode = type(u'') is type('')

def StringMap(words):
    idx = {}

    class StringMap(tuple):
        def __getitem__(self, x):
            # map string/uint to valid string
            y = idx[x]
            return x if isinstance(y, int) else y

        def __call__(self, x):
            # map string/uint to valid uint
            y = idx[x]
            return y if isinstance(y, int) else x

        def __repr__(self):
            return 'StringMap(%s)' % super(StringMap, self).__repr__()

    words = StringMap(words)

    for i, s in enumerate(words):
        # bolt on read-only property per word
        def val(self, ii=i):
            return ii
        setattr(StringMap, s, property(val))

        # add string <=> id mappings
        idx[s] = idx[s.encode() if native_unicode else s.decode()] = i
        idx[i] = s

    return words
