from decimal import Decimal
from six import integer_types, string_types
float_types = (float, Decimal)
numeric_types = integer_types + float_types

def identity(x):
    return x

def number(x):
    try:
        return int(x)
    except ValueError:
        pass
    return float(x)

def _to_int():
    for t in integer_types:
        yield t, identity
    for t in string_types:
        yield t, int

def _to_num():
    for t in numeric_types:
        yield t, identity
    for t in string_types:
        yield t, number

to_int = dict(_to_int())
to_num = dict(_to_num())

def resolve(d, t, x):
    for k in d:
        if isinstance(x, k):
            f = d[t] = d[k]
            return f
    raise KeyError(t)

def uint(x):
    t = type(x)
    try:
        f = to_int[t]
    except KeyError:
        f = resolve(to_int, t, x)
    return f(x)

def unum(x):
    t = type(x)
    try:
        f = to_num[t]
    except KeyError:
        f = resolve(to_num, t, x)
    return f(x)

def boolean(x, false=set([None, False, 0, '0', 'n', 'f', 'no', 'false', ''])):
    if isinstance(x, str):
        x = x.lower()
    return x not in false
