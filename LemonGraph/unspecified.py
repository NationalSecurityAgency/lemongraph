import sys

class unspecified(object):
    '''
    Token to allow named params be explicitly set to None:

    def foo(param=unspecified):
        if param is unspecified:
            do_something()
        else if param is None:
            do_something_else()

    Evaluates to False in boolean context.
    '''
    __slots__ = ()

    def __repr__(self):
        return '<unspecified>'

    if sys.version_info[0] < 3:
        def __nonzero__(self):
            return False
    else:
        def __bool__(self):
            return False

inst = unspecified()

assert bool(inst) is False
assert inst is not False

# stomp module name w/ instance
sys.modules[__name__] = inst
