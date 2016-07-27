# decorator shennanigans to bless class methods such that they can be referenced
# as an iterable property or called with parameters (also returns an iterable)
class CallableIterable(object):
    def __init__(self, func, target):
        self.func = func
        self.target = target

    def __iter__(self):
        return self.func(self.target)

    def __call__(self, *args, **kwargs):
        return self.func(self.target, *args, **kwargs)

class CallableIterableMethod(object):
    def __init__(self, fget=None):
        self.fget = fget

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        try:
            fn = obj.__CallableIterableMethod_cache[self.fget.__name__]
        except KeyError:
            fn = obj.__CallableIterableMethod_cache[self.fget.__name__] = CallableIterable(self.fget, obj)
        except AttributeError:
            obj.__CallableIterableMethod_cache = {}
            fn = obj.__CallableIterableMethod_cache[self.fget.__name__] = CallableIterable(self.fget, obj)
        return fn


