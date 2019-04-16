import sys

if sys.version_info[0] > 2:
    uni_types = str
    try:
        bin_types = (bytes, memoryview)
    except NameError:
        bin_types = bytes

    # for python 3, attempt decode to native unicode
    # but return binary if unicode decode failed
    def decode(data):
        try:
            data.decode()
        except UnicodeDecodeError:
            return data
        return data.decode()

    # encode native unicode string type to binary
    # silently pass binary straight through
    def encode(data):
        if isinstance(data, uni_types):
            return data.encode()
        elif isinstance(data, bin_types):
            return data
        raise TypeError('Unsupported type %s' % type(data))

else:
    uni_types = unicode # noqa
    try:
        bin_types = (str, memoryview)
    except NameError:
        bin_types = str

    # for python 2, attempt decode to unicode
    # but return native string if either
    #  * unicode decode failed, or
    #  * no non-ascii characters were found
    def decode(data):
        try:
            u = data.decode('UTF-8')
        except UnicodeDecodeError:
            return data
        return data if len(u) == len(data) else u

    # encode native unicode string type to binary
    # silently pass binary straight through
    def encode(data):
        if isinstance(data, bin_types):
            return data
        elif isinstance(data, uni_types):
            return data.encode('UTF-8')
        raise TypeError('Unsupported type %s' % type(data))
