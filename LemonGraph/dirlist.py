import os

from . import ffi, lib, wire

def dirlist(path):
    dirp = lib.opendir(wire.encode(path))
    if dirp == ffi.NULL:
        err = ffi.errno
        raise OSError(err, os.strerror(err), path)
    try:
        while True:
            name = lib._readdir(dirp)
            if name == ffi.NULL:
                break
            yield wire.decode(ffi.string(name))
    finally:
        lib.closedir(dirp)
