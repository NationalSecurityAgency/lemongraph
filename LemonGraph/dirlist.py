from . import ffi, lib, wire


def dirlist(path):
    try:
        dirp = lib.opendir(wire.encode(path))
        while True:
            name = lib._readdir(dirp)
            if name == ffi.NULL:
                break
            yield wire.decode(ffi.string(name))
    finally:
        if dirp is not None:
            lib.closedir(dirp)
            dirp = None
