from . import ffi, lib

def dirlist(path):
    try:
        dirp = lib.opendir(str(path))
        while True:
            name = lib._readdir(dirp)
            if name == ffi.NULL:
                break
            yield str(ffi.string(name))
    finally:
        if dirp is not None:
            lib.closedir(dirp)
            dirp = None
