import logging
import os
import sys
import threading

from . import ffi, lib, wire

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

# updated via hooks in LemonGraph.server.__main__
pid = 0

# master proc actually emits logs
# add filter to overwrite pid with child syncd process pid
class SyncdFilter(logging.Filter):
    def filter(self, record):
        record.process = pid
        return True

log.addFilter(SyncdFilter())

class Syncd(object):
    def __init__(self, dir, threads=32):
        # supplied path must not end with dot or slash
        if dir[-1] in '/.':
            raise ValueError()
        self.closed = False
        self.halted = False
        self.dir = wire.encode(dir)
        self.threads = threads
        # sync pipe
        self.sfds = os.pipe()

    def queue(self, uuid):
        return lib.afsync_queue(self.sfds[1], wire.encode(uuid))

    def unqueue(self, uuid):
        return lib.afsync_unqueue(self.sfds[1], wire.encode(uuid))

    def close(self):
        if not self.closed:
            self.shutdown()
            self.closed = True

    def shutdown(self):
        if not self.halted:
            os.close(self.sfds[1]);
            os.close(self.sfds[0]);
            self.halted = True

    # fork and run this
    def receiver(self):
        name = b'syncd'
        lib.lg_log_init(log.level, ffi.from_buffer(name))
        # close write end of sync pipe
        os.close(self.sfds[1])
        try:
            return lib.afsync_receiver(self.dir, self.threads, self.sfds[0])
        finally:
            os.close(self.sfds[0])
