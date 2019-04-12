import logging
import os
import sys
import threading

from . import lib, wire

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

class Syncd(object):
    def __init__(self, dir, threads=32):
        # supplied path must not end with dot or slash
        if dir[-1] in '/.':
            raise ValueError()
        self.closed = False
        self.halted = False
        self.dir = wire.encode(dir)
        self.threads = threads
        # monitor pipe
        self.mfds = os.pipe()
        # sync pipe
        self.sfds = os.pipe()

    def queue(self, uuid):
        return lib.afsync_queue(self.sfds[1], wire.encode(uuid))

    def unqueue(self, uuid):
        return lib.afsync_unqueue(self.sfds[1], wire.encode(uuid))

    def close(self):
        if not self.closed:
            self.shutdown()
            os.close(self.mfds[0]);
            self.closed = True

    def shutdown(self):
        if not self.halted:
            os.close(self.mfds[1]);
            os.close(self.sfds[1]);
            os.close(self.sfds[0]);
            self.halted = True

    # fork and run this
    def receiver(self):
        # close read end of monitor pipe
        os.close(self.mfds[0])
        # close write end of sync pipe
        os.close(self.sfds[1])
        try:
            return lib.afsync_receiver(self.dir, self.threads, self.sfds[0], self.mfds[1])
        finally:
            os.close(self.sfds[0])
            os.close(self.mfds[1])

    def _monitor(self):
        rfd = self.mfds[0]
        try:
            while True:
                msg = os.read(rfd, 1024)
                if not len(msg):
                    break
                log.info(msg.decode())
        except OSError:
            pass
        finally:
            self.close()

    def monitor(self):
        t = threading.Thread(target=self._monitor)
        t.start()
        return t
