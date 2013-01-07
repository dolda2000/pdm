"""Python Daemon Management -- Miscellaneous utilities

This module contains various functions that may be useful to call from
the PDM REPL.
"""

import sys, traceback, threading

def threads():
    "Returns a dict of currently known threads, mapped to their respective frames."
    tt = dict((th.ident, th) for th in threading.enumerate())
    return dict((tt.get(key, key), val) for key, val in sys._current_frames().iteritems())

def traces():
    "Returns the value of threads() with each frame expanded to a backtrace."
    return dict((th, traceback.extract_stack(frame)) for th, frame in threads().iteritems())
