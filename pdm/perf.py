"""Python Daemon Management -- PERF utilities

This module serves two purposes: It has a few utility classes
for implementing PERF interfaces in common ways, and uses those
classes to implement some standard PERF objects that can be used by
PERF clients connecting to any PERF server.

See the documentation for L{pdm.srv.perf} for a description of the
various PERF interfaces.

It contains two named PERF objects:

 - sysres -- A directory containing the following objects pertaining
   to the resource usage of the server process:

    - realtime -- An attribute returning the amount of real time since
      the PDM module was imported (which likely coincides with the
      amount of time the server process has been running).

    - cputime -- An attribute returning the amount of CPU time
      consumed by the server process (in both user and kernel mode).

    - utime -- An attribute returning the amount of CPU time the
      server process has spent in user mode.

    - stime -- An attribute returning the amount of CPU time the
      server process has spent in kernel mode.

    - maxrss -- An attribute returning the largest resident set size
      the server process has used during its lifetime.

    - rusage -- An attribute returning the current rusage of the
      server process.

 - sysinfo -- A directory containing the following objects pertaining
   to the environment of the server process:

    - pid -- An attribute returning the PID of the server process.

    - uname -- An attribute returning the uname information of the
      system.

    - hostname -- An attribute returning the hostname of the system.

    - platform -- An attribute returning the Python build platform.
"""

import os, sys, time, socket, threading

__all__ = ["attrinfo", "simpleattr", "valueattr", "eventobj",
           "staticdir", "event", "procevent", "startevent",
           "finishevent"]

class attrinfo(object):
    """The return value of the `attrinfo' method on `attr' objects as
    described in L{pdm.srv.perf}.

    Currently contains a single data field, `desc', which should have
    a human-readable description of the purpose of the attribute.
    """
    def __init__(self, desc = None):
        self.desc = desc

class perfobj(object):
    def __init__(self, *args, **kwargs):
        super().__init__()
    
    def pdm_protocols(self):
        return []

class simpleattr(perfobj):
    """An implementation of the `attr' interface, which is initialized
    with a function, and returns whatever that function returns when
    read.
    """
    def __init__(self, func, info = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func
        if info is None:
            info = attrinfo()
        self.info = info

    def readattr(self):
        return self.func()

    def attrinfo(self):
        return self.info

    def pdm_protocols(self):
        return super().pdm_protocols() + ["attr"]

class valueattr(perfobj):
    """An implementation of the `attr' interface, which is initialized
    with a single value, and returns that value when read. Subsequent
    updates to the value are reflected in subsequent reads.
    """
    def __init__(self, init, info = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = init
        if info is None:
            info = attrinfo()
        self.info = info

    def readattr(self):
        return self.value

    def attrinfo(self):
        return self.info

    def pdm_protocols(self):
        return super().pdm_protocols() + ["attr"]

class eventobj(perfobj):
    """An implementation of the `event' interface. It keeps track of
    subscribers, and will multiplex any event to all current
    subscribers when submitted with the `notify' method.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subscribers = set()

    def subscribe(self, cb):
        if cb in self.subscribers:
            raise ValueError("Already subscribed")
        self.subscribers.add(cb)

    def unsubscribe(self, cb):
        self.subscribers.remove(cb)

    def notify(self, event):
        """Notify all subscribers with the given event object."""
        for cb in list(self.subscribers):
            try:
                cb(event)
            except: pass

    def pdm_protocols(self):
        return super().pdm_protocols() + ["event"]

class staticdir(perfobj):
    """An implementation of the `dir' interface. Put other PERF
    objects in it using the normal dict assignment syntax, and it will
    return them to requesting clients.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.map = {}

    def __setitem__(self, name, ob):
        self.map[name] = ob

    def __delitem__(self, name):
        del self.map[name]
        
    def __getitem__(self, name):
        return self.map[name]

    def get(self, name, default = None):
        return self.map.get(name, default)

    def listdir(self):
        return list(self.map.keys())

    def lookup(self, name):
        return self.map[name]

    def pdm_protocols(self):
        return super().pdm_protocols() + ["dir"]

class simplefunc(perfobj):
    """An implementation of the `invoke' interface. Put callables in
    it using the normal dict assignment syntax, and it will call them
    when invoked with the corresponding method name. Additionally, it
    updates itself with any keyword-arguments it is initialized with."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.map = {}
        self.map.update(kwargs)

    def __setitem__(self, name, func):
        self.map[name] = func

    def __delitem__(self, name):
        del self.map[name]

    def invoke(self, method, *args, **kwargs):
        if method not in self.map:
            raise AttributeError(method)
        self.map[method](*args, **kwargs)

    def pdm_protocols(self):
        return super().pdm_protocols() + ["invoke"]

class event(object):
    """This class should be subclassed by all event objects sent via
    the `event' interface. Its main utility is that it keeps track of
    the time it was created, so that listening clients can precisely
    measure the time between event notifications.

    Subclasses should make sure to call the __init__ method if they
    override it.
    """
    def __init__(self):
        self.time = time.time()

idlock = threading.Lock()
procevid = 0

def getprocid():
    global procevid
    idlock.acquire()
    try:
        ret = procevid
        procevid += 1
        return ret
    finally:
        idlock.release()

class procevent(event):
    """A subclass of the `event' class meant to group several events
    related to the same process. Create a new process by creating (a
    subclass of) the `startevent' class, and subsequent events in the
    same process by passing that startevent as the `id' parameter.

    It works by having `startevent' allocate a unique ID for each
    process, and every other procevent initializing from that
    startevent copying the ID. The data field `id' contains the ID so
    that it can be compared by clients.

    An example of such a process might be a client connection, where a
    `startevent' is emitted when a client connects, another subclass
    of `procevent' emitted when the client runs a command, and a
    `finishevent' emitted when the connection is closed.
    """
    def __init__(self, id):
        super().__init__()
        if isinstance(id, procevent):
            self.id = id.id
        else:
            self.id = id

class startevent(procevent):
    """A subclass of `procevent'. See its documentation for details."""
    def __init__(self):
        super().__init__(getprocid())

class finishevent(procevent):
    """A subclass of `procevent'. Intended to be emitted when a
    process finishes and terminates. The `aborted' field can be used
    to indicate whether the process completed successfully, if such a
    distinction is meaningful. The `start' parameter should be the
    `startevent' instance used when the process was initiated."""
    def __init__(self, start, aborted = False):
        super().__init__(start)
        self.aborted = aborted

sysres = staticdir()
itime = time.time()
sysres["realtime"] = simpleattr(func = lambda: time.time() - itime)
try:
    import resource
except ImportError:
    pass
else:
    ires = resource.getrusage(resource.RUSAGE_SELF)
    def ct():
        ru = resource.getrusage(resource.RUSAGE_SELF)
        return (ru.ru_utime - ires.ru_utime) + (ru.ru_stime - ires.ru_stime)
    sysres["cputime"] = simpleattr(func = ct)
    sysres["utime"] = simpleattr(func = lambda: resource.getrusage(resource.RUSAGE_SELF).ru_utime - ires.ru_utime)
    sysres["stime"] = simpleattr(func = lambda: resource.getrusage(resource.RUSAGE_SELF).ru_stime - ires.ru_stime)
    sysres["maxrss"] = simpleattr(func = lambda: resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    sysres["rusage"] = simpleattr(func = lambda: resource.getrusage(resource.RUSAGE_SELF))

sysinfo = staticdir()
sysinfo["pid"] = simpleattr(func = os.getpid)
sysinfo["uname"] = simpleattr(func = os.uname)
sysinfo["hostname"] = simpleattr(func = socket.gethostname)
sysinfo["platform"] = valueattr(init = sys.platform)

def reload(modname):
    mod = sys.modules.get(modname)
    if mod is None:
        raise ValueError(modname)
    import importlib
    importlib.reload(mod)

sysctl = simplefunc(exit=lambda status=0: os._exit(status),
                    reload=reload)
