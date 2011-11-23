import os, sys, resource, time, socket, threading

class attrinfo(object):
    def __init__(self, desc = None):
        self.desc = desc

class perfobj(object):
    def __init__(self, *args, **kwargs):
        super(perfobj, self).__init__()
    
    def pdm_protocols(self):
        return []

class simpleattr(perfobj):
    def __init__(self, func, info = None, *args, **kwargs):
        super(simpleattr, self).__init__(*args, **kwargs)
        self.func = func
        if info is None:
            info = attrinfo()
        self.info = info

    def readattr(self):
        return self.func()

    def attrinfo(self):
        return self.info

    def pdm_protocols(self):
        return super(simpleattr, self).pdm_protocols() + ["attr"]

class valueattr(perfobj):
    def __init__(self, init, info = None, *args, **kwargs):
        super(valueattr, self).__init__(*args, **kwargs)
        self.value = init
        if info is None:
            info = attrinfo()
        self.info = info

    def readattr(self):
        return self.value

    def attrinfo(self):
        return self.info

    def pdm_protocols(self):
        return super(valueattr, self).pdm_protocols() + ["attr"]


class eventobj(perfobj):
    def __init__(self, *args, **kwargs):
        super(eventobj, self).__init__(*args, **kwargs)
        self.subscribers = set()

    def subscribe(self, cb):
        if cb in self.subscribers:
            raise ValueError("Already subscribed")
        self.subscribers.add(cb)

    def unsubscribe(self, cb):
        self.subscribers.remove(cb)

    def notify(self, event):
        for cb in self.subscribers:
            try:
                cb(event)
            except: pass

    def pdm_protocols(self):
        return super(eventobj, self).pdm_protocols() + ["event"]

class staticdir(perfobj):
    def __init__(self, *args, **kwargs):
        super(staticdir, self).__init__(*args, **kwargs)
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
        return self.map.keys()

    def lookup(self, name):
        return self.map[name]

    def pdm_protocols(self):
        return super(staticdir, self).pdm_protocols() + ["dir"]

class event(object):
    def __init__(self):
        self.time = time.time()

idlock = threading.Lock()
procevid = 0
class startevent(event):
    def __init__(self):
        super(startevent, self).__init__()
        global procevid
        idlock.acquire()
        try:
            self.id = procevid
            procevid += 1
        finally:
            idlock.release()

class finishevent(event):
    def __init__(self, start, aborted):
        super(finishevent, self).__init__()
        self.id = start.id
        self.aborted = aborted

sysres = staticdir()
itime = time.time()
ires = resource.getrusage(resource.RUSAGE_SELF)
def ct():
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return (ru.ru_utime - ires.ru_utime) + (ru.ru_stime - ires.ru_stime)
sysres["realtime"] = simpleattr(func = lambda: time.time() - itime)
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
