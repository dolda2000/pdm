"""Python Daemon Management -- Client functions

This module implements the client part of the PDM protocols. The
primary objects of interest are the replclient and perfclient classes,
which implement support for their respective protocols. See their
documentation for details.
"""

import socket, pickle, struct, select, threading

__all__ = ["client", "replclient", "perfclient"]

class protoerr(Exception):
    """Raised on protocol errors"""
    pass

def resolve(spec):
    if isinstance(spec, socket.socket):
        return spec
    sk = None
    try:
        if "/" in spec:
            sk = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sk.connect(spec)
        elif spec.isdigit():
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.connect(("localhost", int(spec)))
        elif ":" in spec:
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p = spec.rindex(":")
            sk.connect((spec[:p], int(spec[p + 1:])))
        else:
            raise Exception("Unknown target specification %r" % spec)
        rv = sk
        sk = None
    finally:
        if sk is not None: sk.close()
    return rv

class client(object):
    """PDM client

    This class provides general facilities to speak to PDM servers,
    and is mainly intended to be subclassed to provide for the
    specific protocols, such as replclient and perfclient do.

    `client' instances can be passed as arguments to select.select(),
    and can be used in `with' statements.
    """
    def __init__(self, sk, proto = None):
        """Create a client object connected to the specified
        server. `sk' can either be a socket object, which is used as
        it is, or a string specification very similar to the
        specification for L{pdm.srv.listen}, so see its documentation
        for details. The differences are only that this function does
        not take arguments specific to socket creation, like the mode
        and group arguments for Unix sockets. If `proto' is given,
        that subprotocol will negotiated with the server (by calling
        the select() method).
        """
        self.sk = resolve(sk)
        self.buf = ""
        line = self.readline()
        if line != "+PDM1":
            raise protoerr("Illegal protocol signature")
        if proto is not None:
            self.select(proto)

    def close(self):
        """Close this connection"""
        self.sk.close()

    def fileno(self):
        """Return the file descriptor of the underlying socket."""
        return self.sk.fileno()

    def readline(self):
        """Read a single NL-terminated line and return it."""
        while True:
            p = self.buf.find("\n")
            if p >= 0:
                ret = self.buf[:p]
                self.buf = self.buf[p + 1:]
                return ret
            ret = self.sk.recv(1024)
            if ret == "":
                return None
            self.buf += ret

    def select(self, proto):
        """Negotiate the given subprotocol with the server"""
        if "\n" in proto:
            raise Exception("Illegal protocol specified: %r" % proto)
        self.sk.send(proto + "\n")
        rep = self.readline()
        if len(rep) < 1 or rep[0] != "+":
            raise protoerr("Error reply when selecting protocol %s: %s" % (proto, rep[1:]))

    def __enter__(self):
        return self

    def __exit__(self, *excinfo):
        self.close()
        return False

class replclient(client):
    """REPL protocol client
    
    Implements the client side of the REPL protocol; see
    L{pdm.srv.repl} for details on the protocol and its functionality.
    """
    def __init__(self, sk):
        """Create a connected client as documented in the `client' class."""
        super(replclient, self).__init__(sk, "repl")

    def run(self, code):
        """Run a single block of Python code on the server. Returns
        the output of the command (as documented in L{pdm.srv.repl})
        as a string.
        """
        while True:
            ncode = code.replace("\n\n", "\n")
            if ncode == code: break
            code = ncode
        while len(code) > 0 and code[-1] == "\n":
            code = code[:-1]
        self.sk.send(code + "\n\n")
        buf = ""
        while True:
            ln = self.readline()
            if ln[0] == " ":
                buf += ln[1:] + "\n"
            elif ln[0] == "+":
                return buf
            elif ln[0] == "-":
                raise protoerr("Error reply: %s" % ln[1:])
            else:
                raise protoerr("Illegal reply: %s" % ln)

class perfproxy(object):
    def __init__(self, cl, id, proto):
        self.cl = cl
        self.id = id
        self.proto = proto
        self.subscribers = set()

    def lookup(self, name):
        self.cl.lock.acquire()
        try:
            id = self.cl.nextid
            self.cl.nextid += 1
        finally:
            self.cl.lock.release()
        (proto,) = self.cl.run("lookup", id, self.id, name)
        proxy = perfproxy(self.cl, id, proto)
        self.cl.proxies[id] = proxy
        return proxy

    def listdir(self):
        return self.cl.run("ls", self.id)[0]

    def readattr(self):
        return self.cl.run("readattr", self.id)[0]

    def attrinfo(self):
        return self.cl.run("attrinfo", self.id)[0]

    def invoke(self, method, *args, **kwargs):
        return self.cl.run("invoke", self.id, method, args, kwargs)[0]

    def subscribe(self, cb):
        if cb in self.subscribers:
            raise ValueError("Already subscribed")
        if len(self.subscribers) == 0:
            self.cl.run("subs", self.id)
        self.subscribers.add(cb)

    def unsubscribe(self, cb):
        if cb not in self.subscribers:
            raise ValueError("Not subscribed")
        self.subscribers.remove(cb)
        if len(self.subscribers) == 0:
            self.cl.run("unsubs", self.id)

    def notify(self, ev):
        for cb in self.subscribers:
            try:
                cb(ev)
            except: pass

    def close(self):
        if self.id is not None:
            self.cl.run("unbind", self.id)
            del self.cl.proxies[self.id]
            self.id = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *excinfo):
        self.close()
        return False

class perfclient(client):
    """PERF protocol client
    
    Implements the client side of the PERF protocol; see
    L{pdm.srv.perf} for details on the protocol and its functionality.

    This client class implements functions for finding PERF objects on
    the server, and returns, for each server-side object looked up, a
    proxy object that mimics exactly the PERF interfaces that the
    object implements. As the proxy objects reference live objects on
    the server, they should be released when they are no longer used;
    they implement a close() method for that purpose, and can also be
    used in `with' statements.

    See L{pdm.srv.perf} for details on the various PERF interfaces
    that the proxy objects might implement.
    """
    def __init__(self, sk):
        """Create a connected client as documented in the `client' class."""
        super(perfclient, self).__init__(sk, "perf")
        self.nextid = 0
        self.lock = threading.Lock()
        self.proxies = {}
        self.names = {}

    def send(self, ob):
        buf = pickle.dumps(ob)
        buf = struct.pack(">l", len(buf)) + buf
        self.sk.send(buf)

    def recvb(self, num):
        buf = ""
        while len(buf) < num:
            data = self.sk.recv(num - len(buf))
            if data == "":
                raise EOFError()
            buf += data
        return buf

    def recv(self):
        return pickle.loads(self.recvb(struct.unpack(">l", self.recvb(4))[0]))

    def event(self, id, ev):
        proxy = self.proxies.get(id)
        if proxy is None: return
        proxy.notify(ev)

    def dispatch(self, timeout = None):
        """Wait for an incoming notification from the server, and
        dispatch it to the callback functions that have been
        registered for it. If `timeout' is specified, wait no longer
        than so many seconds; otherwise, wait forever. This client
        object may also be used as argument to select.select().
        """
        rfd, wfd, efd = select.select([self.sk], [], [], timeout)
        if self.sk in rfd:
            msg = self.recv()
            if msg[0] == "*":
                self.event(msg[1], msg[2])
            else:
                raise ValueError("Unexpected non-event message: %r" % msg[0])

    def recvreply(self):
        while True:
            reply = self.recv()
            if reply[0] in ("+", "-"):
                return reply
            elif reply[0] == "*":
                self.event(reply[1], reply[2])
            else:
                raise ValueError("Illegal reply header: %r" % reply[0])

    def run(self, cmd, *args):
        self.lock.acquire()
        try:
            self.send((cmd,) + args)
            reply = self.recvreply()
            if reply[0] == "+":
                return reply[1:]
            else:
                raise reply[1]
        finally:
            self.lock.release()

    def lookup(self, module, obnm):
        """Look up a single server-side object by the given name in
        the given module. Will return a new proxy object for each
        call when called multiple times for the same name.
        """
        self.lock.acquire()
        try:
            id = self.nextid
            self.nextid += 1
        finally:
            self.lock.release()
        (proto,) = self.run("bind", id, module, obnm)
        proxy = perfproxy(self, id, proto)
        self.proxies[id] = proxy
        return proxy

    def find(self, name):
        """Convenience function for looking up server-side objects
        through PERF directories and for multiple uses. The object
        name can be given as "MODULE.OBJECT", which will look up the
        named OBJECT in the named MODULE, and can be followed by any
        number of slash-separated names, which will assume that the
        object to the left of the slash is a PERF directory, and will
        return the object in that directory by the name to the right
        of the slash. For instance, find("pdm.perf.sysres/cputime")
        will return the built-in attribute for reading the CPU time
        used by the server process.

        The proxy objects returned by this function are cached and the
        same object are returned the next time the same name is
        requested, which means that they are kept live until the
        client connection is closed.
        """
        ret = self.names.get(name)
        if ret is None:
            if "/" in name:
                p = name.rindex("/")
                ret = self.find(name[:p]).lookup(name[p + 1:])
            else:
                p = name.rindex(".")
                ret = self.lookup(name[:p], name[p + 1:])
            self.names[name] = ret
        return ret
