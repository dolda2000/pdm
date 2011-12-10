"""Management for daemon processes

This module provides some client support for the daemon management
provided in the pdm.srv module.
"""

import socket, pickle, struct, select, threading

__all__ = ["client", "replclient"]

class protoerr(Exception):
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
    def __init__(self, sk, proto = None):
        self.sk = resolve(sk)
        self.buf = b""
        line = self.readline()
        if line != b"+PDM1":
            raise protoerr("Illegal protocol signature")
        if proto is not None:
            self.select(proto)

    def close(self):
        self.sk.close()

    def readline(self):
        while True:
            p = self.buf.find(b"\n")
            if p >= 0:
                ret = self.buf[:p]
                self.buf = self.buf[p + 1:]
                return ret
            ret = self.sk.recv(1024)
            if ret == b"":
                return None
            self.buf += ret

    def select(self, proto):
        if isinstance(proto, str):
            proto = proto.encode("ascii")
        if b"\n" in proto:
            raise Exception("Illegal protocol specified: %r" % proto)
        self.sk.send(proto + b"\n")
        rep = self.readline()
        if len(rep) < 1 or rep[0] != b"+"[0]:
            raise protoerr("Error reply when selecting protocol %s: %s" % (proto, rep[1:]))

    def __enter__(self):
        return self

    def __exit__(self, *excinfo):
        self.close()
        return False

class replclient(client):
    def __init__(self, sk):
        super(replclient, self).__init__(sk, "repl")

    def run(self, code):
        while True:
            ncode = code.replace("\n\n", "\n")
            if ncode == code: break
            code = ncode
        while len(code) > 0 and code[-1] == "\n":
            code = code[:-1]
        self.sk.send((code + "\n\n").encode("utf-8"))
        buf = b""
        while True:
            ln = self.readline()
            if ln[0] == b" "[0]:
                buf += ln[1:] + b"\n"
            elif ln[0] == b"+"[0]:
                return buf.decode("utf-8")
            elif ln[0] == b"-"[0]:
                raise protoerr("Error reply: %s" % ln[1:].decode("utf-8"))
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
        self.cl.run("unbind", self.id)
        del self.cl.proxies[self.id]

    def __enter__(self):
        return self

    def __exit__(self, *excinfo):
        self.close()
        return False

class perfclient(client):
    def __init__(self, sk):
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
        buf = b""
        while len(buf) < num:
            data = self.sk.recv(num - len(buf))
            if data == b"":
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
