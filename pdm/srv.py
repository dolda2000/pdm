"""Management for daemon processes

This module contains a utility to listen for management commands on a
socket, lending itself to managing daemon processes.
"""

import os, sys, socket, threading, grp, select
import types, pprint, traceback
import pickle, struct

__all__ = ["listener", "unixlistener", "tcplistener", "listen"]

protocols = {}

class repl(object):
    def __init__(self, cl):
        self.cl = cl
        self.mod = types.ModuleType("repl")
        self.mod.echo = self.echo
        self.printer = pprint.PrettyPrinter(indent = 4, depth = 6)
        cl.send("+REPL\n")

    def sendlines(self, text):
        for line in text.split("\n"):
            self.cl.send(" " + line + "\n")

    def echo(self, ob):
        self.sendlines(self.printer.pformat(ob))

    def command(self, cmd):
        try:
            try:
                ccode = compile(cmd, "PDM Input", "eval")
            except SyntaxError:
                ccode = compile(cmd, "PDM Input", "exec")
                exec ccode in self.mod.__dict__
                self.cl.send("+OK\n")
            else:
                self.echo(eval(ccode, self.mod.__dict__))
                self.cl.send("+OK\n")
        except:
            for line in traceback.format_exception(*sys.exc_info()):
                self.cl.send(" " + line)
            self.cl.send("+EXC\n")

    def handle(self, buf):
        p = buf.find("\n\n")
        if p < 0:
            return buf
        cmd = buf[:p + 1]
        self.command(cmd)
        return buf[p + 2:]
protocols["repl"] = repl

class perf(object):
    def __init__(self, cl):
        self.cl = cl
        self.odtab = {}
        cl.send("+PERF1\n")
        self.buf = ""
        self.lock = threading.Lock()
        self.subscribed = {}

    def closed(self):
        for id, recv in self.subscribed.iteritems():
            ob = self.odtab[id]
            if ob is None: continue
            ob, protos = ob
            try:
                ob.unsubscribe(recv)
            except: pass

    def send(self, *args):
        self.lock.acquire()
        try:
            buf = pickle.dumps(args)
            buf = struct.pack(">l", len(buf)) + buf
            self.cl.send(buf)
        finally:
            self.lock.release()

    def bindob(self, id, ob):
        if not hasattr(ob, "pdm_protocols"):
            raise ValueError("Object does not support PDM introspection")
        try:
            proto = ob.pdm_protocols()
        except Exception, exc:
            raise ValueError("PDM introspection failed", exc)
        self.odtab[id] = ob, proto
        return proto

    def bind(self, id, module, obnm):
        resmod = sys.modules.get(module)
        if resmod is None:
            self.send("-", ImportError("No such module: %s" % module))
            return
        try:
            ob = getattr(resmod, obnm)
        except AttributeError:
            self.send("-", AttributeError("No such object: %s" % obnm))
            return
        try:
            proto = self.bindob(id, ob)
        except Exception, exc:
            self.send("-", exc)
            return
        self.send("+", proto)

    def getob(self, id, proto):
        ob = self.odtab.get(id)
        if ob is None:
            self.send("-", ValueError("No such bound ID: %r" % id))
            return None
        ob, protos = ob
        if proto not in protos:
            self.send("-", ValueError("Object does not support that protocol"))
            return None
        return ob

    def lookup(self, tgtid, srcid, obnm):
        src = self.getob(srcid, "dir")
        if src is None:
            return
        try:
            ob = src.lookup(obnm)
        except KeyError, exc:
            self.send("-", exc)
            return
        try:
            proto = self.bindob(tgtid, ob)
        except Exception, exc:
            self.send("-", exc)
            return
        self.send("+", proto)

    def unbind(self, id):
        ob = self.odtab.get(id)
        if ob is None:
            self.send("-", KeyError("No such name bound: %r" % id))
            return
        ob, protos = ob
        del self.odtab[id]
        recv = self.subscribed.get(id)
        if recv is not None:
            ob.unsubscribe(recv)
            del self.subscribed[id]
        self.send("+")

    def listdir(self, id):
        ob = self.getob(id, "dir")
        if ob is None:
            return
        self.send("+", ob.listdir())

    def readattr(self, id):
        ob = self.getob(id, "attr")
        if ob is None:
            return
        try:
            ret = ob.readattr()
        except Exception, exc:
            self.send("-", Exception("Could not read attribute"))
            return
        self.send("+", ret)

    def attrinfo(self, id):
        ob = self.getob(id, "attr")
        if ob is None:
            return
        self.send("+", ob.attrinfo())

    def invoke(self, id, method, args, kwargs):
        ob = self.getob(id, "invoke")
        if ob is None:
            return
        try:
            self.send("+", ob.invoke(method, *args, **kwargs))
        except Exception, exc:
            self.send("-", exc)

    def event(self, id, ob, ev):
        self.send("*", id, ev)

    def subscribe(self, id):
        ob = self.getob(id, "event")
        if ob is None:
            return
        if id in self.subscribed:
            self.send("-", ValueError("Already subscribed"))
        def recv(ev):
            self.event(id, ob, ev)
        ob.subscribe(recv)
        self.subscribed[id] = recv
        self.send("+")

    def unsubscribe(self, id):
        ob = self.getob(id, "event")
        if ob is None:
            return
        recv = self.subscribed.get(id)
        if recv is None:
            self.send("-", ValueError("Not subscribed"))
        ob.unsubscribe(recv)
        del self.subscribed[id]
        self.send("+")

    def command(self, data):
        cmd = data[0]
        if cmd == "bind":
            self.bind(*data[1:])
        elif cmd == "unbind":
            self.unbind(*data[1:])
        elif cmd == "lookup":
            self.lookup(*data[1:])
        elif cmd == "ls":
            self.listdir(*data[1:])
        elif cmd == "readattr":
            self.readattr(*data[1:])
        elif cmd == "attrinfo":
            self.attrinfo(*data[1:])
        elif cmd == "invoke":
            self.invoke(*data[1:])
        elif cmd == "subs":
            self.subscribe(*data[1:])
        elif cmd == "unsubs":
            self.unsubscribe(*data[1:])
        else:
            self.send("-", Exception("Unknown command: %r" % (cmd,)))

    def handle(self, buf):
        if len(buf) < 4:
            return buf
        dlen = struct.unpack(">l", buf[:4])[0]
        if len(buf) < dlen + 4:
            return buf
        data = pickle.loads(buf[4:dlen + 4])
        self.command(data)
        return buf[dlen + 4:]
        
protocols["perf"] = perf

class client(threading.Thread):
    def __init__(self, sk):
        super(client, self).__init__(name = "Management client")
        self.setDaemon(True)
        self.sk = sk
        self.handler = self

    def send(self, data):
        return self.sk.send(data)

    def choose(self, proto):
        if proto in protocols:
            self.handler = protocols[proto](self)
        else:
            self.send("-ERR Unknown protocol: %s\n" % proto)
            raise Exception()

    def handle(self, buf):
        p = buf.find("\n")
        if p >= 0:
            proto = buf[:p]
            buf = buf[p + 1:]
            self.choose(proto)
        return buf

    def run(self):
        try:
            buf = ""
            self.send("+PDM1\n")
            while True:
                ret = self.sk.recv(1024)
                if ret == "":
                    return
                buf += ret
                while True:
                    try:
                        nbuf = self.handler.handle(buf)
                    except:
                        return
                    if nbuf == buf:
                        break
                    buf = nbuf
        finally:
            #for line in traceback.format_exception(*sys.exc_info()):
            #    print line
            try:
                self.sk.close()
            finally:
                if hasattr(self.handler, "closed"):
                    self.handler.closed()
            

class listener(threading.Thread):
    def __init__(self):
        super(listener, self).__init__(name = "Management listener")
        self.setDaemon(True)

    def listen(self, sk):
        self.running = True
        while self.running:
            rfd, wfd, efd = select.select([sk], [], [sk], 1)
            for fd in rfd:
                if fd == sk:
                    nsk, addr = sk.accept()
                    self.accept(nsk, addr)

    def stop(self):
        self.running = False
        self.join()

    def accept(self, sk, addr):
        cl = client(sk)
        cl.start()

class unixlistener(listener):
    def __init__(self, name, mode = 0600, group = None):
        super(unixlistener, self).__init__()
        self.name = name
        self.mode = mode
        self.group = group

    def run(self):
        sk = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        ul = False
        try:
            if os.path.exists(self.name) and os.path.stat.S_ISSOCK(os.stat(self.name).st_mode):
                os.unlink(self.name)
            sk.bind(self.name)
            ul = True
            os.chmod(self.name, self.mode)
            if self.group is not None:
                os.chown(self.name, os.getuid(), grp.getgrnam(self.group).gr_gid)
            sk.listen(16)
            self.listen(sk)
        finally:
            sk.close()
            if ul:
                os.unlink(self.name)

class tcplistener(listener):
    def __init__(self, port, bindaddr = "127.0.0.1"):
        super(tcplistener, self).__init__()
        self.port = port
        self.bindaddr = bindaddr

    def run(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sk.bind((self.bindaddr, self.port))
            sk.listen(16)
            self.listen(sk)
        finally:
            sk.close()

def listen(spec):
    if ":" in spec:
        first = spec[:spec.index(":")]
        last = spec[spec.rindex(":") + 1:]
    else:
        first = spec
        last = spec
    if "/" in first:
        parts = spec.split(":")
        mode = 0600
        group = None
        if len(parts) > 1:
            mode = int(parts[1], 8)
        if len(parts) > 2:
            group = parts[2]
        ret = unixlistener(parts[0], mode = mode, group = group)
        ret.start()
        return ret
    if last.isdigit():
        p = spec.rindex(":")
        host = spec[:p]
        port = int(spec[p + 1:])
        ret = tcplistener(port, bindaddr = host)
        ret.start()
        return ret
    raise ValueError("Unparsable listener specification: %r" % spec)

import pdm.perf
