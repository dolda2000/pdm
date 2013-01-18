"""Python Daemon Management -- Server functions

This module implements the server part of the PDM protocols. The
primary object of interest herein is the listen() function, which is
the most generic way to create PDM listeners based on user
configuration, and the documentation for the repl and perf classes,
which describes the functioning of the REPL and PERF protocols.
"""

import os, sys, socket, threading, grp, select
import types, pprint, traceback
import pickle, struct

__all__ = ["repl", "perf", "listener", "unixlistener", "tcplistener", "listen"]

protocols = {}

class repl(object):
    """REPL protocol handler
    
    Provides a read-eval-print loop. The primary client-side interface
    is the L{pdm.cli.replclient} class. Clients can send arbitrary
    code, which is compiled and run on its own thread in the server
    process, and output responses that are echoed back to the client.

    Each client is provided with its own module, in which the code
    runs. The module is prepared with a function named `echo', which
    takes a single object and pretty-prints it as part of the command
    response. If a command can be parsed as an expression, the value
    it evaluates to is automatically echoed to the client. If the
    evalution of the command terminates with an exception, its
    traceback is echoed to the client.

    The REPL protocol is only intended for interactive usage. In order
    to interact programmatically with the server process, see the PERF
    protocol instead.
    """
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
    """PERF protocol handler
    
    The PERF protocol provides an interface for program interaction
    with the server process. It allows limited remote interactions
    with Python objects over a few defined interfaces.

    All objects that wish to be available for interaction need to
    implement a method named `pdm_protocols' which, when called with
    no arguments, should return a list of strings, each indicating a
    PERF interface that the object implements. For each such
    interface, the object must implement additional methods as
    described below.

    A client can find PERF objects to interact with either by
    specifying the name of such an object in an existing module, or by
    using the `dir' interface, described below. Thus, to make a PERF
    object available for clients, it needs only be bound to a global
    variable in a module and implement the `pdm_protocols'
    method. When requesting an object from a module, the module must
    already be imported. PDM will not import new modules for clients;
    rather, the daemon process needs to import all modules that
    clients should be able to interact with. PDM itself always imports
    the L{pdm.perf} module, which contains a few basic PERF
    objects. See its documentation for details.

    The following interfaces are currently known to PERF.

     - attr:
       An object that implements the `attr' interface models an
       attribute that can be read by clients. The attribute can be
       anything, as long as its representation can be
       pickled. Examples of attributes could be such things as the CPU
       time consumed by the server process, or the number of active
       connections to whatever clients the program serves. To
       implement the `attr' interface, an object must implement
       methods called `readattr' and `attrinfo'. `readattr' is called
       with no arguments to read the current value of the attribute,
       and `attrinfo' is called with no arguments to read a
       description of the attribute. Both should be
       idempotent. `readattr' can return any pickleable object, and
       `attrinfo' should return either None to indicate that it has no
       description, or an instance of the L{pdm.perf.attrinfo} class.

     - dir:
       The `dir' interface models a directory of other PERF
       objects. An object implementing it must implement methods
       called `lookup' and `listdir'. `lookup' is called with a single
       string argument that names an object, and should either return
       another PERF object based on the name, or raise KeyError if it
       does not recognize the name. `listdir' is called with no
       arguments, and should return a list of known names that can be
       used as argument to `lookup', but the list is not required to
       be exhaustive and may also be empty.

     - invoke:
       The `invoke' interface allows a more arbitrary form of method
       calls to objects implementing it. Such objects must implement a
       method called `invoke', which is called with one positional
       argument naming a method to be called (which it is free to
       interpret however it wishes), and with any additional
       positional and keyword arguments that the client wishes to pass
       to it. Whatever `invoke' returns is pickled and sent back to
       the client. In case the method name is not recognized, `invoke'
       should raise an AttributeError.

     - event:
       The `event' interface allows PERF objects to notify clients of
       events asynchronously. Objects implementing it must implement
       methods called `subscribe' and `unsubscribe'. `subscribe' will
       be called with a single argument, which is a callable of one
       argument, which should be registered to be called when an event
       pertaining to the `event' object in question occurs. The
       `event' object should then call all such registered callables
       with a single argument describing the event. The argument could
       be any object that can be pickled, but should be an instance of
       a subclass of the L{pdm.perf.event} class. If `subscribe' is
       called with a callback object that it has already registered,
       it should raise a ValueError. `unsubscribe' is called with a
       single argument, which is a previously registered callback
       object, which should then be unregistered to that it is no
       longer called when an event occurs. If the given callback
       object is not, in fact, registered, a ValueError should be
       raised.

    The L{pdm.perf} module contains a few convenience classes which
    implements the interfaces, but PERF objects are not required to be
    instances of them. Any object can implement a PERF interface, as
    long as it does so as described above.

    The L{pdm.cli.perfclient} class is the client-side implementation.
    """
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
    """PDM listener

    This subclass of a thread listens to PDM connections and handles
    client connections properly. It is intended to be subclassed by
    providers of specific domains, such as unixlistener and
    tcplistener.
    """
    def __init__(self):
        super(listener, self).__init__(name = "Management listener")
        self.setDaemon(True)

    def listen(self, sk):
        """Listen for and accept connections."""
        self.running = True
        while self.running:
            rfd, wfd, efd = select.select([sk], [], [sk], 1)
            for fd in rfd:
                if fd == sk:
                    nsk, addr = sk.accept()
                    self.accept(nsk, addr)

    def stop(self):
        """Stop listening for client connections

        Tells the listener thread to stop listening, and then waits
        for it to terminate.
        """
        self.running = False
        self.join()

    def accept(self, sk, addr):
        cl = client(sk)
        cl.start()

class unixlistener(listener):
    """Unix socket listener"""
    def __init__(self, name, mode = 0600, group = None):
        """Create a listener that will bind to the Unix socket named
        by `name'. The socket will not actually be bound until the
        listener is started. The socket will be chmodded to `mode',
        and if `group' is given, the named group will be set as the
        owner of the socket.
        """
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
            sk.setblocking(False)
            self.listen(sk)
        finally:
            sk.close()
            if ul:
                os.unlink(self.name)

class tcplistener(listener):
    """TCP socket listener"""
    def __init__(self, port, bindaddr = "127.0.0.1"):
        """Create a listener that will bind to the given TCP port, and
        the given local interface. The socket will not actually be
        bound until the listener is started.
        """
        super(tcplistener, self).__init__()
        self.port = port
        self.bindaddr = bindaddr

    def run(self):
        sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sk.bind((self.bindaddr, self.port))
            sk.listen(16)
            sk.setblocking(False)
            self.listen(sk)
        finally:
            sk.close()

def listen(spec):
    """Create and start a listener according to a string
    specification. The string specifications can easily be passed from
    command-line options, user configuration or the like. Currently,
    the two following specification formats are recognized:

    PATH[:MODE[:GROUP]] -- PATH must contain at least one slash. A
    Unix socket listener will be created listening to that path, and
    the socket will be chmodded to MODE and owned by GROUP. If MODE is
    not given, it defaults to 0600, and if GROUP is not given, the
    process' default group is used.

    ADDRESS:PORT -- PORT must be entirely numeric. A TCP socket
    listener will be created listening to that port, bound to the
    given local interface address. Since PDM has no authentication
    support, ADDRESS should probably be localhost.
    """
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
