import sys, os
import subprocess, socket, fcntl, select

class sshsocket(object):
    def __init__(self, host, path, user = None, port = None):
        args = ["ssh"]
        if user is not None:
            args += ["-u", str(user)]
        if port is not None:
            args += ["-p", str(int(port))]
        args += [host]
        args += ["python", "-m", "pdm.sshsock", path]
        self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
        fcntl.fcntl(self.proc.stdout, fcntl.F_SETFL, fcntl.fcntl(self.proc.stdout, fcntl.F_GETFL) | os.O_NONBLOCK)

    def close(self):
        if self.proc is not None:
            self.proc.stdin.close()
            self.proc.stdout.close()
            self.proc.wait()
            self.proc = None

    def send(self, data, flags = 0):
        self.proc.stdin.write(data)
        return len(data)

    def recv(self, buflen, flags = 0):
        if (flags & socket.MSG_DONTWAIT) == 0:
            select.select([self.proc.stdout], [], [])
        return self.proc.stdout.read(buflen)

    def fileno(self):
        return self.proc.stdout.fileno()

    def __del__(self):
        self.close()

def cli():
    fcntl.fcntl(sys.stdin, fcntl.F_SETFL, fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
    sk = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sk.connect(sys.argv[1])
        buf1 = ""
        buf2 = ""
        while True:
            wfd = []
            if buf1: wfd.append(sk)
            if buf2: wfd.append(sys.stdout)
            rfd, wfd, efd = select.select([sk, sys.stdin], wfd, [])
            if sk in rfd:
                ret = sk.recv(65536)
                if ret == "":
                    break
                else:
                    buf2 += ret
            if sys.stdin in rfd:
                ret = sys.stdin.read()
                if ret == "":
                    break
                else:
                    buf1 = ret
            if sk in wfd:
                ret = sk.send(buf1)
                buf1 = buf1[ret:]
            if sys.stdout in wfd:
                sys.stdout.write(buf2)
                sys.stdout.flush()
                buf2 = ""
    finally:
        sk.close()

if __name__ == "__main__":
    cli()