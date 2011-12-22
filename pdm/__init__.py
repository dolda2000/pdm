"""Python Daemon Management

This package aids in inspecting and managing daemon processes. A
program intended for running as a daemon may create PDM listeners, to
which PDM clients may connect in order to interact with the
process.

This package contains the following modules:

 * srv -- Server module
 * cli -- Client module
 * perf -- Library for implementing object for the PERF protocol

The protocol allows multiple management subprotocols for different
modes of operation. Currently, the following two management protocols
are supported.

 * The REPL protocol implements a simple read-eval-print loop which
   accepts arbitrary Python code, executes it in the daemon process,
   and returns its replies, all in text form. The protocol is simple,
   generic, and has few failure modes, but is hardly suitable for
   programmatic interaction. See the documentation for pdm.srv.repl
   and pdm.cli.replclient for further details.

 * The PERF protocol is intended for programmatic interaction with the
   daemon process. Various Python modules may expose objects that
   implement one or several of a few pre-defined interfaces that allow
   for various forms of inspection and management of the program
   state. See the documentation for pdm.srv.perf and
   pdm.cli.perfclient for further details.
"""
