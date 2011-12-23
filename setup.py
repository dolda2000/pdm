#!/usr/bin/python

from distutils.core import setup

setup(name = "pdm",
      version = "0.2",
      description = "Python daemon management library",
      author = "Fredrik Tolf",
      author_email = "fredrik@dolda2000.com",
      url = "http://www.dolda2000.com/~fredrik/pdm/",
      packages = ["pdm"],
      scripts = ["pdm-repl"],
      license = "GPL-3")
