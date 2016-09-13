#!/usr/bin/env python

### become cooperative

import gevent.monkey
gevent.monkey.patch_all()

### import

import sys
import nose

from os.path import dirname, realpath
sys.path.append(dirname(dirname(realpath(__file__))))

from mqks.tests.env import EnvPlugin

### tests main

if __name__ == '__main__':
    nose.main(addplugins=[EnvPlugin()])
