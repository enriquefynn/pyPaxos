#!/usr/bin/env python2

import gevent, socket, struct
from gevent import select
import sys

from entity import Entity

class Proposer(Entity):
    def __init__(self, config_path):
        super(Proposer, self).__init__('acceptors', config_path)
        def reader_loop():
            while True:
                msg = self.recv()
                print msg

        def read_input():
            while True:
                try:
                    print 'Waiting for input'
                    select.select([sys.stdin], [], [])
                    msg = raw_input()
                    self.send(msg)
                except EOFError, KeyboardInterrupt:
                    pass
        gevent.joinall([
            gevent.spawn(reader_loop),
            gevent.spawn(read_input)
        ])

if __name__ == '__main__':
    proposer = Proposer(sys.argv[1])
