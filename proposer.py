#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Proposer(Entity):
    def __init__(self, pid, config_path):
        super(Proposer, self).__init__(pid, 'proposers', config_path)
        self.leader = self._id
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                print parsed_message

        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    proposer = Proposer(int(sys.argv[1]), sys.argv[2])
