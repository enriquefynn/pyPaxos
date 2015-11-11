#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Proposer(Entity):
    def __init__(self, pid, config_path):
        super(Proposer, self).__init__(pid, 'acceptors', config_path)
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                print parsed_message

        def read_input():
            while True:
                try:
                    print 'Waiting for input'
                    select.select([sys.stdin], [], [])
                    msg = raw_input()
                    message = message_pb2.Message()
                    message.name = 'acceptors'
                    message.id = self._id
                    message.msg = msg
                    message.type = message_pb2.Message.PHASE1A
                    self.send(message.SerializeToString(), 'proposers')
                except EOFError, KeyboardInterrupt:
                    exit(0)
        gevent.joinall([
            gevent.spawn(reader_loop),
            gevent.spawn(read_input)
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    proposer = Proposer(int(sys.argv[1]), sys.argv[2])
