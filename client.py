#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

from logger import get_Logger
from sys import argv
critical, info, debug = get_Logger(__name__, argv)

class Client(Entity):
    def __init__(self, pid, config_path):
        super(Client, self).__init__(pid, 'clients', config_path)
    def reader_loop(self):
        while True:
            msg = self.recv()
            parsed_message = message_pb2.Message()
            parsed_message.ParseFromString(msg[0])

    def read_input(self):
        while True:
            try:
                debug('Waiting for input')
                select.select([sys.stdin], [], [])
                msg = raw_input()
                message = message_pb2.Message()
                message.id = self._id
                message.msg = msg
                message.type = message_pb2.Message.PROPOSAL
                self.send(message.SerializeToString(), 'proposers')
            except EOFError, KeyboardInterrupt:
                exit(0)
    

        
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    client = Client(int(sys.argv[1]), sys.argv[2])
    gevent.joinall([
        gevent.spawn(client.reader_loop),
        gevent.spawn(client.read_input)
    ])
