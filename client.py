#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

from logger import get_logger
from sys import argv
critical, info, debug = get_logger(__name__, argv)

class Client(Entity):
    def __init__(self, pid, config_path, values):
        super(Client, self).__init__(pid, 'clients', config_path)
        for value in values:
            msg = message_pb2.Message(id = self._id,
                                      msg = value,
                                      type = message_pb2.Message.PROPOSAL)
            self.send(msg.SerializeToString(), 'proposers')

    def reader_loop(self):
        while True:
            msg = self.recv()
            parsed_message = message_pb2.Message()
            parsed_message.ParseFromString(msg[0])
        
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()

    client = Client(int(sys.argv[1]), sys.argv[2], [line.strip() for line in sys.stdin])
    gevent.joinall([
        gevent.spawn(client.reader_loop),
    ])
