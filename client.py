#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

from message_pb2 import Message
from entity import Entity

from logger import get_logger
critical, info, debug = get_logger(__name__)

class Client(Entity):
    def __init__(self, pid, config_path):
        super(Client, self).__init__(pid, 'clients', config_path)

    def read_input(self):
        while True:
            try:
                select.select([sys.stdin], [], [])
                msg_text = raw_input()
                msg = Message(instance = -1,
                              id = self._id,
                              msg = msg_text,
                              type = Message.PROPOSAL)

                self.send(msg, 'proposers')
            except EOFError, KeyboardInterrupt:
                exit(0)
        
if __name__ == '__main__':
    from args import args
    client = Client(args.id, args.config)
    gevent.joinall([
        gevent.spawn(client.read_input)
    ])
