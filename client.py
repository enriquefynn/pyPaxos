#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

from message_pb2 import Message
from entity import Entity

from logger import get_logger
critical, info, debug = get_logger(__name__)

class Client(Entity):
    def __init__(self, pid, config_path, values, messages=0):
        super(Client, self).__init__(pid, 'clients', config_path)
        if messages != 0:
            msg = Message(id = self._id,
                                     instance = -1,
                                     msg = '0'*100,
                                     type = Message.PROPOSAL)

            for i in xrange(messages):
                self.send(msg, 'proposers')
            
        for value in values:
            msg = Message(id = self._id,
                                      instance = -1,
                                      msg = value,
                                      type = Message.PROPOSAL)
            self.send(msg, 'proposers')

    def reader_loop(self):
        while True:
            msg = self.recv()
            parsed_message = Message.FromString(msg[0])
        
if __name__ == '__main__':
    from args import args
    if args.b == 0:
        client = Client(args.id, args.config, [line.strip() for line in sys.stdin]) 
    else:
        client = Client(args.id, args.config, [], messages=args.b) 
    gevent.joinall([
        gevent.spawn(client.reader_loop),
    ])
