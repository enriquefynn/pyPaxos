#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

from message_pb2 import Message
from entity import Entity

from logger import get_logger
from sys import argv
critical, info, debug = get_logger(__name__, argv)


class Acceptor(Entity):
    def __init__(self, pid, config_path):
        super(Acceptor, self).__init__(pid, 'acceptors', config_path)
        self.bigger_ballot = 0;
        #maps Instance -> (rnd, v-ballot, v-value)
        self.instance = {}
    def reader_loop(self):
        while True:
            msg = self.recv()
            parsed_message = Message()
            parsed_message.ParseFromString(msg[0])
            if parsed_message.type == Message.PHASE1A:
                if not parsed_message.instance in self.instance:
                    #Init with null
                    self.instance[parsed_message.instance] = (parsed_message.ballot, -1, '')
                if parsed_message.ballot >= self.instance[parsed_message.instance][0]:
                    self.instance[parsed_message.instance] = (parsed_message.ballot,
                    self.instance[parsed_message.instance][1],
                    self.instance[parsed_message.instance][2])
                    message = Message(type = Message.PHASE1B,
                                                  id = self._id,
                                                  instance = parsed_message.instance,
                                                  ballot = self.instance[parsed_message.instance][0],
                                                  vballot = self.instance[parsed_message.instance][1],
                                                  vmsg = self.instance[parsed_message.instance][2])
                    self.send(message, 'proposers')
                else: #We require a higher ballot
                    message = Message(instance = parsed_message.instance,
                                                  type = Message.HIGHBAL,
                                                  id = self._id)
                    self.send(message, 'proposers')
                
            elif parsed_message.type == Message.PHASE2A:
                debug('Received decide')
                debug(parsed_message)
                debug(self.instance[parsed_message.instance])
                debug('EAAAA1 %s %s %s', parsed_message.ballot, self.instance[parsed_message.instance][0], self.instance[parsed_message.instance][1])
                if (parsed_message.ballot >= self.instance[parsed_message.instance][0] and
                    parsed_message.ballot != self.instance[parsed_message.instance][1]):
                    self.instance[parsed_message.instance] = (parsed_message.ballot, 
                    parsed_message.ballot,
                    parsed_message.msg)
                    message = Message(instance = parsed_message.instance,
                                                  ballot = parsed_message.ballot,
                                                  type = Message.PHASE2B,
                                                  id = self._id,
                                                  msg = parsed_message.msg)
                    self.send(message, 'proposers')
                debug('EAAAA2 %s %s %s', parsed_message.ballot, self.instance[parsed_message.instance][0], self.instance[parsed_message.instance][1])


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    acceptor = Acceptor(int(sys.argv[1]), sys.argv[2])
    gevent.joinall([
            gevent.spawn(acceptor.reader_loop),
        ])
