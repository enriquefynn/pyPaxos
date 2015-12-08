#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

from message_pb2 import Message
from entity import Entity

from collections import namedtuple
class InstanceState(namedtuple('InstanceState', 'ballot vballot vmsg')):
    def __new__(cls, ballot=None, vballot=-1, vmsg=''):
        return super(InstanceState, cls).__new__(cls, ballot, vballot, vmsg)

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
            msg = Message.FromString(msg[0])
            if msg.type == Message.PHASE1A:
                if not msg.instance in self.instance:
                    #Init with null
                    self.instance[msg.instance] = InstanceState(msg.ballot)
                if msg.ballot >= self.instance[msg.instance].ballot:
                    self.instance[msg.instance] = self.instance[msg.instance]._replace(ballot=msg.ballot)                   
                    message = Message(type = Message.PHASE1B,
                                      id = self._id,
                                      instance = msg.instance,
                                      **self.instance[msg.instance]._asdict())
                    self.send(message, 'proposers')
                else: #We require a higher ballot
                    message = Message(instance = msg.instance,
                                      type = Message.HIGHBAL,
                                      id = self._id)
                    self.send(message, 'proposers')

            elif msg.type == Message.PHASE2A:
                debug('Received decide')
                debug(msg)
                debug(self.instance[msg.instance])
                debug('EAAAA1 %s %s %s', msg.ballot, self.instance[msg.instance])
                if (msg.ballot >= self.instance[msg.instance].ballot and
                    msg.ballot != self.instance[msg.instance].vballot):
                    self.instance[msg.instance] = InstanceState(msg.ballot, 
                                                                           msg.ballot,
                                                                           msg.msg)
                    message = Message(instance = msg.instance,
                                      ballot = msg.ballot,
                                      type = Message.PHASE2B,
                                      id = self._id,
                                      msg = msg.msg)
                    self.send(message, 'proposers')
                debug('EAAAA2 %s %s %s', msg.ballot, self.instance[msg.instance])


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    acceptor = Acceptor(int(sys.argv[1]), sys.argv[2])
    gevent.joinall([
            gevent.spawn(acceptor.reader_loop),
        ])
