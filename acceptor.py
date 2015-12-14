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
critical, info, debug = get_logger(__name__)


class Acceptor(Entity):
    def __init__(self, pid, config_path):
        super(Acceptor, self).__init__(pid, 'acceptors', config_path)
        self.bigger_ballot = 0;
        #maps Instance -> (rnd, v-ballot, v-value)
        self.state = {}
    def reader_loop(self):
        while True:
            msg = self.recv()
            msg = Message.FromString(msg[0])
            if msg.type == Message.PHASE1A:
                if not msg.instance in self.state:
                    #Init with null
                    self.state[msg.instance] = InstanceState(msg.ballot)
                if msg.ballot >= self.state[msg.instance].ballot:
                    self.state[msg.instance] = self.state[msg.instance]._replace(ballot=msg.ballot)                   
                    message = Message(type = Message.PHASE1B,
                                      id = self._id,
                                      instance = msg.instance,
                                      **self.state[msg.instance]._asdict())
                    self.send(message, 'proposers')

            elif msg.type == Message.PHASE2A:
                debug('Received decide')
                debug(msg)
                debug(self.state[msg.instance])
                if (msg.ballot >= self.state[msg.instance].ballot and
                    msg.ballot != self.state[msg.instance].vballot):
                    self.state[msg.instance] = InstanceState(msg.ballot, 
                                                                           msg.ballot,
                                                                           msg.msg)
                    message = Message(instance = msg.instance,
                                      ballot = msg.ballot,
                                      type = Message.PHASE2B,
                                      id = self._id,
                                      msg = msg.msg)
                    self.send(message, 'proposers')

if __name__ == '__main__':
    from args import args
    acceptor = Acceptor(args.id, args.config)
    gevent.joinall([
            gevent.spawn(acceptor.reader_loop),
        ])
