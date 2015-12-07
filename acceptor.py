#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Acceptor(Entity):
    def __init__(self, pid, config_path):
        super(Acceptor, self).__init__(pid, 'acceptors', config_path)
        self.bigger_ballot = 0;
        #maps Instance -> (rnd, v-ballot, v-value)
        self.instance = {}
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                if parsed_message.type == message_pb2.Message.PHASE1A:
                    if not parsed_message.instance in self.instance:
                        #Init with null
                        self.instance[parsed_message.instance] = (parsed_message.ballot, -1, '')
                    if parsed_message.ballot >= self.instance[parsed_message.instance][0]:
                        self.instance[parsed_message.instance] = (parsed_message.ballot,
                        self.instance[parsed_message.instance][1],
                        self.instance[parsed_message.instance][2])
                        message = message_pb2.Message()
                        message.type = message_pb2.Message.PHASE1B
                        message.id = self._id
                        message.instance = parsed_message.instance
                        message.ballot = self.instance[parsed_message.instance][0]
                        message.vballot = self.instance[parsed_message.instance][1];
                        message.vmsg = self.instance[parsed_message.instance][2];
                        self.send(message.SerializeToString(), 'proposers')
                    else: #We require a higher ballot
                        message = message_pb2.Message()
                        message.instance = parsed_message.instance
                        message.type = message_pb2.Message.HIGHBAL
                        message.id = self._id
                        self.send(message.SerializeToString(), 'proposers')
                    
                elif parsed_message.type == message_pb2.Message.PHASE2A:
                    if (parsed_message.ballot >= self.instance[parsed_message.instance][0] and
                        parsed_message.ballot != self.instance[parsed_message.instance][1]):
                        self.instance[parsed_message.instance] = (parsed_message.ballot, 
                        parsed_message.ballot,
                        parsed_message.msg)
                        message = message_pb2.Message()
                        message.instance = parsed_message.instance
                        message.ballot = parsed_message.ballot
                        message.type = message_pb2.Message.PHASE2B
                        message.id = self._id
                        message.msg = parsed_message.msg
                        self.send(message.SerializeToString(), 'proposers')
                    
        
        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    acceptor = Acceptor(int(sys.argv[1]), sys.argv[2])
