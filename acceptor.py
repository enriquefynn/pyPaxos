#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Acceptor(Entity):
    def __init__(self, pid, config_path):
        super(Proposer, self).__init__(pid, 'acceptors', config_path)
        self.bigger_ballot = 0;
        #maps Instance -> (v-ballot, v-value)
        self.instance_accepted = {}
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                if parsed_message.type == message_pb2.Message.PHASE1A:
                    if not parsed_message.instance in self.instance_accepted:
                        #Init with null
                        self.instance_accepted[parsed_message.instance] = (-1, None)
                    if parsed_message.ballot > self.instance_accepted[parsed_message.instance][0]:
                        self.ballot = parsed_message.ballot
                        message = message_pb2.Message()
                        message.type = message_pb2.Message.PHASE1B
                        message.id = self._id
                        message.instance = parsed_message.instance
                        message.vballot = self.instance_accepted[parsed_message.instance][0];
                        message.vmsg = self.instance_accepted[parsed_message.instance][1];
                        self.send(message.SerializeToString(), 'proposers')
                    
                print parsed_message

        
        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    proposer = Proposer(int(sys.argv[1]), sys.argv[2])
