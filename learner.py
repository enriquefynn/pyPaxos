#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Learner(Entity):
    def __init__(self, pid, config_path):
        super(Learner, self).__init__(pid, 'learners', config_path)
        self.bigger_ballot = 0;
        #maps Instance -> (v-ballot, v-value)
        self.instance = {}
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                if parsed_message.type == message_pb2.Message.PHASE2B:
                    print parsed_message
        
        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    learner = Learner(int(sys.argv[1]), sys.argv[2])
