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
        self.last_received_instance = -1
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                if parsed_message.type == message_pb2.Message.DECISION:
                    print parsed_message.instance, self.last_received_instance
                    #Print out last message
                    if parsed_message.instance == self.last_received_instance + 1:
                        self.last_received_instance = parsed_message.instance
                        print self.last_received_instance
                        if parsed_message.msg != "":
                            print parsed_message
                    #Catch up
                    elif parsed_message.instance > self.last_received_instance:
                        print 'Catching up with messages'
                        for instance in xrange(self.last_received_instance+1, 
                                parsed_message.instance+1):
                            message = message_pb2.Message()
                            message.instance = instance
                            message.id = self._id
                            message.msg = ''
                            message.type = message_pb2.Message.PROPOSAL
                            print 'doing instance {}'.format(instance)
                            #self.send(message.SerializeToString(), 'proposers')
        
        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    learner = Learner(int(sys.argv[1]), sys.argv[2])
