#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Learner(Entity):
    def __init__(self, pid, config_path):
        super(Learner, self).__init__(pid, 'learners', config_path)
        self.last_received_instance = -1
        self.maximum_instance = -1
        self.non_printed_instances = {}
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                if parsed_message.type == message_pb2.Message.DECISION:
                    if parsed_message.instance == self.last_received_instance + 1:
                        print parsed_message.msg
                        #Print the past
                        if (self.maximum_instance != self.last_received_instance and
                        len(self.non_printed_instances) != 0):
                            for instance in xrange(parsed_message.instance, self.maximum_instance+1):
                                if (instance in self.non_printed_instances and 
                                self.non_printed_instances[instance] != ""):
                                    print self.non_printed_instances[instance]
                            self.non_printed_instances = {}
                            self.last_received_instance = self.maximum_instance
                        else:
                            self.last_received_instance = parsed_message.instance
                            self.maximum_instance = parsed_message.instance
                    #Catch up
                    else:
                        if parsed_message.instance > self.maximum_instance:
                            self.maximum_instance = parsed_message.instance
                        if parsed_message.instance < self.last_received_instance:
                            continue
                        self.non_printed_instances[parsed_message.instance] = parsed_message.msg
                        print self.non_printed_instances
                        catch_up = parsed_message.instance - 1
                        while catch_up in self.non_printed_instances:
                            catch_up-=1
                        message = message_pb2.Message()
                        message.instance = catch_up
                        message.id = self._id
                        message.msg = ''
                        message.type = message_pb2.Message.PROPOSAL
                        if catch_up != -1:
                            print 'Catching up with message {}'.format(catch_up)
                            self.send(message.SerializeToString(), 'proposers')
                    
        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    learner = Learner(int(sys.argv[1]), sys.argv[2])
