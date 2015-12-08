#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

from logger import get_logger
from sys import argv
critical, info, debug = get_logger(__name__, argv)


class Learner(Entity):
    def __init__(self, pid, config_path):
        super(Learner, self).__init__(pid, 'learners', config_path)
        self.last_received_instance = -1
        self.maximum_instance = -1
        self.non_printed_instances = {}
        self.decided  = {}

    def reader_loop(self):
        while True:
            msg = self.recv()
            parsed_message = message_pb2.Message.FromString(msg[0])
            if parsed_message.type == message_pb2.Message.DECISION:
               
                if parsed_message.instance == self.last_received_instance + 1:
                    debug(parsed_message)
                    #Print the past
                    if (self.maximum_instance != self.last_received_instance and
                    len(self.non_printed_instances) != 0):
                        for instance in xrange(parsed_message.instance, self.maximum_instance+1):
                            if (instance in self.non_printed_instances and 
                            self.non_printed_instances[instance] != ""):
                                info('Decided %s', parsed_message.msg)
                                self.decided[parsed_message.instance] = parsed_message.msg
                        self.non_printed_instances = {}
                        self.last_received_instance = self.maximum_instance
                    else:
                        self.last_received_instance = parsed_message.instance
                        self.maximum_instance = parsed_message.instance
                        info('Decided %s', parsed_message.msg)
                        self.decided[parsed_message.instance] = parsed_message.msg

                #Catch up
                else:
                    if parsed_message.instance > self.maximum_instance:
                        self.maximum_instance = parsed_message.instance
                    if parsed_message.instance < self.last_received_instance:
                        continue
                    self.non_printed_instances[parsed_message.instance] = parsed_message.msg
                    debug(self.non_printed_instances)
                    catch_up = parsed_message.instance - 1
                    while catch_up in self.non_printed_instances:
                        catch_up-=1
                    message = message_pb2.Message(instance = catch_up,
                                                  id = self._id,
                                                  msg = '',
                                                  type = message_pb2.Message.PROPOSAL)
                    if catch_up != -1:
                        debug('Catching up with message {}'.format(catch_up))
                        self.send(message.SerializeToString(), 'proposers')

    def check_loop(self, values, callback):
        while True:
            if set(self.decided.values()) == set(values):
                callback()
            else:
                debug('Missing {}'.format(set(values) - set(self.decided.values())))
            gevent.sleep(1)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    learner = Learner(int(sys.argv[1]), sys.argv[2])
    gevent.joinall([
        gevent.spawn(learner.reader_loop),
        ])