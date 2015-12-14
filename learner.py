#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

from message_pb2 import Message
from entity import Entity

from logger import get_logger
critical, info, debug = get_logger(__name__)


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
            parsed_message = Message.FromString(msg[0])
            if parsed_message.type == Message.DECISION:
                #Ignore the past
                if parsed_message.instance <= self.last_received_instance:
                    continue
                self.maximum_instance = max(self.maximum_instance, parsed_message.instance)
                debug('{} {}'.format(self.last_received_instance, self.maximum_instance))
                debug(parsed_message)

                self.non_printed_instances[parsed_message.instance] = parsed_message.msg
                while (self.last_received_instance + 1) in self.non_printed_instances:
                    self.last_received_instance+=1
                    if parsed_message.msg != '':
                        sys.stdout.write("%s\n" % self.non_printed_instances[self.last_received_instance])
                        sys.stdout.flush()
                    self.decided[self.last_received_instance] = self.non_printed_instances[self.last_received_instance]
                if self.last_received_instance < self.maximum_instance:
                    message = Message(instance = self.last_received_instance+1,
                                      id = self._id,
                                      msg = '',
                                      type = Message.PROPOSAL)
                    debug('Catching up with message {}'.format(self.last_received_instance+1))
                    self.send(message, 'proposers')

    def check_loop(self, values, callback):
        while True:
            if set(self.decided.values()) == set(values):
                callback()
            else:
                debug('Missing {}'.format(set(values) - set(self.decided.values())))
            gevent.sleep(1)

if __name__ == '__main__':
    from args import args
    learner = Learner(args.id, args.config)
    gevent.joinall([
        gevent.spawn(learner.reader_loop),
        ])
