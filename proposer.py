#!/usr/bin/env python2
import gevent, socket, time
from sets import Set
from gevent import select
import sys

import message_pb2
from entity import Entity

from logger import get_logger
from sys import argv
critical, info, debug = get_logger(__name__, argv)


class Proposer(Entity):
    def __init__(self, pid, config_path):
        super(Proposer, self).__init__(pid, 'proposers', config_path)
        self.instance = 0
        self.leader = 0
        self.incremental = self._id
        # State has:
        #  {instance: {ballot, acceptor_messages, phase, timestamp}}
        self.state = {}
        # {instance: [msgs]}
        self.acceptor_messages = {}
        # {instance: [msgs]}
        self.acceptor_decide = {}

    def reader_loop(self):
        while True:
            msg = self.recv()
            parsed_message = message_pb2.Message.FromString(msg[0])
            #Got a Proposal from client
            if parsed_message.type == message_pb2.Message.PROPOSAL:
                debug(parsed_message)
                self.state[self.instance] = {
                'ballot': self._id,
                'acceptor_messages': [],
                'timestamp': time.time(),
                'phase': message_pb2.Message.PHASE1A,
                'msg': parsed_message.msg
                }
                #Build Phase1A
                message = message_pb2.Message(type = message_pb2.Message.PHASE1A,
                                              id = self._id)
                #learner catch up msg
                if parsed_message.instance != -1:
                    message.instance = parsed_message.instance
                    self.state[parsed_message.instance]['ballot']+=100
                    message.ballot = self.state[parsed_message.instance]['ballot']
                #client instance
                else:
                    message.ballot = self.state[self.instance]['ballot']
                    message.instance = self.instance
                    self.instance+=1
                self.send(message.SerializeToString(), 'acceptors')
            
            #Got a Phase 1B
            elif (parsed_message.type == message_pb2.Message.PHASE1B and
            parsed_message.instance in self.state):
                if not parsed_message.instance in self.acceptor_messages:
                    self.acceptor_messages[parsed_message.instance] = [parsed_message]
                else:
                    self.acceptor_messages[parsed_message.instance].append(parsed_message)
                #Already sent 2A (had a quorum)
                if self.state[parsed_message.instance]['phase'] == message_pb2.Message.PHASE2A:
                    continue
                #See if quorum is reached
                n_msgs = Set([])
                current_propose = (-1, self.state[parsed_message.instance]['msg'])
                debug(self.acceptor_messages)
                for msg in self.acceptor_messages[parsed_message.instance]:
                    debug('-----00-----')
                    debug(msg)
                    if msg.ballot == self.state[parsed_message.instance]['ballot']:
                        n_msgs.add(msg.id)
                        current_propose = max(current_propose, 
                        (parsed_message.vballot, parsed_message.vmsg))
                    debug(n_msgs)

                if len(n_msgs) >= (self.get_number_of_acceptors()+1)/2:
                    debug('Quorum reached, initiating 2A')
                    self.state[parsed_message.instance]['phase'] = message_pb2.Message.PHASE2A
                    self.state[parsed_message.instance]['timestamp'] = time.time()
                    message = message_pb2.Message(type = message_pb2.Message.PHASE2A,
                                                  id = self._id,
                                                  ballot = self.state[parsed_message.instance]['ballot'],
                                                  msg = current_propose[1],
                                                  instance = parsed_message.instance)
                    self.send(message.SerializeToString(), 'acceptors')

            #Received phase 2B
            elif (parsed_message.type == message_pb2.Message.PHASE2B and
            parsed_message.instance in self.state):
                if not parsed_message.instance in self.acceptor_decide:
                    self.acceptor_decide[parsed_message.instance] = [parsed_message]
                else:
                    self.acceptor_decide[parsed_message.instance].append(parsed_message)
                n_msgs = Set([])
                for msg in self.acceptor_decide[parsed_message.instance]:
                    n_msgs.add(msg.id)
                
                #Quorum reached, must inform learners
                if (len(n_msgs) >= (self.get_number_of_acceptors()+1)/2 and
                self.state[parsed_message.instance]['phase'] != message_pb2.Message.DECISION):
                    debug('Informing decide to learners')
                    debug(parsed_message)
                    self.state[parsed_message.instance]['phase'] = message_pb2.Message.DECISION
                    self.state[parsed_message.instance]['timestamp'] = time.time()
                    message = message_pb2.Message(type = message_pb2.Message.DECISION,
                                                  id = self._id,
                                                  msg = parsed_message.msg,
                                                  instance = parsed_message.instance)
                    self.send(message.SerializeToString(), 'learners')
            
            #Acceptor told me to pick a higher ballot
            elif parsed_message.type == message_pb2.Message.HIGHBAL:
                #I'm not in that instance yet
                if not parsed_message.instance in self.state:
                    continue
                #Grow ballot by arbitrary number
                self.state[parsed_message.instance]['ballot'] += 100 
                self.state[parsed_message.instance]['timestamp'] = time.time()
                #Build Phase1A
                #TODO: don't replicate code
                message = message_pb2.Message(type = message_pb2.Message.PHASE1A,
                                              id = self._id,
                                              instance = parsed_message.instance,
                                              ballot = self.state[parsed_message.instance]['ballot'])
                self.send(message.SerializeToString(), 'acceptors')
    
    def check_unresponsive_msgs(self):
        #FIXME: Do this later :-P
        return 0
        while True:
            for instance in self.state:
                if (self.state[instance]['phase'] != message_pb2.Message.DECISION and
                time.time() - self.state[instance]['timestamp'] > self.get_timeout_msgs()):
                    debug('Found unresponsive messages, will try again')

                    #Grow ballot by arbitrary number
                    self.state[instance]['ballot'] +=100
                    self.state[instance]['timestamp'] = time.time()
                    self.state[instance]['phase'] = message_pb2.Message.PHASE1A

                    #Build Phase1A
                    #TODO: don't replicate code
                    message = message_pb2.Message(type = message_pb2.Message.PHASE1A,
                                                  id = self._id,
                                                  instance = instance,
                                                  ballot = self.state[instance]['ballot'])
                    debug('msg:')
                    debug(message)
                    self.send(message.SerializeToString(), 'acceptors')

            gevent.sleep(self.get_timeout_msgs())

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    proposer = Proposer(int(sys.argv[1]), sys.argv[2])

    gevent.joinall([
        gevent.spawn(proposer.reader_loop),
        gevent.spawn(proposer.check_unresponsive_msgs),
    ])
