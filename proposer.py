#!/usr/bin/env python2
import gevent, socket, time
from gevent import select
import sys

from message_pb2 import Message
from entity import Entity

from collections import namedtuple
class InstanceState(namedtuple('InstanceState', 'ballot acceptor_messages timestamp phase msg')):
    def __new__(cls, ballot, timestamp, phase, msg):
        return super(InstanceState, cls).__new__(cls, ballot, [], timestamp, phase, msg)   

from logger import get_logger
critical, info, debug = get_logger(__name__)


class Proposer(Entity):
    def __init__(self, pid, config_path):
        super(Proposer, self).__init__(pid, 'proposers', config_path)
        self.instance = 0
        self.leaders = {} #leader candidates
        self.leader = self._id #start as the leader
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
            parsed_message = Message.FromString(msg[0])
            #See if it's time to change the leader
            if parsed_message.type == Message.LEADER:
                #See if we are in sync with instances
                if parsed_message.instance > self.instance:
                    self.instance = parsed_message.instance
                self.leaders[parsed_message.id] = time.time()
                k = sorted(self.leaders.keys())[0]
                if time.time() - self.leaders[k] > 2*self.get_timeout_msgs():
                    #Not the leader anymore
                    del self.leaders[k]
                    if len(self.leaders) > 0:
                        self.leader = sorted(self.leaders.keys())[0]
                    else:
                        self.leader = self._id
                    info('Switchig to next leader {}'.format(self.leader))
                else:
                    self.leader = k

            #Im not the leader
            if self.leader != self._id:
                continue
            #Got a Proposal from client
            if parsed_message.type == Message.PROPOSAL:
                debug(parsed_message)
                self.state[self.instance] = InstanceState(ballot = self._id,
                                                          timestamp = time.time(),
                                                          phase = Message.PHASE1A,
                                                          msg = parsed_message.msg)
                #Build Phase1A
                message = Message(type = Message.PHASE1A,
                                              id = self._id)
                #learner catch up msg
                if parsed_message.instance != -1:
                    message.instance = parsed_message.instance
                    if not parsed_message.instance in self.state:
                        self.state[parsed_message.instance] = InstanceState(ballot = self._id,
                                                                            timestamp = time.time(),
                                                                            phase = Message.PHASE1A,
                                                                            msg = parsed_message.msg)
                    message.ballot = self.state[parsed_message.instance].ballot +100
                    self.state[parsed_message.instance] = self.state[parsed_message.instance]._replace(ballot = message.ballot)
                #client instance
                else:
                    message.ballot = self.state[self.instance].ballot
                    message.instance = self.instance
                    self.instance+=1
                self.send(message, 'acceptors')
            
            #Got a Phase 1B
            elif (parsed_message.type == Message.PHASE1B and
            parsed_message.instance in self.state):
                if not parsed_message.instance in self.acceptor_messages:
                    self.acceptor_messages[parsed_message.instance] = [parsed_message]
                else:
                    self.acceptor_messages[parsed_message.instance].append(parsed_message)
                #Already sent 2A (had a quorum)
                if self.state[parsed_message.instance].phase == Message.PHASE2A:
                    continue
                #See if quorum is reached
                n_msgs = set([])
                current_propose = (-1, self.state[parsed_message.instance].msg)
                debug(self.acceptor_messages)
                for msg in self.acceptor_messages[parsed_message.instance]:
                    debug(msg)
                    if msg.ballot == self.state[parsed_message.instance].ballot:
                        n_msgs.add(msg.id)
                        current_propose = max(current_propose, 
                        (parsed_message.vballot, parsed_message.vmsg))
                    debug(n_msgs)

                if len(n_msgs) >= self.get_number_of_acceptors()//2+1:
                    debug('Quorum reached, initiating 2A')
                    self.state[parsed_message.instance] = self.state[parsed_message.instance]._replace(phase = Message.PHASE2A, timestamp = time.time())
                    message = Message(type = Message.PHASE2A,
                                      id = self._id,
                                      ballot = self.state[parsed_message.instance].ballot,
                                      msg = current_propose[1],
                                      instance = parsed_message.instance)
                    self.send(message, 'acceptors')

            #Received phase 2B
            elif (parsed_message.type == Message.PHASE2B and
            parsed_message.instance in self.state):
                if not parsed_message.instance in self.acceptor_decide:
                    self.acceptor_decide[parsed_message.instance] = [parsed_message]
                else:
                    self.acceptor_decide[parsed_message.instance].append(parsed_message)
                n_msgs = set([])
                for msg in self.acceptor_decide[parsed_message.instance]:
                    n_msgs.add(msg.id)
                
                #Quorum reached, must inform learners
                if (len(n_msgs) >= (self.get_number_of_acceptors()+1)/2 and
                    self.state[parsed_message.instance].phase != Message.DECISION):
                    debug('Informing decide to learners')
                    debug(parsed_message)

                    self.state[parsed_message.instance] = self.state[parsed_message.instance]._replace(phase = Message.DECISION, timestamp = time.time())

                    message = Message(type = Message.DECISION,
                                                  id = self._id,
                                                  msg = parsed_message.msg,
                                                  instance = parsed_message.instance)
                    self.send(message, 'learners')
            
    def check_unresponsive_msgs(self):
        while True:
            for instance in self.state:
                if (self.state[instance].phase != Message.DECISION and
                abs(time.time() - self.state[instance].timestamp) > self.get_timeout_msgs()):
                    debug('Found unresponsive messages, will try again')

                    #Grow ballot by arbitrary number
                    debug(self.state[instance])
                    self.state[instance] = self.state[instance]._replace(ballot = self.state[instance].ballot + 100, timestamp = time.time(), phase = Message.PHASE1A)

                    #Build Phase1A
                    #TODO: don't replicate code
                    message = Message(type = Message.PHASE1A,
                                                  id = self._id,
                                                  instance = instance,
                                                  ballot = self.state[instance].ballot)
                    debug('msg:')
                    debug(message)
                    self.send(message, 'acceptors')

            gevent.sleep(self.get_timeout_msgs())
    
    def leader_election(self):
        while True:
            message = Message(type = Message.LEADER,
                                id = self._id,
                                instance = self.instance)
            self.send(message, 'proposers')
            gevent.sleep(self.get_leader_timeout())

if __name__ == '__main__':
    from args import args
    proposer = Proposer(args.id, args.config)
    gevent.joinall([
        gevent.spawn(proposer.reader_loop),
        gevent.spawn(proposer.check_unresponsive_msgs),
        gevent.spawn(proposer.leader_election),
    ])
