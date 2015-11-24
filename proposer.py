#!/usr/bin/env python2
import gevent, socket, struct
from gevent import select
import sys

import message_pb2
from entity import Entity

class Proposer(Entity):
    def __init__(self, pid, config_path):
        super(Proposer, self).__init__(pid, 'proposers', config_path)
        self.instance = -1
        self.leader = 0
        self.incremental = self._id
        #State has:
        #  [{ballot, acceptor_messages, phase}]
        self.state = []
        def reader_loop():
            while True:
                msg = self.recv()
                parsed_message = message_pb2.Message()
                parsed_message.ParseFromString(msg[0])
                if parsed_message.type == message_pb2.Message.PROPOSAL:
                    self.state.append({'ballot': self._id, 
                    'acceptor_messages': [], 
                    'phase': message_pb2.Message.PHASE1A})
                    self.instance+= 1
                    #Build Phase1A
                    message = message_pb2.Message()
                    message.type = message_pb2.Message.PHASE1A
                    message.id = self._id
                    message.instance = self.instance
                    message.ballot = self.state[self.instance]['ballot']
                    self.send(message.SerializeToString(), 'acceptors')
                
                #Got a Phase 1B
                elif (parsed_message.type == message_pb2.Message.PHASE1B and
                parsed_message.ballot == self.incremental):
                    if not parsed_message.instance in self.acceptor_messages:
                        self.acceptor_messages[parsed_message.instance] = [parsed_message]
                    else:
                        self.acceptor_messages.append(parsed_message)
                    #See if quorum is reached
                    if (len(self.acceptor_messages[parsed_message.instance]) >= 
                        (self.get_number_of_acceptors()+1)/2):
                        print('Quorum reached, initiating 2B')
                        mesage = message_pb2.Message()
                        message.type = message_pb2.Message.PHASE1B
                        message.instance = self.instance
                        
                     
                print parsed_message

        gevent.joinall([
            gevent.spawn(reader_loop),
        ])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('./acceptor.py <id> <config>')
        sys.exit()
    proposer = Proposer(int(sys.argv[1]), sys.argv[2])
