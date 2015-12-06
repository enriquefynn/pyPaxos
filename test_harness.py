#!/usr/bin/env python

from gevent import joinall, spawn

from acceptor import Acceptor
from proposer import Proposer
from client import Client
from learner import Learner

from logger import get_logger
from logging import getLogger, CRITICAL, INFO, DEBUG
from sys import argv

critical, debug, info = get_logger(__name__, argv)

if __name__ == '__main__':
	debug('Starting processes')

	from sys import stdin
	values = [line.strip() for line in stdin]

	config = 'config.txt'
	acceptors = [Acceptor(1, config),
				 Acceptor(2, config),
				 Acceptor(3, config)]
	proposers = [Proposer(1, config)]
	learners  = [Learner(1, config)]

	
	clients   = [Client(3, config, values)]

	# suppress logging
	for module in (x.__module__ for x in {Acceptor, Proposer, Client}):
		getLogger(module).setLevel(level=CRITICAL)
	getLogger(Learner.__module__).setLevel(level=INFO)

	from sys import exit
	joinall([spawn(x.reader_loop) for x in acceptors+proposers+learners+clients]
		  + [spawn(x.check_loop, values, lambda: exit(0)) for x in learners])
