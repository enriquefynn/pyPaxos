from logging import getLogger, basicConfig, CRITICAL, DEBUG, INFO
from args import args
verbosities = {1: CRITICAL, 2: INFO, 3: DEBUG}

def get_logger(name):
	logger = getLogger(name)

	basicConfig(level=verbosities.get(args.v, CRITICAL+1))
	return logger.critical, logger.info, logger.debug


