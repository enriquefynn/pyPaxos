from logging import getLogger, basicConfig, CRITICAL, DEBUG, INFO

verbosities = {'-v': CRITICAL, '-vv': INFO, '-vvv': DEBUG}

def getLogger(name, argv):
	logger = getLogger(name)

	verbosity = min(verbosities.get(arg, CRITICAL+1) for arg in argv or [None])
	basicConfig(level=verbosity)

	return logger.critical, logger.info, logger.debug
