#!/usr/bin/env python3

import logging
from jarmanifest.configuration import config

loggers = {}
configured = False
logginglevel = eval(config.get('logging','level'))

def getLoggingLevel():
	return logginglevel

def configureLogging():
	global configured
	logging.basicConfig(level=logginglevel,format='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')
	configured=True

def setLoggingLevel(tovalue,logger=None):
	global logginglevel
	logginglevel = tovalue
	if logger is None:
		for logger in loggers:
			loggers[logger].setLevel(logginglevel)
	elif logger in loggers.keys():
		loggers[logger].setLevel(logginglevel)

def debugmode():
	setLoggingLevel(logging.DEBUG)

def quiet():
	setLoggingLevel(logging.CRITICAL)

def getLogger(logger='default',level=None):
	"""
	Return the logger associated with a given name. We create the logger
	if it does not exist.
	"""
	if not configured:
		configureLogging()

	if logger not in loggers.keys():
		loggers[logger] = logging.getLogger(logger)

	if level is not None:
		loggers[logger].setLevel(level)

	return loggers[logger]

