import traceback
import sqlalchemy
import math
import time
import threading
import logging
import BAC0
import csv
import requests
from datetime import datetime, timezone, timedelta
from hvacDBMapping import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from queue import Queue
from threading import Thread
from logging.handlers import RotatingFileHandler

global componentsList, componentsClasses, readings, bacnetConnection, notReadableDataPoints

#componentsList = ["Thermafuser"]
componentsList = ["AHU", "VFD", "Filter", "Damper", "Fan", "HEC", "SAV", "VAV", "Thermafuser"]
componentsClasses = {"ahu":AHU, "vfd":VFD, "filter":Filter, "damper":Damper, "fan":Fan, "hec":HEC, "sav":SAV, "vav":VAV, "thermafuser":Thermafuser}
readingClasses = {"ahu":AHUReading, "vfd":VFDReading, "filter":FilterReading, "damper":DamperReading, "fan":FanReading, "hec":HECReading, "sav":SAVReading, "vav":VAVReading, "thermafuser":ThermafuserReading}
bacnetObjectTypes = {"BAV":"analogValue", "BAI":"analogInput", 
"BMSV":"multiStateValue", "BAO":"analogOutput", "ASVI":"analogInput", "BBV":"binaryValue", "BFM":"analogOutput"}
notReadableDataPoints = list()

class PullingWorker(Thread):
	"""multithreaded worker to pull data from the data source (bacnet)"""

	global readings, bacnetConnection, notReadableDataPoints

	def __init__(self, queue, lock, key, tname, maxReadAttempts=5):
		Thread.__init__(self, name=tname)
		self.queue = queue
		self.key = key
		self.lock = lock
		self.maxAttempts = maxReadAttempts

	def attemptReading(self, bacnetQueryString, attemptNumber):

		presentValue = 0
		errorMsg = ''

		#Attempt to get the data from the bacnet
		try:
			presentValue = bacnetConnection.read(bacnetQueryString)
		except Exception as e:
			if attemptNumber < self.maxAttempts:
				presentValue, _ = self.attemptReading(bacnetQueryString, attemptNumber+1)
			else:
				presentValue = None

			errorMsg = type(e).__name__

		return presentValue, errorMsg

	def run(self):

		while self.queue.empty() == False:

			# Get the work from the queue and expand the tuple
			data = self.queue.get()

			dataPoint, timesRead = data
			path, bacnetAddress, bacnetDevId, bacnetObjectType, componentId, pointType, databaseMapping = dataPoint

			#if the devId of the point is known proceed otherwise assign value of -1
			if bacnetDevId != -1:

				reading = readings[componentId]

				#Build the bacnet query string
				readObjectType = bacnetObjectTypes[bacnetObjectType]

				if readObjectType == None:
					print("Object type not determined for " + dataPoint)
					pass

				bacnetQueryString = str(bacnetAddress) + " " + readObjectType + " " + str(bacnetDevId) + " " + "presentValue"

				#Attempt to get the data from the bacnet
				readingValue, errorMsg = self.attemptReading(bacnetQueryString, 1)

				#The point could not be read
				if readingValue == None:
					notReadableDataPoints.append((dataPoint, errorMsg))
					logging.error("Error in retrieving value for " + bacnetQueryString + " " + errorMsg)

				setattr(reading, databaseMapping, readingValue)
			else:
				setattr(reading, databaseMapping, -1)  #For points whose address is unknown

			self.queue.task_done()


def createReadingClasses(dataPoints, readingDateTime, key):
	"""Create the necessary reading classes to store the readings"""

	global readings

	readings = dict()

	for dataPoint in dataPoints[key]:
		path, _, _, _, componentId, _, databaseMapping = dataPoint

		if componentId not in readings:
			readingClass = readingClasses[key](readingDateTime, componentId)
			readings[componentId] = readingClass


def getDatabaseConnection(databaseString):
	"""Attempt connection to the database"""
	
	sqlsession = None

	try:
		sqlengine = sqlalchemy.create_engine(databaseString)
		SQLSession = sessionmaker(bind=sqlengine)
		sqlsession = SQLSession()

		sqlsession.query("1").from_statement(text('SELECT 1')).all() #Test connection

		print("Connection to " + databaseString + " successfull")
		logging.info("Connection to " + databaseString + " successfull")
	except Exception as e:
		logging.error("Error in connection to the database")
		logging.error(traceback.format_exc())
		print("Error in connection to the database")
		raise e

	return sqlsession


def pauseExecution(timeNow, timeIntervalMin, timezone):

	#Wait till timenow is a multiple of the timeInterval
	while timeNow.minute%timeIntervalMin != 0:
		approxSleepTime = timeIntervalMin * math.ceil(timeNow.minute/timeIntervalMin) - timeNow.minute

		if approxSleepTime == 1: #Less than a minute away from desired time
			print("Sleeping for 5 seconds until the next minute multiple of {} is reached".format(timeIntervalMin))
			time.sleep(5)
		else:
			print("Sleeping for approximately {} minutes until the next minute multiple of {} is reached".
				format(approxSleepTime, timeIntervalMin))
			time.sleep((approxSleepTime-1)*60)

		timeNow = datetime.now(tz=timezone)

	return timeNow


def dumpNotReadableDataPoints():

	global notReadableDataPoints

	with open('notReadableDataPoints.csv','w') as t:
		writer = csv.writer(t)
		writer.writerow (['Path','Address', 'DevId', 'ObjectType', 'PointType', 'Error Type'])

		for dataPoint in notReadableDataPoints:
			dp, errorType = dataPoint
			writer.writerow ([dp[0], dp[1], dp[2], dp[3], dp[5], errorType])


def init_readings(databaseSession):
	"""Retrieve the dataPoints to be read and init the readingClasses. A call to this method is necessary before attempting pullData_multiThread"""

	#get the datapoints and separate them by component type (this should be relaunched everytime the database is modified)
	dataPoints = {key.lower():databaseSession.query(DataPoint._path, DataPoint._bacnetAddress, DataPoint._bacnetDevId, 
		DataPoint._bacnetObjectType, DataPoint._componentId, DataPoint._pointType, PathMapping._databaseMapping).
	join(PathMapping).filter(PathMapping._componentType == key).all() for key in componentsList}

	#create the necessary classes for the readings. Note that the readings are global since the are to be used by the threads later on
	#for key in dataPoints:
	#	createReadingClasses(dataPoints, readingDateTime, key)

	return dataPoints


def pullData_multiThread(databaseSession, readingDateTime, dataPoints, numberOfThreads):
	"""Retrieve the data stored in the trend points of the WebCtrl program from the indicated startDateTime onwards and store them in the database.
	This function will pull data from the database every 5 minutes starting from startDateTime and will keep doing it indefinetly."""

	global readings, notReadableDataPoints  #Readings should have already been initialized by a previous call to init_readings

	lock = threading.Lock()

	logging.info("Pulling data from at " + str(readingDateTime))

	notReadableDataPoints = list()

	#For each type of components get its readings from the bacnet
	for key in dataPoints:
			
		#create the necessary classes for the readings
		createReadingClasses(dataPoints, readingDateTime, key)

		print("\nPulling points of " + key + "\n")
		logging.info("\nPulling points of " + key + "\n")

		# Create a queue to communicate with the worker threads
		queue = Queue()

		#Add datapoints to the queue
		for dataPoint in dataPoints[key]:
			if dataPoint._databaseMapping != None:
				queue.put((dataPoint, 1))
			else:
				print("No db mapping for "+dataPoint)
			
		# Create numberOfThreads worker threads and start them
		workingThreads  = list()
		for i in range(numberOfThreads):
			workingThreads.append(PullingWorker(queue, lock, key, 'Thread-' + str(i+1), 2))
			workingThreads[i].start()

		#Wait until all of the threads have finished
		queue.join()
		for i in range(numberOfThreads):
			workingThreads[i].join()
		
		databaseSession.add_all(readings.values())

	databaseSession.commit()
	dumpNotReadableDataPoints()
	print("Readings stored in the Database")
	logging.info("Readings stored in the Database")


def main():

	global bacnetConnection #Needs to be global for the threads to use the connection

	databaseString = "mysql+mysqldb://ihvac:ihvac@192.168.100.2:3306/HVAC2018_03"
	timeIntervalMin = 5
	timeIntervalSec = timeIntervalMin*60

	critical_error_msg = "Critical error, attempting to restart. See log for more information"

	for handler in logging.root.handlers[:]:
		logging.root.removeHandler(handler)

	#set the logger config
	log_formatter = logging.Formatter(fmt='%(levelname)s:%(threadName)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')
	logFile = 'dataPull.log'
	my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024, backupCount=2, encoding=None, delay=0)
	my_handler.setFormatter(log_formatter)
	my_handler.setLevel(logging.ERROR)
	app_log = logging.getLogger('root')
	app_log.setLevel(logging.ERROR)
	app_log.addHandler(my_handler)


	logging.basicConfig(handlers=[my_handler])

	'''logging.basicConfig(filename='dataPull.log', level=logging.ERROR, \
	format='%(levelname)s:%(threadName)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')'''

	#Make sure starting time is a multiple of 5 in the minutes and that its a past time.
	#To ensure that we will be able to get the readings we try to get the readings from 5+ minutes before the current time. 
	PDT = timezone(-timedelta(hours=7), 'PDT')
	timeNow = datetime.now(tz=PDT)
	timeDelta = timedelta(minutes = timeIntervalMin)

	#finishingDateTime = timeNow + timeDelta  #This will perform the reading from a limited perior of time
	finishingDateTime = None 	#This will perform the reading indefinetely

	#Attempt connection to the bacnet and the database
	try:
		bacnetConnection = BAC0.connect('10.20.0.169/22', bokeh_server=False)
		sqlsession = getDatabaseConnection(databaseString)
	except Exception as e:
		print(critical_error_msg)
		requests.get('http://192.168.100.2/iHvac/services/errorMail', params={'message':critical_error_msg, 'subj':'Critical error in server'})
		raise
	else:
		pass
	finally:
		sqlsession.close()

	#Make sure reading starts at a multiple of timeIntervalMin
	#timeNow = pauseExecution(timeNow, timeIntervalMin, PDT)

	if sqlsession != None:

		PDT = timezone(-timedelta(hours=7), 'PDT')

		#Define stopping condition. If a finishing datetime is defined continue until that datetime is reached, otherwise continue indefinetely
		if finishingDateTime != None:
			continueUntil = lambda readingDateTime : readingDateTime <= finishingDateTime
			print("Finishing dateTime " + str(finishingDateTime))
			logging.info("Finishing dateTime " + str(finishingDateTime))
		else:
			continueUntil = lambda readingDateTime : True

		#Start reading from current time
		#Make second and microsecond 0 for simplicity purposes.
		timeNow = timeNow.replace(second = 0) 
		timeNow = timeNow.replace(microsecond = 0)
		readingDateTime = timeNow

		#Init the reading classes and the dataPoints
		dataPoints = init_readings(sqlsession)

		print("Reading started")

		#Start reading from the devices and keep reading until the stopping condition (either finishing time reached or manual stop)
		while continueUntil(readingDateTime):

			#Attempt reading from bacnet and storing into DB
			try:
				pullData_multiThread(sqlsession, readingDateTime, dataPoints, 20)
			except Exception as e:
				print(critical_error_msg)
				requests.get('http://192.168.100.2/iHvac/services/errorMail', params={'message':critical_error_msg, 'subj':'Critical error in server'})
				raise
			else:
				pass
			finally:
				sqlsession.close()

			#Ensure that no more than one read is done per time interval
			timeNow = datetime.now(tz=PDT)
			if timeNow.minute - readingDateTime.minute == 0:
				print("Sleeping for less than 1 minute before continuing (to prevent multiple readings at the same timestamp)")
				time.sleep(60-timeNow.second+1)

			#Wait until the next time interval
			timeNow = datetime.now(tz=PDT)
			readingDateTime = pauseExecution(timeNow, timeIntervalMin, PDT)

	print("Main exit")

	#Free resources
	sqlsession.close()


main()