import traceback
import sqlalchemy
import math
import time
import threading
import logging
import BAC0
import csv
from datetime import datetime, timezone, timedelta
from hvacDBMapping import *
from sqlalchemy.orm import sessionmaker
from queue import Queue
from threading import Thread

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

		#print("Thread started " + threading.current_thread().getName())

		while self.queue.empty() == False:

			# Get the work from the queue and expand the tuple
			data = self.queue.get()

			dataPoint, timesRead = data
			#print(data)
			path, bacnetAddress, bacnetDevId, bacnetObjectType, componentId, pointType, databaseMapping = dataPoint

			#if the devId of the point is known proceed otherwise assign value of -1
			if bacnetDevId != -1:

				reading = readings[componentId]
				#print(reading._AHUNumber)

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
					#lock.acquire()
					#print("Error in retrieving value for " + path)
					notReadableDataPoints.append((dataPoint, errorMsg))
					logging.error("Error in retrieving value for " + bacnetQueryString + " " + errorMsg)
					#lock.release()

				#print(databaseMapping)
				setattr(reading, databaseMapping, readingValue)
			else:
				setattr(reading, databaseMapping, -1)  #For points whose address is unknown

			self.queue.task_done()

		#print("Thread exit " + threading.current_thread().getName())


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

		print("Connection to " + databaseString + " successfull")
		logging.info("Connection to " + databaseString + " successfull")
	except Exception as e:
		logging.error("Error in connection to the database")
		logging.error(traceback.format_exc())
		print("Error in connection to the database")

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


def pullData_multiThread(databaseSession, startDateTime, timeIntervalMin, finishingDateTime=None):
	"""Retrieve the data stored in the trend points of the WebCtrl program from the indicated startDateTime onwards and store them in the database.
	This function will pull data from the database every 5 minutes starting from startDateTime and will keep doing it indefinetly."""

	global readings, notReadableDataPoints

	numberOfThreads = 20

	#get the datapoints and separate them by component type (this should be relaunched everytime the database is modified)
	dataPoints = {key.lower():databaseSession.query(DataPoint._path, DataPoint._bacnetAddress, DataPoint._bacnetDevId, 
		DataPoint._bacnetObjectType, DataPoint._componentId, DataPoint._pointType, PathMapping._databaseMapping).
	join(PathMapping).filter(PathMapping._componentType == key).all() for key in componentsList}

	#print(dataPoints["ahu"])
	#print(dataPoints["ahu"][0]._databaseMapping)

	PDT = timezone(-timedelta(hours=7), 'PDT')
	#timeDelta = timedelta(minutes = 5)

	lock = threading.Lock()

	if finishingDateTime != None:
		continueUntil = lambda readingDateTime : readingDateTime <= finishingDateTime
		print("Finishing dateTime " + str(finishingDateTime))
		logging.info("Finishing dateTime " + str(finishingDateTime))
	else:
		continueUntil = lambda readingDateTime : True

	readingDateTime = startDateTime
	#endDateTime = startDateTime + timeDelta
	#If a finishing datetime is defined continue until that datetime is reached, otherwise continue indefinetely
	while continueUntil(readingDateTime):

		#print("Pulling data from at " + str(readingDateTime))
		logging.info("Pulling data from at " + str(readingDateTime))

		notReadableDataPoints = list()
		#For each type of components get its readings from the bacnet
		for key in dataPoints:
			
			print("\nPulling points of " + key + "\n")
			logging.info("\nPulling points of " + key + "\n")

			#create the necessary classes for the readings
			createReadingClasses(dataPoints, readingDateTime, key)
			#print(readings)

			# Create a queue to communicate with the worker threads
			queue = Queue()

			#Add datapoints to the queue
			for dataPoint in dataPoints[key]:
				if dataPoint._databaseMapping != None:
					queue.put((dataPoint, 1))
				else:
					print("No db mapping for "+dataPoint)
			
			#create the threads and start them
			# Create numberOfThreads worker threads
			workingThreads  = list()
			for i in range(numberOfThreads):
				workingThreads.append(PullingWorker(queue, lock, key, 'Thread-' + str(i+1)))
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

		#Ensure that no more than one read is done per time interval
		timeNow = datetime.now(tz=PDT)
		if timeNow.minute - readingDateTime.minute == 0:
			print("Sleeping for less than 1 minute before continuing (to prevent multiple readings at the sime timestamp)")
			time.sleep(60-timeNow.second+1)

		#Wait until the next time interval
		timeNow = datetime.now(tz=PDT)
		readingDateTime = pauseExecution(timeNow, timeIntervalMin, PDT)


def main():

	global bacnetConnection

	databaseString = "mysql+mysqldb://ihvac:ihvac@192.168.100.2:3306/HVAC2018_03"
	timeIntervalMin = 5 #This defines the time interval to be used for storing the readings
	timeIntervalSec = timeIntervalMin*60

	bacnetConnection = BAC0.connect('10.20.0.169/22', bokeh_server=False)

	for handler in logging.root.handlers[:]:
		logging.root.removeHandler(handler)

	#set the logger config
	logging.basicConfig(filename='dataPull.log', level=logging.WARNING,\
	format='%(levelname)s:%(threadName)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')

	#Make sure starting time is a multiple of 5 in the minutes and that its a past time.
	#To ensure that we will be able to get the readings we try to get the readings from 5+ minutes before the current time. 
	PDT = timezone(-timedelta(hours=7), 'PDT')
	timeNow = datetime.now(tz=PDT)
	timeDelta = timedelta(minutes = timeIntervalMin)

	timeNow = pauseExecution(timeNow, timeIntervalMin, PDT)

	print("Reading started")

	#Make second and microsecond 0 for simplicity purposes.
	timeNow = timeNow.replace(second = 0)
	timeNow = timeNow.replace(microsecond = 0)
	finishingDateTime = timeNow + timeDelta

	sqlsession = getDatabaseConnection(databaseString)

	#Start reading from the devices and keep reading until the code is stopped
	if sqlsession != None:
		#pullData_multiThread(sqlsession)
		pullData_multiThread(sqlsession, timeNow, timeIntervalMin, finishingDateTime)

	print("Main exit")


main()