import logging
from dataManagement2 import DataManager

def getDesiredData(dataManager, desiredComponents, startDateTime, endDateTime, removeSetpoints=False, removeRequests=False, removeBooleans=False):
	"""Get the data from the desired components at the specified dates. After retrieval, clean and reshape the dataframes
	to disregard unuseful data such as setpoints and requestpoints. Return the cleaned dataframes"""

	#Get the desired data and clean it
	dataFrames = dataManager.readData(startDateTime, endDateTime, desiredComponents)

	for key in dataFrames:
		df = dataFrames[key]
		dataFrames[key] = dataManager.reshapeAndCleanDataFrame(df, removeSetpoints=removeSetpoints, removeRequests=removeRequests, removeBooleans=removeBooleans)

	return dataFrames

def main():

	#set the logger config
	logging.basicConfig(filename='intelligentHVAC.log', level=logging.DEBUG,\
	format='%(levelname)s:%(threadName)s:%(asctime)s:%(filename)s:%(funcName)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S')

	dataManager = DataManager(user='dlaredorazo', password='Dexsys13')

	dataFrames = getDesiredData(dataManager, desiredComponents, startDateTime, endDateTime, removeSetpoints=True, removeRequests=True, removeBooleans=True)


#invoke main
main()