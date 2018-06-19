from flask import Flask
from flask import request
from . import db


@app.route('/reportHvacIssue', methods=['POST'])
def reporthvacIssue():

	if request.method == 'POST':
		locationBuilding = request.form['locBuilding']
    	locationFloor = request.form['locFloor']
    	locationRoom = request.form['locRoom']
    	description = request.form['description']

    return "parameters are " +  locationBuilding + " and " + locationFloor + " and " + locationRoom + " and " + description

@app.route('/getBuildingRoomsByFloor', methods=['POST'])
def reporthvacIssue():

	if request.method == 'POST':
		locationBuilding = request.form['locBuilding']
    	locationFloor = request.form['locFloor']
    	locationRoom = request.form['locRoom']
    	description = request.form['description']

    return "parameters are " +  locationBuilding + " and " + locationFloor + " and " + locationRoom + " and " + description