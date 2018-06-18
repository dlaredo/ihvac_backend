from flask import Flask
from flask import request


app = Flask(__name__)

@app.route('/reportHvacIssue', methods=['POST'])
def reporthvacIssue():

	locationBuilding = request.form['locBuilding']
	locationFloor = request.form['locFloor']
	locationRoom = request.form['locRoom']
	description = request.form['description']

	return "parameters are " +  locationBuilding + " and " + locationFloor + " and " + locationRoom + " and " + description