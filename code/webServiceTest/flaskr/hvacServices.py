from flask import request, current_app, Blueprint, render_template
from sqlalchemy import and_
from flask_sqlalchemy import SQLAlchemy
from flaskr.db import *
from flaskr.hvacIssueDBMapping import *
from . import global_s
from flask import jsonify

bp = Blueprint('services', __name__, url_prefix='/services')

# a simple page that says hello
@bp.route('/reportHvacIssueForm')
def reportHvacIssueForm():
    return render_template('report_hvac_issue.html')

@bp.route('/reportHvacIssue', methods=['POST'])
def reporthvacIssue():

    if request.method == 'POST':
    	data = request.get_json()

    	locationBuilding = data['buildingId']
    	locationFloor = data['floorId']
    	locationRoom = data['roomId']
    	description = data['issueDescription']

    	hvacIssue = HVACIssue(locationBuilding, locationFloor, locationRoom, description)

    	global_s.dbConnection.session.add(hvacIssue)
    	global_s.dbConnection.session.commit()

    return jsonify(True)

@bp.route('/getBuildingRoomsByFloor', methods=['POST'])
def getBuildingRoomsByFloor():

    if request.method == 'POST':
        data = request.get_json()

        floor = global_s.dbConnection.session.query(Floor).filter(and_(Floor._buildingId == data["buildingId"]), Floor._floorNumber == data["buildingFloor"]).one()

        rooms = [r.serialize() for r in floor._rooms]

    return jsonify(rooms)

@bp.route('/getBuildingFloors', methods=['POST'])
def getBuildingFloors():

    if request.method == 'POST':
        data = request.get_json()

        building = global_s.dbConnection.session.query(Building).filter(Building._id == data["buildingId"]).one()

        floors = [f.serialize() for f in building.floors]

    return jsonify(floors)

@bp.route('/getBuildings', methods=['POST'])
def getBuildings():

    if request.method == 'POST':

        buildings = global_s.dbConnection.session.query(Building).all()

        bldgs = [b.serialize() for b in buildings]

    return jsonify(bldgs)