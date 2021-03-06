from flask import request, current_app, Blueprint, render_template
from sqlalchemy import and_
from flask_sqlalchemy import SQLAlchemy
from iHvac.db import *
from iHvac.hvacIssueDBMapping import *
from . import global_s
from flask import jsonify
from smtplib import SMTP
import datetime

bp = Blueprint('services', __name__, url_prefix='/services')

# a simple page that says hello
@bp.route('/hello', methods=['POST'])
def hello():
    return "hello"

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

@bp.route('/errorMail', methods=['GET'])
def errorMail():

    if request.method == 'GET':

    	message_text = request.args.get('message')
    	subj = request.args.get('subj')
    	#message_text = "Hello\nThis is a mail from your server\n\nBye\n"
    	sendMail(message_text, subj)
    
    return "Mail sent"   	

def sendMail(message_text, subj):

	smtp = SMTP("smtp.gmail.com:587")
	smtp.ehlo()
	smtp.starttls()
	smtp.login("controlslab.uc@gmail.com", "controlslab.uc")

	from_addr = "Controls Lab <controlslab.uc@gmail.com>"
	to_addr = "dlaredorazo@ucmerced.edu"

	#subj = "Critical"
	date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

	#message_text = "Hello\nThis is a mail from your server\n\nBye\n"

	msg = "From %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % (from_addr, to_addr, subj, date, message_text)

	smtp.sendmail(from_addr, to_addr, msg)
	smtp.quit()