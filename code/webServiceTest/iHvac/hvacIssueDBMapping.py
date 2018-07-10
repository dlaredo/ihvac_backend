from sqlalchemy import Column, Integer, String, Table, DateTime, Float, Boolean, ForeignKey, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, class_mapper

Base = declarative_base()

class HVACIssue(Base):
	"""Class to map to the HVACIssue table in the HVAC DB"""

	__tablename__ = 'HVACIssue'

	_id = Column('IssueId', Integer, primary_key = True, autoincrement = True)
	_buildingLocation = Column('BuildingLocation', String(255))
	_floorLocation = Column('FloorLocation', Integer)
	_roomLocation = Column('RoomLocation', String(255))
	_description = Column('Description', String(255))

	#Constructor

	def __init__(self, buildingLocation, floorLocation, roomLocation, description):

		#self._id = identifier
		self._buildingLocation = buildingLocation
		self._floorLocation = floorLocation
		self._roomLocation = roomLocation
		self._description = description

	#Properties

	@property
	def id(self):
		return self._id

	@id.setter
	def id(self, value):
		self._id = value

	@property
	def buildingLocation(self):
		return self._buildingLocation

	@buildingLocation.setter
	def buildingLocation(self, value):
		self._buildingLocation = value

	@property
	def floorLocation(self):
		return self._floorLocation

	@floorLocation.setter
	def floorLocation(self, value):
		self._floorLocation = value

	@property
	def roomLocation(self):
		return self._roomLocation

	@roomLocation.setter
	def roomLocation(self, value):
		self._roomLocation = value

	@property
	def description(self):
		return self._description

	@description.setter
	def description(self, value):
		self._description = value

	def __str__(self):
		return "<HvacIssue(id = '%s', buildingLocation = '%s', floorLocation = '%s', roomLocation = '%s', description = '%s')>" \
		% (self._id, self._buildingLocation, self._floorLocation, self._roomLocation, self._description)


class Building(Base):
	"""Class to map to the HVACIssue table in the HVAC DB"""

	__tablename__ = 'Building'

	_id = Column('Id', Integer, primary_key = True)
	_buildingName = Column('BuildingName', String(255))

	#Relationships
	_floors = relationship('Floor', back_populates = '_building') #Floors and Building

	#Constructor

	def __init__(self, identifier, buildingName):

		self._id = identifier
		self._buildingName = buildingName

	#Properties

	@property
	def id(self):
		return self._id

	@id.setter
	def id(self, value):
		self._id = value

	@property
	def buildingName(self):
		return self._buildingName

	@buildingName.setter
	def buildingName(self, value):
		self._buildingName = value

	@property
	def floors(self):
		return self._floors

	@floors.setter
	def floors(self, value):
		self._floors = value

	#Methods

	def __str__(self):
		return "<Building(id = '%s', buildingName = '%s')>" \
		% (self._id, self._buildingName)

	def serialize(self):
		return{
			'buildingId':self._id,
			'buildingName':self._buildingName
		}


class Floor(Base):
	"""Class to map to the HVACIssue table in the HVAC DB"""

	__tablename__ = 'Floor'

	_buildingId = Column('BuildingId', Integer, ForeignKey("Building.Id"), primary_key = True)
	_floorNumber = Column('FloorNumber', Integer, primary_key = True)
	_floorName = Column('FloorName', String(255))

	#Relationships
	_building = relationship('Building', back_populates = '_floors') #Floors and Building
	_rooms = relationship('Room', back_populates = '_floor') #Floor and Rooms

	#Constructor

	def __init__(self, buildingId, floorNumber, floorName):

		self._buildingId = buildingId
		self._floorNumber = floorNumber
		self._floorName = floorName

	#Properties

	@property
	def buildingId(self):
		return self._buildingId

	@buildingId.setter
	def buildingId(self, value):
		self._buildingId = value

	@property
	def floorNumber(self):
		return self._floorNumber

	@floorNumber.setter
	def floorNumber(self, value):
		self._floorNumber = value

	@property
	def floorName(self):
		return self._floorName

	@floorName.setter
	def floorName(self, value):
		self._floorName = value

	#Methods

	def __str__(self):
		return "<Floor(buildingId = '%s', floorNumber = '%s', floorName = '%s')>" \
		% (self._buildingId, self._floorNumber, self._floorName)

	def serialize(self):
		return{
			'buildingId':self._buildingId,
			'floorNumber':self._floorNumber,
			'floorName':self._floorName,
		}


class Room(Base):
	"""Class to map to the HVACIssue table in the HVAC DB"""

	__tablename__ = 'Room'

	_buildingId = Column('BuildingId', Integer, primary_key = True)
	_floorNumber = Column('FloorNumber', Integer, primary_key = True)
	_roomId = Column('RoomId', Integer, primary_key = True)
	_roomName = Column('RoomName', String(255))

	#Foreign key
	__table_args__ = (ForeignKeyConstraint(['BuildingId', 'FloorNumber'], ['Floor.BuildingId', 'Floor.FloorNumber'], onupdate="CASCADE", ondelete="CASCADE"), {})

	#Relationships
	_floor = relationship('Floor', back_populates = '_rooms') #Rooms and Floor

	#Constructor

	def __init__(self, buildingId, floorNumber, roomId, roomName):

		self._buildingId = buildingId
		self._floorNumber = floorNumber
		self._roomId = roomId
		self._roomName = roomName

	#Properties

	@property
	def buildingId(self):
		return self._buildingId

	@buildingId.setter
	def buildingId(self, value):
		self._buildingId = value

	@property
	def floorNumber(self):
		return self._floorNumber

	@floorNumber.setter
	def floorNumber(self, value):
		self._floorNumber = value

	@property
	def roomId(self):
		return self._roomId

	@roomId.setter
	def roomId(self, value):
		self._roomId = value

	@property
	def roomName(self):
		return self._floorName

	@roomName.setter
	def roomName(self, value):
		self._floorName = value

	#Methods

	def __str__(self):
		return "<Room(buildingId = '%s', floorNumber = '%s', roomId = '%s', roomName = '%s')>" \
		% (self._buildingId, self._floorNumber, self._roomId, self._roomName)

	def serialize(self):
		return{
			'buildingId':self._buildingId,
			'floorNumber':self._floorNumber,
			'roomId':self._roomId,
			'roomName':self._roomName
		}





