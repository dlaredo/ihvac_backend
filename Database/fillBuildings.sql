use HVACIssues2018_01;

#Fill Buildings
#insert into Building (Id, BuildingName) values (1, 'SE 2');

#Fill Floors
#insert into Floor (BuildingId, FloorNumber, FloorName) values (1, 0, 'Basement');

#Fill Rooms
insert into Room (BuildingId, FloorNumber, RoomId, RoomName) values (1, 0, 1, 'Room 1A');
insert into Room (BuildingId, FloorNumber, RoomId, RoomName) values (1, 0, 2, 'Room 2A');
insert into Room (BuildingId, FloorNumber, RoomId, RoomName) values (1, 0, 3, 'Room 3A');
insert into Room (BuildingId, FloorNumber, RoomId, RoomName) values (1, 0, 4, 'Room 4A');
insert into Room (BuildingId, FloorNumber, RoomId, RoomName) values (1, 0, 5, 'Room 5A');
