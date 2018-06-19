create database HVACIssues2018_01;
use HVACIssues2018_01;

CREATE TABLE HVACIssue (
  IssueId   int(10) NOT NULL AUTO_INCREMENT, 
  BuildingLocation varchar(255) NOT NULL, 
  FloorLocation  int(10) NOT NULL, 
  RoomLocation   varchar(255) NOT NULL, 
  Description	varchar(255) NOT NULL,
  PRIMARY KEY (IssueId)) ENGINE=InnoDB;
  
CREATE TABLE Building (
  Id   int(10) NOT NULL AUTO_INCREMENT, 
  BuildingName varchar(255) NOT NULL UNIQUE, 
  PRIMARY KEY (Id)) ENGINE=InnoDB;
  
CREATE TABLE Floor (
  BuildingId   int(10) NOT NULL, 
  FloorNumber int(10) NOT NULL, 
  FloorName varchar(255) NOT NULL UNIQUE, 
  PRIMARY KEY (BuildingId, FloorNumber)) ENGINE=InnoDB;
  
CREATE TABLE Room (
  BuildingId   int(10) NOT NULL, 
  FloorNumber int(10) NOT NULL,
  RoomId int(10) NOT NULL,
  RoomName varchar(255) NOT NULL UNIQUE, 
  PRIMARY KEY (BuildingId, FloorNumber, RoomId)) ENGINE=InnoDB;
  
ALTER TABLE Floor ADD INDEX FKFloor_Building (BuildingId), ADD CONSTRAINT FKFloor_Building FOREIGN KEY (BuildingId) REFERENCES Building (Id);
ALTER TABLE Room ADD INDEX FKRoom_Floor (BuildingId, FloorNumber), ADD CONSTRAINT FKRoom_Floor FOREIGN KEY (BuildingId, FloorNumber) REFERENCES Floor (BuildingId, FloorNumber);