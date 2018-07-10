var app = angular.module('hvacIssueApp', []);

app.controller('hvacIssueCtrl', function($scope, $http) {
    
    $scope.changeBuildingLocation = function() {

    	var building = {buildingId:$scope.selectedBuilding}
    	
        dataJSON = JSON.stringify(building)

    	$http({
        	method : "POST",
        	url : "http://169.236.181.40/iHvac/services/getBuildingFloors",
        	data : dataJSON
    	}).then(function Sucess(response) {
        	$scope.buildingFloors = response.data;

            $scope.selectedFloor = $scope.buildingFloors[0].floorNumber
            $scope.changeFloorLocation()
    	}, function Error(response) {
        	$scope.myWelcome = response.statusText;
    	});

    };

    $scope.changeFloorLocation = function() {

    	var building_floor = {buildingId:$scope.selectedBuilding, buildingFloor:$scope.selectedFloor}
    	dataJSON = JSON.stringify(building_floor)

    	$http({
        	method : "POST",
        	url : "http://169.236.181.40/iHvac/services/getBuildingRoomsByFloor",
        	data : dataJSON
    	}).then(function Sucess(response) {
    		$scope.buildingRoomsByFloor = response.data;
            $scope.selectedRoom = $scope.buildingRoomsByFloor[0].roomId
    	}, function Error(response) {
        	$scope.myWelcome = response.statusText;
    	});

    };

    $scope.submit = function(){

        var hvacIssue = {buildingId:$scope.selectedBuilding, floorId:$scope.selectedFloor, roomId:$scope.selectedRoom, issueDescription:$scope.description}
        dataJSON = JSON.stringify(hvacIssue)

        $http({
            method : "POST",
            url : "http://169.236.181.40/iHvac/services/reportHvacIssue",
            data : dataJSON
        }).then(function Sucess(response) {
            alert("Your message has been successfully processed")
            $scope.description = "";
        }, function Error(response) {
            $scope.myWelcome = response.statusText;
        });
    } 

    function init() {
        $scope.selectedBuilding = 0;
        $scope.selectedFloor = 0;
        $scope.selectedRoom = 0;
        $scope.buildingFloors = null;
        $scope.buildingRoomsByFloor = null;
        $scope.buildings = null;
        $scope.description = "";

        //Get a list of all buildings
        $http({
            method : "POST",
            url : "http://169.236.181.40/iHvac/services/getBuildings",
            data : null
        }).then(function Sucess(response) {
            $scope.buildings = response.data;
            $scope.selectedBuilding = $scope.buildings[0].buildingId
            $scope.changeBuildingLocation()
        }, function Error(response) {
            $scope.myWelcome = response.statusText;
        });

    }

    init();

});