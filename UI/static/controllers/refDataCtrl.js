angular.module("myapp")

.controller("RefDataController", function($scope,mainService,$state) {
$scope.dataList = [];
  $scope.getDataList = function(filename){
    $scope.filename = filename;
    mainService.getCsvToJson(filename,function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $scope.dataList = JSON.parse(JSON.parse(response.data));
      }
      else {
        $state.go("error");
      }
    });
  }

  $scope.newDataList = [{id: 'data1'}];
  $scope.addRow = function(){
    var newItemNo = $scope.newDataList.length+1;
    $scope.newDataList.push({'id':'data'+newItemNo});
  };

  $scope.removeData = function(index){
    $scope.dataList.splice(index,1);
  };

  $scope.removeNewData = function(index){
    $scope.newDataList.splice(index,1);
  };

  $scope.getJsonToCsv = function(data,filename,col){
    mainService.getJsonToCsv(data,filename,col,function(response){
      if (response.status >= 200 && response.status <= 299) {
        console.log("Set Reference Data:",response.data);
        swal("Good job!", "Reference Data saved Successfully!", "success")
        // $('#setDataModal').modal('show')
        $scope.getDataList($scope.filename);
        $scope.newDataList = [{id: 'data1'}];
      }
      else{
        $state.go("error");
      }
    });
  };

  $scope.setDegreeExcep = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].degree_in_sot && $scope.newDataList[i].degree_in_master_list) {
        var obj = {
          "degree_in_sot": $scope.newDataList[i].degree_in_sot,
          "degree_in_master_list": ($scope.newDataList[i].degree_in_master_list).toUpperCase()
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["degree_in_sot","degree_in_master_list"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };

  $scope.setDegreeList = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].code && $scope.newDataList[i].description  && $scope.newDataList[i].is_mid_level) {
        var obj = {
          "code": ($scope.newDataList[i].code).toUpperCase(),
          "description": ($scope.newDataList[i].description).toUpperCase(),
          "is_mid_level": $scope.newDataList[i].is_mid_level
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["code","description","is_mid_level"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };

  $scope.setSpcList = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].spec && $scope.newDataList[i].is_mid_level) {
        var obj = {
          "spec": ($scope.newDataList[i].spec).toUpperCase(),
          "is_mid_level": $scope.newDataList[i].is_mid_level
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["spec","is_mid_level"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };

  $scope.setLangCodeList = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].code && $scope.newDataList[i].language_description) {
        var obj = {
          "code": $scope.newDataList[i].code,
          "language_description": $scope.newDataList[i].language_description
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["code","language_description"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };

  $scope.setUsStateList = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].us_state && $scope.newDataList[i].abbreviation) {
        var obj = {
          "us_state": $scope.newDataList[i].us_state,
          "abbreviation": ($scope.newDataList[i].abbreviation).toUpperCase()
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["us_state","abbreviation"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };

  $scope.setHspAffList = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].acronym && $scope.newDataList[i].abb) {
        var obj = {
          "acronym": ($scope.newDataList[i].acronym).toUpperCase(),
          "abb": $scope.newDataList[i].abb
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["acronym","abb"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };

  $scope.setPCPVsSpecList = function(){
    for (var i = 0; i < $scope.newDataList.length; i++) {
      if ($scope.newDataList[i].specialty_name && $scope.newDataList[i].ndb_specialty_code && $scope.newDataList[i].cosmos_provider_number) {
        var obj = {
          "specialty_name": $scope.newDataList[i].specialty_name,
          "ndb_specialty_code": $scope.newDataList[i].ndb_specialty_code,
          "cosmos_provider_number": $scope.newDataList[i].cosmos_provider_number
        };
        $scope.dataList.push(obj);
      }
    }
    var cols = ["specialty_name","ndb_specialty_code","cosmos_provider_number"]
    $scope.getJsonToCsv($scope.dataList,$scope.filename,cols);
  };
});
