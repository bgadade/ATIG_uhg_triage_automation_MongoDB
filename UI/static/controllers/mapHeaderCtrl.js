angular.module("myapp")

.controller("MappingHeaderController", function($scope,mainService,authService,$state,$stateParams,$rootScope) {
    $('#mydiv').hide();
    $scope.userName = authService.getUser().name;
    $scope.map = $stateParams.map;
    $scope.mappingStatus = {};
    $scope.mapBtn = {};
    $scope.onTabSelected = function(tabId) {
    var obj = {
        mapBtn : $scope.mapBtn,
        mappingStatus: $scope.mappingStatus
      }
      mainService.setIntermMapJson(tabId,obj);
    };

    $scope.getSheets = function(){
      var headers = mainService.getHeaders();
      $scope.headerValues = headers.headervalues;
      $scope.mappingStatus  = mainService.getMappingHeaders().mappingStatus;
      $scope.mapBtn = mainService.getMappingHeaders().mapBtn;
      if ($scope.mappingStatus == undefined) {
        $scope.mappingStatus = {};
        $scope.mapBtn = {};
      }
      $scope.mapData = headers.mappingData;

      if ($scope.map == "map") {
        for (var sheet in $scope.mapData) {
          if ($scope.mapData[sheet].matchPer != 0) {
            mainService.transformMappings($scope.mapData[sheet].mappings.sv,sheet);
            mainService.transformMvMappings($scope.mapData[sheet].mappings.mv,sheet);
            $scope.mappingStatus[sheet] = "Process";
            $scope.mapBtn[sheet] = {
              "sv":$scope.mapData[sheet].mappings.sv,
              "mv":$scope.mapData[sheet].mappings.mv
            };
          }
          else{
            $scope.mappingStatus[sheet] = "Process";
          }
        }
      }
    }

    $scope.checkMap = function(e){
      var flag = true;
      $scope.transformMappings = mainService.getTransformMapping();
      if ($scope.transformMappings != undefined) {
        for(var key in $scope.mappingStatus){
          if($scope.mappingStatus[key] == 'Process'){
            flag = true;
            if ($scope.mapData[key].matchPer == 'Customised' || $scope.mapData[key].matchPer == 100) {
              flag = false;
            }
            else{
              flag = true;
              break;
            }
          }
        }
      }
      return flag;
    }

    $scope.setMvMappingFields = function(){
        $('#mydiv').show();
        var sheets = [];
        for (var key in $scope.mappingStatus) {
          if ($scope.mappingStatus[key] == "Ignore") {
            $scope.transformMappings[key] = "to be ignored";
            $scope.mapData[key]['process'] = false;
            $scope.mapData[key]['mappings'] = $scope.transformMappings[key];
          }
          else{
            $scope.mapData[key]['process'] = true;
            $scope.mapData[key]['mappings'] = $scope.transformMappings[key];
          }
        }
        mainService.setMvMappingFields($scope.userName,$scope.mapData,function(response){
          if(response == 'success'){
            console.log(" setting 1:M mapping Fields:",response);
            $state.go("transType");
          }
          else{
            $state.go("error");
          }
        });
    };
    $scope.sizeOf = function(obj) {
    return Object.keys(obj).length;
  };
  $scope.viewMapping = function(sheetMap){
    mainService.viewMapping(sheetMap,function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        console.log("View Mapping:",response.data);
        $scope.providerMappingList = response.data;
      }
      else {
        $state.go("error");
      }
    });
  }
});
