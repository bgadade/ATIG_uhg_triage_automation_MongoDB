angular.module("myapp")

.controller("MappingController", function($scope,mainService,$stateParams,$state,$uibModal,$rootScope) {
  $scope.myStyle={};
  $scope.sheetName =  $stateParams.sheet,
  $scope.selectedInObject = {};
  $scope.selectedOutObject = {};
  $scope.selectedSugObject = {};
  $scope.mappingJson =[];
  $('#mydiv').show();

$scope.getFields = function(){
  $scope.headerValues = mainService.getHeaders().headervalues;
  if ($scope.headerValues['delegateStatus'] == 'existing') {

  }
  mainService.getMappingFields($scope.sheetName,function(response){
    if (response.status >= 200 && response.status <= 299)
    {
      $scope.mappingJson.push(response.svMap);
      $('#mydiv').hide();
      // for (var i = 0; i < $scope.mappingJson[0].unmapOtFields.length; i++) {
      //   if ($scope.mappingJson[0].unmapOtFields[i].bestMatch != null && $scope.mappingJson[0].unmapOtFields[i].bestMatch != undefined) {
      //     $scope.mappingJson[0].unmapOtFields[i].original_map = 'nlp';
      //   }
      // }
    }
    else {
      $state.go("error");
    }
  });
};

$scope.countUnmapOtpt = function(){
  $scope.unmappedCount = $scope.mappingJson[0].unmapOtFields.length;
};

$scope.clearMappings = function(){
  mainService.clearMappings($scope.sheetName);
}

$scope.setMappingFields = function(){
    mainService.userCommittedSvMappings($scope.mappingJson,$scope.sheetName);

    var mappedFields = {};
    for(var i=0;i<$scope.mappingJson[0].mappedOtFields.length;i++){
      mappedFields[$scope.mappingJson[0].mappedOtFields[i].inputField]=$scope.mappingJson[0].mappedOtFields[i].outputField;
    }
    console.log(mappedFields);
    mainService.transformMappings(mappedFields,$scope.sheetName);
    $('#commitModal').on('hidden.bs.modal', function (e) {
      $state.go('mvMap',{'sheet':$scope.sheetName});
    })
};

$scope.selectInItem = function (item) {
    if ($scope.selectedInObject == item) //reference equality should be sufficient
        $scope.selectedInObject = {}; //de-select if the same object was re-clicked
    else
        $scope.selectedInObject = item;
};
$scope.selectOutItem = function (item) {
    if ($scope.selectedOutObject == item) //reference equality should be sufficient
        $scope.selectedOutObject = {}; //de-select if the same object was re-clicked
    else
        $scope.selectedOutObject = item;
};

$scope.selectSugItem = function(item){
  if ($scope.selectedSugObject == item) //reference equality should be sufficient
      $scope.selectedSugObject = {}; //de-select if the same object was re-clicked
  else
      $scope.selectedSugObject = item;
}

$scope.createMap = function(){
  if(Object.keys($scope.selectedOutObject).length!=0 && Object.keys($scope.selectedInObject).length!=0){
    if($scope.selectedInObject=='NA'){
      $scope.mappingJson[0].mappedOtFields.push({"inputField":$scope.selectedInObject,"outputField":$scope.selectedOutObject.outputField});
      $scope.selectedInObject = {};
    }
    else{
      $scope.mappingJson[0].mappedOtFields.push({"inputField":$scope.selectedInObject.inputField,"outputField":$scope.selectedOutObject.outputField});
      for(var i=0; i<$scope.mappingJson[0].inFields.length;i++){
        if($scope.mappingJson[0].inFields[i].inputField == $scope.selectedInObject.inputField){
          $scope.mappingJson[0].inFields[i].mapped=true;
        }
      }
    }
    for(var i=0; i<$scope.mappingJson[0].unmapOtFields.length;i++){
      if($scope.mappingJson[0].unmapOtFields[i].outputField == $scope.selectedOutObject.outputField){
        $scope.mappingJson[0].unmapOtFields.splice(i,1);
      }
    }
    $scope.selectedOutObject = {};
    $scope.selectedInObject = {};
  }
  else{
    $scope.selectedInObject = {};
  }
}

$scope.sugCreateMap = function(item){
  $scope.mappingJson[0].mappedOtFields.push({"inputField":item.bestMatch,"outputField":item.outputField});
  for(var i=0; i<$scope.mappingJson[0].unmapOtFields.length;i++){
    if($scope.mappingJson[0].unmapOtFields[i].outputField == item.outputField){
      $scope.mappingJson[0].unmapOtFields.splice(i,1);
    }
  }
  for(var i=0; i<$scope.mappingJson[0].inFields.length;i++){
    if($scope.mappingJson[0].inFields[i].inputField == item.bestMatch){
      $scope.mappingJson[0].inFields[i].mapped=true;
    }
  }
}

$scope.rejectSugMap = function(item){
  for(var i=0; i<$scope.mappingJson[0].inFields.length;i++){
    if($scope.mappingJson[0].inFields[i].inputField == item.bestMatch){
      $scope.mappingJson[0].inFields[i].mapped=false;
    }
  }
  for(var i=0; i<$scope.mappingJson[0].unmapOtFields.length; i++){
    if($scope.mappingJson[0].unmapOtFields[i].outputField == item.outputField){
      $scope.mappingJson[0].unmapOtFields[i].bestMatch ="";
    }
  }
}

$scope.rejectMap = function(item){
  for(var i=0; i<$scope.mappingJson[0].mappedOtFields.length;i++){
    if($scope.mappingJson[0].mappedOtFields[i].outputField == item.outputField  && $scope.mappingJson[0].mappedOtFields[i].inputField == item.inputField){
      $scope.mappingJson[0].mappedOtFields.splice(i,1);
    }
  }
  $scope.mappingJson[0].unmapOtFields.push({"outputField":item.outputField,"bestMatch":""});
  for(var i=0; i<$scope.mappingJson[0].inFields.length;i++){
    if($scope.mappingJson[0].inFields[i].inputField == item.inputField){
      $scope.mappingJson[0].inFields[i].mapped=false;
    }
  }
  $scope.selectedInObject = {};
  $scope.selectedOutObject = {};
}

});
