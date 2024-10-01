angular.module("myapp")

.controller("SvAdminMapController", function($scope,mainService,$state) {
  $scope.singleValueMap = [];

  $scope.getSvAdminMap = function(){
    mainService.getSvAdminMap(function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $scope.singleValueMap.push(response.data);
      }
      else {
        $state.go("error");
      }
    });
  };

  $scope.rmSvOutField = function(outFieldSelected,inFieldSelected){
    for(var j=0;j<$scope.singleValueMap[0][outFieldSelected].Input_Column.length;j++){
      if($scope.singleValueMap[0][outFieldSelected].Input_Column[j]==inFieldSelected){
        $scope.singleValueMap[0][outFieldSelected].Input_Column.splice(j,1);
      }
    }
  }

  $scope.setSvAdminMap = function(){
    if($scope.singleValueMap.length >= 1 && $scope.singleValueMap[0] === undefined ){
        $scope.singleValueMap[0] = {};
    }
    mainService.setSvAdminMap($scope.singleValueMap[0],function(response){
      if (response.status >= 200 && response.status <= 299) {
        console.log("Set SinglValue Admin Mapping:",response.data);
        // $('#setSvMapModal').on('hidden.bs.modal', function (e) {
        //   $state.go('mvAdminMap');
        // })
        $scope.getSvAdminMap();
      }
      else{
        $state.go("error");
      }
    });
  };

});
