angular.module("myapp")

.controller("AdminExceptionController", function($scope,mainService,$state) {

  $scope.getExceptions = function(){
    mainService.getExceptions(function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $scope.exceptions = JSON.parse(response.data.specData);
        $scope.userRprtExcept = response.data.userSpecData;
      }
      else {
        $state.go("error");
      }
    });
  };

  $scope.newExceptions = [{id: 'excpetion1'}];
  $scope.addRow = function(){
    var newItemNo = $scope.newExceptions.length+1;
    $scope.newExceptions.push({'id':'excpetion'+newItemNo});
  };

  $scope.removeException = function(index,itemSelected){
    console.log(JSON.stringify(itemSelected));
    $scope.exceptions.splice(index,1);
  };

  $scope.removeNewException = function(index){
    $scope.newExceptions.splice(index,1);
  };

  $scope.mergeRow = function(index){
    $scope.exceptions.push($scope.userRprtExcept[index]);
    $scope.userRprtExcept.splice(index,1);
  };

  $scope.setExceptions = function(){
    for (var i = 0; i < $scope.newExceptions.length; i++) {
      if ($scope.newExceptions[i].specialty_name_in_sot && $scope.newExceptions[i].specialty_name_in_taxonomy_grid && $scope.newExceptions[i].degree) {
        var obj = {
          "specialty_name_in_sot": $scope.newExceptions[i].specialty_name_in_sot,
          "specialty_name_in_taxonomy_grid": $scope.newExceptions[i].specialty_name_in_taxonomy_grid,
          "degree": $scope.newExceptions[i].degree
        };
        // obj[$scope.newExceptions[i].sot] = $scope.newExceptions[i].ndb;
        $scope.exceptions.push(obj);
      }
    }
    console.log(JSON.stringify($scope.exceptions));
    mainService.setAdminExceptions($scope.exceptions,function(response){
      if (response.status >= 200 && response.status <= 299) {
        console.log("Set Exceptions:",response.data);
        swal("Good job!", "Exception Added Successfully!", "success")
        $scope.getExceptions();
        $scope.newExceptions = [{id: 'excpetion1'}];
      }
      else{
        $state.go("error");
      }
    });
  };
});
