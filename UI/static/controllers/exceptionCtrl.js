angular.module("myapp")

.controller("ExceptionController", function($scope,mainService,authService,$state) {
  $scope.userName=authService.getUser().name;

  $scope.excpetions = [{id: 'excpetion1'}];

  $scope.addNewException = function() {
    var newItemNo = $scope.excpetions.length+1;
    $scope.excpetions.push({'id':'excpetion'+newItemNo});
  };

  $scope.removeException = function(index) {
    $scope.excpetions.splice(index,1);
  };

  $scope.sbmtExcept = function() {
      var finalExcept = [];
      for (var i = 0; i < $scope.excpetions.length; i++) {
        if ($scope.excpetions[i].specialty_name_in_sot && $scope.excpetions[i].specialty_name_in_taxonomy_grid && $scope.excpetions[i].degree) {
          var newObj  = {
            "specialty_name_in_sot": $scope.excpetions[i].specialty_name_in_sot,
            "specialty_name_in_taxonomy_grid": $scope.excpetions[i].specialty_name_in_taxonomy_grid,
            "specialty_code": null,
            "degree": $scope.excpetions[i].degree
          };
          finalExcept.push(newObj);
        }
      }
      mainService.setException(finalExcept,$scope.userName,function(response){
        if (response.status >= 200 && response.status <= 299){
          $scope.excpetions = [{id: 'excpetion1'}];
          swal("Good job!", "Exception Raised Successfully!", "success")
        }
        else {
          $state.go("error");
        }
      });
  }
});
