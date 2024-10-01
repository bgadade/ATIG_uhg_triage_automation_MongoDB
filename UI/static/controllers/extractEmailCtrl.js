angular.module("myapp")
.controller("ExtractEmailController", function($scope,$stateParams,mainService,$state) {

  $scope.filename = $stateParams.emailFile;
  $scope.attachmentFile = $stateParams.attachmentFile;
  $scope.extractEmail = function(){
    mainService.extractEmail($scope.filename,function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        console.log(response.data);
        $scope.mailData = response.data;
      }
      else {
        $state.go("error");
      }
    });
  }
});
