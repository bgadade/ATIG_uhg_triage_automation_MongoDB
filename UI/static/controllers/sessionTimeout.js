angular.module("myapp")

.controller("SessionController",function ($scope, $rootScope, $uibModal, Idle, authService, $state) {
  $rootScope.$on("CallMethod", function(){
         closeModals();
        })
      //  $rootScope.displayPopUp = authService.sessionPopUp;
    function closeModals() {
    if ($scope.warning) {
      $scope.warning.close();
      $scope.warning = null;
    }

    if ($scope.timedout) {
      $scope.timedout.close();
      $scope.timedout = null;
    }
  }

  $scope.$on('IdleStart', function() {
    closeModals();
    $scope.warning = $uibModal.open({
      templateUrl: 'warning-dialog.html'
    });
  });

  $scope.$on('IdleEnd', function() {
    closeModals();
  });
  $scope.$on('IdleTimeout', function() {
    closeModals();
    $scope.timedout = $uibModal.open({
      templateUrl: 'timedout-dialog.html'
    });
     Idle.unwatch();
    authService.signout(function(response){
    if (response.status >= 200 && response.status <= 299){
        $state.go("logout");
    } else{
    state.go("error");
    }
});
    $state.go("login");
  });
})