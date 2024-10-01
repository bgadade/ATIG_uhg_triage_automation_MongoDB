angular.module("myapp")

.controller("LoginController", function($scope,$state,authService,$window,Idle) {
    $scope.signIn=function(){
      $scope.selectedIndex = 0;
    }
    $scope.success="true";
//   if ($window.sessionStorage['member-user'] == undefined) {
//   }
//  else{
//     if (JSON.parse(window.sessionStorage['member-user'])["role"] == "admin") {
//      $state.go("adminMap");     }
//    else if(JSON.parse(window.sessionStorage['member-user'])["role"] == "user"){
//       $state.go("home");
//    }
// }
    $scope.login = function() {
      $scope.error = "";
        authService.signIn($scope.user).then(function(user) {
          if(user.role=="admin"){
            sessionStorage.setItem("role", "admin");
            $state.go("adminMap");
          }
          else if(user.role=="user"){
          sessionStorage.setItem("role", "user");
            $state.go("home");
          }
          else
          {
            $scope.error=user.role;
          }
        }, function(err) {
          console.log("error:",err);
          $scope.error = err.error;
        });

    }
    $scope.loginStart = function(){
      Idle.unwatch();
    }
});
