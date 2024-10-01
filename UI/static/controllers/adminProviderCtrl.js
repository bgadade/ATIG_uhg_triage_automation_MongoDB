angular.module("myapp")

.controller("AdminProviderController", function($scope,mainService,$state) {

  $scope.userProv = {}
  $('#mydiv').hide();

  $scope.getProviderMap = function(){
    mainService.getAdminProvMap(function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $scope.providerMap = response.data;
        console.log(JSON.stringify($scope.providerMap));
      }
      else {
        $state.go("error");
      }
    });
  }

  $scope.activeUserMap = null;
  $scope.setActiveProvData = function(userMap,provider){
    $scope.activeProvider = provider;
    $scope.activeUserMap = userMap;
    $scope.activeNewUserMap = null;
  }

  $scope.activeNewUserMap = null;
  $scope.setActiveNewProvData = function(userMap,provider){
    $scope.activeProvider = provider;
    $scope.activeNewUserMap = userMap;
    $scope.activeUserMap = null;
  }

  $scope.isEmpty = function(userMap){
    return Object.keys(userMap).length !=0
  }

  $scope.setProvUser = function(){
    console.log($scope.userProv);
    $('#mydiv').show();
    mainService.setProvUserMap($scope.userProv,function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $('#mydiv').hide();
        console.log("Set Provider Mappings:",response.data);
        $scope.getProviderMap();
        $scope.activeUserMap = null;
        $scope.activeNewUserMap = null;
        $scope.activeProvider = null;
        swal("Good job!", "Provider Mappings saved Successfully!", "success")
      }
      else {
        $state.go("error");
      }
    });
  };

  $scope.clearProvUser = function(provider){
    $scope.userProv[provider] = "none"
  }

  $scope.setIgnoreStatus = function(provider){
    swal("Ignore all User Mappings For", provider)
    $scope.userProv[provider] = "ignore"
  }
})
.filter('typeof', function() {
  return function(obj, fltr) {
    if(fltr){
      return typeof obj===fltr
    }
    return typeof obj
  };
});
