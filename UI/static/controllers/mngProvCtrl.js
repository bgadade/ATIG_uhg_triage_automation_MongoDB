angular.module("myapp")

.controller("ProviderController", function($scope,mainService,$state,encryptDecryptService) {
    $scope.getProviderList = function(){
      mainService.getJSON("providers.json",function(response){
        if (response.status >= 200 && response.status <= 299)
        {
          $scope.decProList = encryptDecryptService.decrypt(response.data,encryptDecryptService.key)
          $scope.providerList = JSON.parse($scope.decProList);
          $scope.copyproviderList=angular.copy($scope.providerList);
        }
        else {
          $state.go("error");
        }
      });
    }
    // $scope.abcd = null;
    $scope.newProviders = [{id: 'provider1'}];
    $scope.addProviders = function(){
      var newItemNo = $scope.newProviders.length+1;
      $scope.newProviders.push({'id':'provider'+newItemNo});
    };

    $scope.removeProvider = function(key){
      $scope.providerList[key].status = "deleted";
    };

    $scope.changeStatus = function(key){
      $scope.providerList[key] = $scope.copyproviderList[key];
    };

    $scope.removeNewProvider = function(index){
      $scope.newProviders.splice(index,1);
    };
    $scope.disableFlag=null;
    $scope.setProvidersList = function(){
      for (var i = 0; i < $scope.newProviders.length; i++) {
        if ($scope.newProviders[i].provider) {
          if (!$scope.providerList[$scope.newProviders[i].provider]) {
            $scope.providerList[(($scope.newProviders[i].provider).toUpperCase())] = {
              "status": "created",
              "name": ($scope.newProviders[i].name).toUpperCase()
            };
          }
        }
      }
        console.log(JSON.stringify($scope.providerList));
        mainService.setProviderList($scope.providerList,function(response) {
          if (response.status >= 200 && response.status <= 299)
          {
            $scope.newProviders = [{id: 'provider1'}];
            $scope.getProviderList();
            console.log("Providers saved successfully:",response);
            $('#setProviderModal').modal('show')
          }
          else{
            $state.go("error");
          }
        });
    };
});
