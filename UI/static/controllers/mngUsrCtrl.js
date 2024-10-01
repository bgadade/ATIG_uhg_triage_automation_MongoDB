angular.module("myapp")

.controller("MngUsrController", function($scope,mainService,$state,encryptDecryptService) {
    $scope.usersList = [];

     $scope.validateLen = {
                    name: {
                         required: true,
                         minlength: 5,
                         maxlength: 25
                    }
               }

    $scope.getUserList = function(){
      mainService.getJSON("credentials.json",function(response){
        $scope.newUsersList = [];
        if (response.status >= 200 && response.status <= 299)
        {
          decryptUserData = encryptDecryptService.decrypt(response.data,encryptDecryptService.key);
          credJson = JSON.parse(decryptUserData);
          $scope.keys = Object.keys(credJson);
          $scope.usersList = credJson;

          Object.keys($scope.usersList).forEach(function(key) {
                $scope.newUsersList.push($scope.usersList[key]);
           });
        }
        else {
          $state.go("error");
        }
      });
    }

    $scope.addNewUser = function(){
      console.log($scope.user);
      $scope.user.role = "user";
      encryptUserData = encryptDecryptService.encrypt(JSON.stringify($scope.user),encryptDecryptService.key);
      mainService.addNewUser(encryptUserData,function(response) {
        if (response.status >= 200 && response.status <= 299)
        {
          $scope.user = {};
          $scope.confirmPassword=null;
          console.log("user saved successfully:",response);
          $('#setUserModal').modal('show')
        }
        else{
          $state.go("error");
        }
      });
    };

    $scope.removeUser = function(index){
      $scope.newUsersList.splice(index,1);
    };

      $scope.unlockUser = function(index){
      $scope.newUsersList[index].accountStatus = "Unlocked";
      $scope.newUsersList[index].failedAttemptCount = 0;
    };

    $scope.setUsersList = function(){
       $scope.modifiedUsersObject = {}
       for(var i=0; i < $scope.newUsersList.length; i++) {
       console.log($scope.newUsersList[i]);
       $scope.modifiedUsersObject[$scope.newUsersList[i].userName] = $scope.newUsersList[i];
       }
      $scope.modifiedUsersObject = angular.toJson($scope.modifiedUsersObject);
      mainService.setDataList($scope.modifiedUsersObject,"credentials.json",function(response){
        if (response.status >= 200 && response.status <= 299) {
          console.log("Set Users:",response.data);
          $('#setUserModal').modal('show')
          $scope.getUserList();
        }
        else{
          $state.go("error");
        }
      });
    };

});
