angular.module("myapp")

.controller("AdminMapController", function($scope,mainService,$state,Idle,$rootScope,$window,authService) {
  $('#mydiv').hide();

  $scope.singleValueMap=[];
  $scope.multiValueMap=[];
  $scope.totalCount = null;
  $scope.rowFlag = false;

  $scope.getAdminMap = function(){
  $('#mydiv').show();
    mainService.getAdminMap(function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $('#mydiv').hide();
        if(Object.keys(response.data.unMapData).length === 0){
          $scope.singleValueMap = [];
          $scope.singleValueMap[0] ={};
          $scope.multiValueMap = [];
          $scope.multiValueMap[0] ={};
          userMapIds = mainService.getSavedUserMapIds();
          $scope.rowFlag = false;
          if(userMapIds.length===0){
            swal("There is no data to fetch")
          }else{
               $('#continueModal').modal('show')
                $scope.totalCount = mainService.getTotalCount();
          }

        }
        else{
            $scope.rowFlag = true;
            $scope.singleValueMap.push(response.data.unMapData.sv);
            $scope.multiValueMap.push(response.data.unMapData.mv);
             if($scope.singleValueMap.length>1){
               $scope.singleValueMap.splice(0, 1);
            }
             if($scope.multiValueMap.length>1){
               $scope.multiValueMap.splice(0, 1);
            }
            for (var parent in $scope.multiValueMap[0]) {
                for(var outField in $scope.multiValueMap[0][parent]){
                    for(var tagField in $scope.multiValueMap[0][parent][outField]){
                        if (tagField != 'tags') {
                            var obj = [];
                            obj = $scope.multiValueMap[0][parent][outField][tagField];
                            $scope.multiValueMap[0][parent][outField][tagField] = [];

                    for(var i=0;i<obj.length;i++){
                        var newObj = {};
                        newObj[tagField] = obj[i];
                        $scope.multiValueMap[0][parent][outField][tagField].push(newObj);
                  }
                }
              }
            }
          }
        }
      }
      else {
        $state.go("error");
      }
    });
  };

  $scope.setAdminMap = function(){
    $('#mydiv').show();

    if($scope.singleValueMap.length >= 1 && $scope.singleValueMap[0] === undefined ){
        $scope.singleValueMap[0] ={};
    }
    if($scope.multiValueMap.length >= 1 && $scope.multiValueMap[0] === undefined){
        $scope.multiValueMap[0] ={};
    }
    var adminMap = {
      'sv':$scope.singleValueMap[0],
      'mv':$scope.multiValueMap[0]
    };
    console.log(JSON.stringify($scope.multiValueMap[0]));
    mainService.setAdminMap(adminMap,function(response){
      $('#mydiv').hide();
      if (response.status >= 200 && response.status <= 299) {
          $scope.singleValueMap = [];
          $scope.singleValueMap[0] ={};
          $scope.multiValueMap = [];
          $scope.multiValueMap[0] ={};
          $('#setMapModal').modal('show')
          $scope.rowFlag = false;
      }
      else{
        $state.go("error");
      }
    });
  };

  $scope.editTag = function(inputFields){
    $scope.tagFields=[];
    $scope.tagFields = inputFields;
    // for(var key in inputFields){
    //   $scope.tagFields.push(key);
    // }
  }

  $scope.onEdit = function(typeSelected,inputSelected,objectSelected,newTypeSelected,parentSelected,outputSelected,indexSelected){
    var flag = true;
    for(var i=0;i<$scope.multiValueMap[0][parentSelected][outputSelected][typeSelected].length;i++){
      if($scope.multiValueMap[0][parentSelected][outputSelected][typeSelected][i][typeSelected] == inputSelected){
        var obj = {};
        obj[newTypeSelected] = inputSelected;
        $scope.multiValueMap[0][parentSelected][outputSelected][typeSelected].splice(i,1);
        for (var key in $scope.multiValueMap[0][parentSelected][outputSelected]) {
          if (key == newTypeSelected) {
            flag = false;
          }
        }
        if (flag) {
          $scope.multiValueMap[0][parentSelected][outputSelected][newTypeSelected] = [];
        }
        $scope.multiValueMap[0][parentSelected][outputSelected][newTypeSelected].push(obj);
      }
    }
  }

  $scope.rmSvOutField = function(outFieldSelected,inFieldSelected){
    for(var j=0;j<$scope.singleValueMap[0][outFieldSelected].length;j++){
      if($scope.singleValueMap[0][outFieldSelected][j]==inFieldSelected){
        $scope.singleValueMap[0][outFieldSelected].splice(j,1);
      }
    }
  }

  $scope.rmMvOutField = function(parentSelected,outFieldSelected,tagSelected,inFieldSelected){
    for(var i=0;i<$scope.multiValueMap[0][parentSelected][outFieldSelected][tagSelected].length;i++){
      if($scope.multiValueMap[0][parentSelected][outFieldSelected][tagSelected][i][tagSelected]==inFieldSelected){
        $scope.multiValueMap[0][parentSelected][outFieldSelected][tagSelected].splice(i,1);
      }
    }
  }
    //session timeout
    $scope.start = function() {
        $rootScope.$emit("CallMethod", {});
        Idle.watch();
    }

  $scope.logout = function(){
    authService.signout()
    .then(function(res){
      authService.removeUser();
      $state.go("login");
    },
    function(res){
      $state.go("login");
      // , {error: "Invalid signin attempt, please retry with valid credentials"});
    });
  }

  $scope.getNextMappings = function(){
  $('#mydiv').show();
    mainService.getNextMappings(function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        $scope.rowFlag = false;
        $scope.getAdminMap();
        $('#mydiv').hide();
        $('#continueModal').modal('hide')
      }
      else {
        $state.go("error");
      }
    });
  }
 $window.onbeforeunload =  $scope.logout;
});
