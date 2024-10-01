angular.module("myapp")

.controller("LogReportController", function($scope,mainService,$state) {
  $('#mydiv').hide();
    $scope.logData = null;
    $scope.selectedUser =null
    $scope.users = []
    $scope.getLogData = function(){
        $('#mydiv').show();
        mainService.getLogData(function(response){
            $('#mydiv').hide();
            $scope.loggedData = JSON.parse(response.data['logData']);
            $scope.logData = angular.copy($scope.loggedData)
            $scope.logData.colSet.splice(6,1)
            for(var i=0;i<$scope.logData.dataSet.length;i++){
                $scope.logData.dataSet[i].splice(6,1)
            }
         $scope.users = response.data['users'];
        });
    }

    $scope.reportLog = false
    $scope.fetchReportLog = function(){
        $scope.reportLog = true
    }

    $scope.username = null
    $scope.start_date = null
    $scope.end_date = null
    $scope.filterLogData = function(selectedUser,start_date,end_date){
        $scope.logData = mainService.getSavedLogData().logData;
        $scope.filterData = {colSet : $scope.logData.colSet,dataSet:[]}
        $scope.filterFlag = false;
        if ($scope.selectedUser=='Select User'){
            $scope.selectUserFlag = false;
        }else{
        $scope.selectUserFlag = true;
    }
    if(start_date){
        start_date = start_date.toLocaleDateString()
        start_date = start_date.replace(/\//g, '-')
        format_date =start_date.split('-');
        start_date = format_date[2]+'-'+format_date[0]+'-'+format_date[1]
        var d1 = new Date(start_date);
    }
    if(end_date){
        end_date = end_date.toLocaleDateString()
        end_date = end_date.replace(/\//g, '-')
        format_date =end_date.split('-');
        end_date = format_date[2]+'-'+format_date[0]+'-'+format_date[1]
        var d2 = new Date(end_date);
    }
    if($scope.selectUserFlag && start_date && end_date && selectedUser){
         for(var i=0;i<$scope.logData.dataSet.length;i++){
            if($scope.selectedUser && (d1 < new Date($scope.logData.dataSet[i][0])) && (d2 > new Date($scope.logData.dataSet[i][0]))){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
         $scope.filterFlag = true;
        }
    }
    else if(!$scope.filterFlag && $scope.selectUserFlag && start_date && selectedUser){
         for(var i=0;i<$scope.logData.dataSet.length;i++){
            if($scope.selectedUser && (d1 < new Date($scope.logData.dataSet[i][0]))){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
         $scope.filterFlag = true;
        }
    }
     else if(!$scope.filterFlag && $scope.selectUserFlag &&  end_date && selectedUser){
         for(var i=0;i<$scope.logData.dataSet.length;i++){
            if($scope.selectedUser && (d2 > new Date($scope.logData.dataSet[i][0]))){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
         $scope.filterFlag = true;
        }
    }
     else if(!$scope.filterFlag  &&  start_date && end_date){
         for(var i=0;i<$scope.logData.dataSet.length;i++){
            if((d1 < new Date($scope.logData.dataSet[i][0])) && (d2 > new Date($scope.logData.dataSet[i][0]))){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
         $scope.filterFlag = true;
         }
    }
    else if(!$scope.filterFlag  && start_date){
         for(var i=0;i<$scope.logData.dataSet.length;i++){
            if(d1 < new Date($scope.logData.dataSet[i][0])){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
         $scope.filterFlag = true;
         }
    }
     else if(!$scope.filterFlag  && end_date){
         for(var i=0;i<$scope.logData.dataSet.length;i++){
            if(d2 < new Date($scope.logData.dataSet[i][0])){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
         $scope.filterFlag = true;
         }
    }
    if(!$scope.filterFlag && selectedUser && $scope.selectUserFlag){
        for(var i=0;i<$scope.logData.dataSet.length;i++){
            if($scope.logData.dataSet[i][6]==selectedUser){
                 $scope.filterData.dataSet.push($scope.logData.dataSet[i])
            }
        $scope.filterFlag = true;
        }
    }
    if($scope.filterFlag){
        $scope.logData = $scope.filterData
        $scope.logData.colSet.splice(6,1)
        for(var i=0;i<$scope.logData.dataSet.length;i++){
            $scope.logData.dataSet[i].splice(6,1)
        }
    }
    }

    $scope.selectedUser = "Select User";
    $scope.selectUser = function(user){
        $scope.selectedUser = user;
    }

    $scope.resetData = function(){
        $scope.logData = angular.copy($scope.loggedData);
        $scope.logData.colSet.splice(6,1)
        for(var i=0;i<$scope.logData.dataSet.length;i++){
            $scope.logData.dataSet[i].splice(6,1)
        }
        $scope.selectedUser = "Select User";
        $scope.start_date = ''
        $scope.end_date = ''
    }

});
