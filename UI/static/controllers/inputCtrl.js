angular.module("myapp")
.controller("InputController", function($location,$scope,Upload,fileUpload,mainService,authService,$state,$rootScope,$stateParams) {
    mainService.initialize(); //to initialize all global variables

    $scope.inputFiles = {};
    $scope.upload = function (file) {
        $scope.myFile = "";

        $scope.files = [];
        for (var i = 0; i < file.length; i++) {

            $scope.files.push(file[i]);
            $scope.myFile = $scope.myFile + "," + file[i].name
         }
         $scope.myFile = $scope.myFile.substring(1)
    };
   $scope.selectType = 'attachment'; // to auto-select Attachment type
   $scope.delNondelType = 'delegate';

   $scope.getDelegateList = function(){
     mainService.getDelegates(function(response){
       if (response.status >= 200 && response.status <= 299)
       {
         $scope.delegateList = [];
         for (var key in response.data) {
           var obj = {
             "code": key,
             "name": response.data[key].name,
             "status": response.data[key].status
           }
          $scope.delegateList.push(obj);
         }
       }
       else {
         $state.go("error");
       }
     });
   }

   $scope.selectedDelegate = {};
   $scope.selectDelegate = function(item){
     if ($scope.selectedDelegate == item)
      $scope.selectedDelegate = {};
     else
      $scope.selectedDelegate = item;
    }
        $scope.saveDelegate = function(){
            $scope.checkNullDelegate =($scope.selectedDelegate === undefined || $scope.selectedDelegate === null || Object.keys($scope.selectedDelegate).length === 0)
            if($scope.inputFiles.attachmentFile && $scope.selectType=="attachment"){
              mainService.setDelegate($scope.selectedDelegate,$scope.inputFiles.attachmentFile,'attachmentFile',$scope.delNondelType);
              $state.go("processInput");
            }
            else if($scope.inputFiles.pdfFile && $scope.selectType=="pdf"){
            if(!$scope.checkNullDelegate){
                mainService.setDelegate($scope.selectedDelegate,$scope.inputFiles.pdfFile,'pdf',$scope.delNondelType);
                $state.go("processInput");
            }else if(!$scope.checkNullDelegate && $scope.inputFiles.pdfFile && $scope.selectType=="pdf"){
                mainService.setDelegate($scope.selectedDelegate,$scope.inputFiles.pdfFile,'pdf');
                $state.go("pdfInfoJson");
            }else{
             swal({
              text:'Please select Delegate',
              type: 'warning',
              confirmButtonText: 'OK'
            })
            }
     }
     else if($scope.selectType=="nonDelPdf"){
        mainService.setDelegate($scope.selectedDelegate,$scope.inputFiles.nonDelPdf,'nonDelPdf',$scope.delNondelType);
        fileObj = [];
        $scope.allFiles = mainService.getHeaders().filename;
        for(file in $scope.allFiles){
            fileObj.push({filename:$scope.allFiles[file],tabFlag:false})
         }
        mainService.saveFileFlags(fileObj);
        $state.go("nonDelMapData");
     }
    }
   $scope.uploadFile = function(){
       if ($scope.selectType) {
        if ($scope.files) {
            var filename = $scope.files[0].name;
            var index = filename.lastIndexOf(".");
            var strsubstring = filename.substring(index, filename.length);
            if ($scope.selectType == "email"){
                if (strsubstring == '.html' || strsubstring == '.htm')
                {
                    $scope.fileUpload();
                }
                else {
                    swal({
                        text:'Upload an email file',
                        type: 'warning',
                        confirmButtonText: 'OK'
                    })
                }
            }
            else if ($scope.selectType == "attachment"){
                if (strsubstring == '.xls' || strsubstring.toLowerCase() == '.xlsx' || strsubstring == '.xlsm' )
                {
                        $scope.fileUpload();

                }
                else {
                     swal({
                          text:'Upload excel files',
                          type: 'warning',
                          confirmButtonText: 'OK'
                        })
                }
            }
            else if ($scope.selectType == "pdf" || $scope.selectType=="nonDelPdf"){
                if (strsubstring == '.pdf' || strsubstring=='.PDF')
                {
                        $scope.fileUpload();

                }
                else {
                     swal({
                          text:'Upload pdf file',
                          type: 'warning',
                          confirmButtonText: 'OK'
                        })
                }
            }
        }
        else{
            swal({
                text:'Upload the file',
                type: 'warning',
                confirmButtonText: 'OK'
            })
        }
     }
     else{
       swal({
            text:'Select the file type',
            type: 'warning',
            confirmButtonText: 'OK'
        })
     }
   };

   $scope.fileUpload = function(){
       var fileObj = []
       for(var i=0;i<$scope.files.length;i++){
            file = $scope.files[i].name.replace(/ +/g,"_").replace(/[^a-zA-Z0-9_.]/g,"");
            fileObj.push($scope.files[i])
       }
       var uploadUrl = "/uploadFiles/upload";
       var upload = Upload.upload({
           url: uploadUrl,
           data: {
               'type': $scope.selectType,
                file: fileObj
                }
            }).then(function (resp) {
                mainService.setToken(resp.data.token,resp.data.fNmUniq)
                console.log('Success ' + resp.data.filename[0] + 'uploaded. Response: ' + resp.data);
                if ($scope.selectType == 'email') {
                  $scope.inputFiles["emailFile"] = resp.data.filename[0];
                }
                else if($scope.selectType == 'attachment') {
                  $scope.inputFiles["attachmentFile"] = resp.data.filename[0];
                }
                else if($scope.selectType == 'pdf') {
                  $scope.inputFiles["pdfFile"] = resp.data['filename'][0];
                }
                else if($scope.selectType == 'nonDelPdf'){
                    $scope.inputFiles["nonDelPdf"] = resp.data['filename'];
                }
                swal({
                  text: $scope.selectType.charAt(0).toUpperCase()+ $scope.selectType.slice(1) +' '+'uploaded successfully',
                  type: 'success',
                  confirmButtonText: 'OK'
                })
            }, function (resp) {
                console.log('Error status: ' + resp.status);
            }, function (evt) {
                var progressPercentage = parseInt(100.0 * evt.loaded / evt.total);
                console.log('progress: ' + progressPercentage + '% ' + evt.config.data.file.name);
              });
   }
   $scope.uploadDoc = function(e){
     // if (Object.keys($scope.inputFiles).length == 2)
     if (Object.keys($scope.inputFiles).length == 1)
     return false;
   else
    return true;
   };


    $scope.uploadPDF =  function(){
//    $scope.checkNullDelegate =($scope.selectedDelegate === undefined || $scope.selectedDelegate === null || Object.keys($scope.selectedDelegate).length === 0)
    if($scope.inputFiles.pdfFile && $scope.selectType=="pdf"){
        mainService.setDelegate($scope.selectedDelegate,$scope.inputFiles.pdfFile,'pdf');
        $state.go("pdfInfoJson");

    }
    }

  $scope.reset = function(selectType) {
      $scope.myFile= null;
      $scope.inputFiles = {};
      $scope.files = "";
      $scope.selectType = selectType;
      };

  $scope.delNondelTypeFlag = true;
  $scope.resetNonDel = function(delNondelType) {
    if(delNondelType=="delegate"){
        $scope.delNondelTypeFlag = true;
        $scope.selectType = "attachment"
    }else{
        $scope.delNondelTypeFlag = false;
        $scope.selectType = "nonDelPdf"
    }
      $scope.myFile= null;
      $scope.inputFiles = {};
      $scope.files = "";

  };

})
.directive('fileModel', ['$parse', function ($parse) {
            return {
               restrict: 'A',
               link: function(scope, element, attrs) {
                  var model = $parse(attrs.fileModel);
                  var modelSetter = model.assign;

                  element.bind('change', function(){
                     scope.$apply(function(){
                        modelSetter(scope, element[0].files[0]);
                     });
                  });
               }
            };
}]);