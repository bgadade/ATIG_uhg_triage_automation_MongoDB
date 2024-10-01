angular.module("myapp")

.controller("AdminInputController", function($scope,Upload,fileUpload) {
  $('#mydiv').hide();
  var fileUploaded =null;

  $scope.upload = function (file) {
    fileUploaded = $scope.myFile;
    $scope.files = file;
 };

 $scope.uploadFile = function(){
   if ($scope.selectType) {
     if (fileUploaded) {
       $('#mydiv').show();
       var uploadUrl = "/uploadGridFiles/upload/";
       var upload = Upload.upload({
           url: uploadUrl,
           data: {
             'type': $scope.selectType,
             file: fileUploaded
           }
       }).success(function(data, status, headers, config) {
         console.log('uploaded succesfully...');
         $('#mydiv').hide();
         swal(
            'Success',
            'File Saved Successfully!',
            'success'
          )
        //  $('#confirmMapModal').modal('show')
       }).error(function(err) {
        //  console.log(err);
         $('#mydiv').hide();
         swal({
            title: 'Error!',
            text: err,
            type: 'error',
            confirmButtonText: 'Close'
          })
       });
     }
   }
   else{
     $('#typeSelectModal').modal('show')
   }
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
