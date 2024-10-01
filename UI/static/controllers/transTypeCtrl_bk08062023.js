angular.module("myapp")
.controller("TransTypeController", function($scope,mainService,$state) {
  $('#mydiv').show();
  $scope.allTypes = {};
  $scope.filteredItems = {};
  $scope.selectedArray = [];
  $scope.selectAll = false;

  $scope.getTranstypes = function(){
    mainService.getTransactionData(function(response){
      console.log("TransactionType:",JSON.stringify(response.data.sysActionsJson));
      $scope.transData = response.data.sysActionsJson;
      $scope.mvMappingResponse = response.data.previousScrData;
      $scope.selectedSheetName = Object.keys($scope.transData.data)[0];
      for (var i = 0; i < $scope.transData['AllTransactionTypes'].length; i++) {
        $scope.allTypes[$scope.transData['AllTransactionTypes'][i]] = 0;
      }
      for (var sheet in $scope.transData['data']) {
        $scope.filteredItems[sheet] = {};
        for (var i = 0; i < $scope.transData['data'][sheet].length; i++) {
          $scope.allTypes[$scope.transData['data'][sheet][i].TransactionType] +=1;
        }
      }
      $('#mydiv').hide();
    });
  };

  $scope.showSheet = function(selectedItem){
    $scope.selectedSheetName = selectedItem;
    $scope.selectedArray = [];
    $scope.selectAll = false;
  }

  $scope.dropSuccessHandler = function($event,selectedItems,array,selectedRow){
    if (selectedItems.length != 0) {
      for (var i = 0; i < selectedItems.length; i++) {
        $scope.allTypes[array[selectedItems[i]].TransactionType] -= 1;
        array[selectedItems[i]].TransactionType = [];
        array[selectedItems[i]].TransactionType.push($scope.selectedType);
        $scope.allTypes[$scope.selectedType] += 1;
      }
    }
    // To enable the drag and drop without selecting the row
    // else if (selectedRow) {
    //   var index = array.indexOf(selectedRow)
    //   $scope.allTypes[selectedRow.TransactionType] -= 1;
    //   array[index].TransactionType = [];
    //   array[index].TransactionType.push($scope.selectedType);
    //   $scope.allTypes[$scope.selectedType] += 1;
    // }
    $scope.selectedArray = [];
    $scope.selectAll = false;
  };

  $scope.onDrop = function($event,$data,array){
      $scope.selectedType = array;
  };

  $scope.highlightDragRow = function(rowIndex){
    if ($scope.selectedArray.indexOf(rowIndex) == -1) {
      $scope.selectedArray.push(rowIndex);
    }
    else{
      $scope.selectedArray.splice($scope.selectedArray.indexOf(rowIndex),1);
    }
  }

  $scope.toggleAll = function(){
    if ($scope.selectAll) {
      $scope.selectedArray = [];
      for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
        $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
      }
    }
    else{
      $scope.selectedArray = [];
    }
  }

  $scope.applySearch = function(){
    if ($scope.selectAll) {
      $scope.selectedArray = [];
      for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
        $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
      }
    }
  }

  $scope.checkType = function(item){
    if (item.length >1) {
      return true;
    }
    else {
      return false;
    }
    // return Array.isArray(item);
  }

  $scope.generateOutput = function(){
    console.log(JSON.stringify($scope.transData));
        var flag = [];
        var flag1 = [];
        for(var sheet in $scope.transData.data){
          for (var i = 0; i < $scope.transData.data[sheet].length; i++) {
            if ($scope.transData.data[sheet][i].TransactionType.length > 1) {
              flag.push(sheet+$scope.transData.data[sheet][i].rowIndex);
            };
            if($scope.transData.data[sheet][i].TransactionType.length == 0){
                flag1.push(sheet+$scope.transData.data[sheet][i].rowIndex);
            }
          }
        }
        if (flag.length>0) {
          swal({
            type: 'error',
            title: 'Multiple Transaction types',
            text: 'exist for some columns...',
          })
        }
        else if(flag1.length>0){
          swal({
            type: 'error',
            title: 'No Transaction type Found',
            text: 'exist for some columns...',
          })
        }
        else {
            $('#mydiv').show()
            mainService.generateOutput($scope.transData,$scope.mvMappingResponse,function(response){
              if(response == 'success'){
                console.log("Output Generated:",response);
                $('#mydiv').hide()
                $state.go("review");
              }
              else{
                $state.go("error");
              }
        });
       }
  }

})
.directive("uiDraggable", [
        '$parse',
        '$rootScope',
        function ($parse, $rootScope) {
            return function (scope, element, attrs) {
                if (window.jQuery && !window.jQuery.event.props.dataTransfer) {
                    window.jQuery.event.props.push('dataTransfer');
                }
                element.attr("draggable", false);
                attrs.$observe("uiDraggable", function (newValue) {
                    element.attr("draggable", newValue);
                });
                var dragData = "";
                scope.$watch(attrs.drag, function (newValue) {
                    dragData = newValue;
                });
                element.bind("dragstart", function (e) {
                    var sendData = angular.toJson(dragData);
                    var sendChannel = attrs.dragChannel || "defaultchannel";
                    e.dataTransfer.setData("Text", sendData);
                    $rootScope.$broadcast("ANGULAR_DRAG_START", sendChannel);

                });

                element.bind("dragend", function (e) {
                    var sendChannel = attrs.dragChannel || "defaultchannel";
                    $rootScope.$broadcast("ANGULAR_DRAG_END", sendChannel);
                    if (e.dataTransfer && e.dataTransfer.dropEffect !== "none") {
                        if (attrs.onDropSuccess) {
                            var fn = $parse(attrs.onDropSuccess);
                            scope.$apply(function () {
                                fn(scope, {$event: e});
                            });
                        }
                    }
                });


            };
        }
    ])
    .directive("uiOnDrop", [
        '$parse',
        '$rootScope',
        function ($parse, $rootScope) {
            return function (scope, element, attr) {
                var dropChannel = "defaultchannel";
                var dragChannel = "";
                var dragEnterClass = attr.dragEnterClass || "on-drag-enter";
                var dragHoverClass = attr.dragHoverClass || "on-drag-hover";

                function onDragOver(e) {

                    if (e.preventDefault) {
                        e.preventDefault(); // Necessary. Allows us to drop.
                    }

                    if (e.stopPropagation) {
                        e.stopPropagation();
                    }
                    e.dataTransfer.dropEffect = 'move';
                    return false;
                }

                function onDragEnter(e) {
                    $rootScope.$broadcast("ANGULAR_HOVER", dropChannel);
                    element.addClass(dragEnterClass);
                    element.addClass(dragHoverClass);
                }

                function onDrop(e) {
                    if (e.preventDefault) {
                        e.preventDefault(); // Necessary. Allows us to drop.
                    }
                    if (e.stopPropagation) {
                        e.stopPropagation(); // Necessary. Allows us to drop.
                    }
                    var data = e.dataTransfer.getData("Text");
                    data = angular.fromJson(data);
                    var fn = $parse(attr.uiOnDrop);
                    scope.$apply(function () {
                        fn(scope, {$data: data, $event: e});
                    });
                    element.removeClass(dragEnterClass);
                }


                $rootScope.$on("ANGULAR_DRAG_START", function (event, channel) {
                    dragChannel = channel;
                    if (dropChannel === channel) {

                        element.bind("dragover", onDragOver);
                        element.bind("dragenter", onDragEnter);

                        element.bind("drop", onDrop);
                        // element.addClass(dragEnterClass);
                    }

                });



                $rootScope.$on("ANGULAR_DRAG_END", function (e, channel) {
                    dragChannel = "";
                    if (dropChannel === channel) {

                        element.unbind("dragover", onDragOver);
                        element.unbind("dragenter", onDragEnter);

                        element.unbind("drop", onDrop);
                        element.removeClass(dragHoverClass);
                        element.removeClass(dragEnterClass);
                    }
                });


                $rootScope.$on("ANGULAR_HOVER", function (e, channel) {
                    if (dropChannel === channel) {
                      element.removeClass(dragHoverClass);
                      element.removeClass(dragEnterClass);
                    }
                });


                attr.$observe('dropChannel', function (value) {
                    if (value) {
                        dropChannel = value;
                    }
                });


            };
        }
    ]);
