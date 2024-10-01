angular.module("myapp")
  .controller("TransTypeController", function ($scope, mainService, $state) {
    $('#mydiv').show();
    $scope.allTypes = {};
    $scope.filteredItems = {};
    $scope.selectedArray = [];
    $scope.selectAll = false;
    $scope.selectedRowItem = [];
    $scope.clonedArray = [];

    $scope.getTranstypes = function () {
      mainService.getTransactionData(function (response) {
        console.log("TransactionType:", JSON.stringify(response.data.sysActionsJson));
        $scope.transData = response.data.sysActionsJson;
        $scope.mvMappingResponse = response.data.previousScrData;
        $scope.selectedSheetName = Object.keys($scope.transData.data)[0];
        for (var i = 0; i < $scope.transData['AllTransactionTypes'].length; i++) {
          $scope.allTypes[$scope.transData['AllTransactionTypes'][i]] = 0;
        }
        for (var sheet in $scope.transData['data']) {
          $scope.filteredItems[sheet] = {};
          for (var i = 0; i < $scope.transData['data'][sheet].length; i++) {
            $scope.allTypes[$scope.transData['data'][sheet][i].TransactionType] += 1;
          }
        }
        $('#mydiv').hide();
      });
    };

    $scope.showSheet = function (selectedItem) {
      $scope.selectedSheetName = selectedItem;
      $scope.selectedArray = [];
      $scope.selectAll = false;
      $scope.selectedRowItem = [];
      $scope.clonedArray = [];
    }

    $scope.dropSuccessHandler = function ($event, selectedItems, array, selectedRow) {
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
      $scope.selectedRowItem = [];
      $scope.clonedArray = [];
    };

    $scope.onDrop = function ($event, $data, array) {
      $scope.selectedType = array;
    };

    $scope.highlightDragRow = function (rowIndex, selectedArray, sheetData, data) {
      // console.log($scope.filteredItems["Update"],"array after push");
      if ($scope.selectedArray.indexOf(rowIndex) == -1) {
        $scope.selectedArray.push(rowIndex);
        $scope.selectedRowItem.push(data);
        let dupfilteredItems = $scope.filteredItems["Update"];
        dupfilteredItems.forEach(element => {
          if (element.rowIndex == data.rowIndex) {
            $scope.selectedRowItem.push(element);
          }
        });
        console.log($scope.selectedRowItem, $scope.filteredItems["Update"], "array after push");
      }
      else {
        $scope.selectedArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
        $scope.selectedRowItem.splice($scope.selectedRowItem.findIndex(e => e.rowIndex === data.rowIndex), 1);
        console.log($scope.selectedRowItem, "arr");
      }
      $scope.filteredItems["Update"].splice($scope.filteredItems["Update"].findIndex(e => e.rowIndex === data.rowIndex), 1);
    }
    $scope.cloneSelectedRow = function () {
      let selecterArr = [];
      selecterArr = $scope.selectedRowItem;
      // let newArr = [];
      // selecterArr.forEach((element, i) => {
      //   console.log(element, "********", i);
      //   // delete element.rowIndex;
      //   // element.rowIndex = i;
      //   newArr.push(element);
      // })
      // console.log(newArr, "newArr")
      // let newrowArr = [];
      const newrowArr = selecterArr.map((element, j) => {
        let obj = {};
        if (selecterArr[j + 1] != undefined) {
          if ((selecterArr[j].rowIndex === selecterArr[j + 1].rowIndex) && (selecterArr[j + 1].rowIndex - selecterArr[j].rowIndex == 0)) {
            obj = {
              ...element,
              rowIndex: element.rowIndex + j
            };
          } else {
            obj = {
              ...element,
              rowIndex: element.rowIndex
            };
          }
        }
        return obj;
      });
      console.log(newrowArr, "newrowArr");

      // $scope.clonedArray = newrowArr;
      // let dupfilteredItems = $scope.filteredItems["Update"];
      // // console.log(dupfilteredItems, "dupfiltered items");

      // var matches = [];
      // for (var i = 0; i < dupfilteredItems.length; i++) {
      //   for (var e = 0; e < newrowArr.length; e++) {
      //     if (dupfilteredItems[i].rowIndex === newrowArr[e].rowIndex) matches.push(dupfilteredItems[i]);
      //   }
      // }
      // console.log(matches, "matches");
      // var matchedArr = matches.map(ele => {
      //   return {
      //     ...ele,
      //     rowIndex: ele.rowIndex + 1
      //   };
      // })
      // console.log(matchedArr, "matchedArr");
      // var result = dupfilteredItems.filter(function (o1) {
      //   return !matches.some(function (o2) {
      //     return o1.rowIndex === o2.rowIndex; // return the ones with equal id
      //   });
      // });
      // console.log(result, "result");
      // var finalArr = result.concat(...$scope.clonedArray, matchedArr);
      // console.log(finalArr, "finalArr");
      // let dupfilteredItems = $scope.filteredItems["Update"];
      // dupfilteredItems = $scope.filteredItems["Update"].concat(...$scope.clonedArray);
      // console.log(dupfilteredItems,"arr ***************");
      // dupfilteredItems = $scope.filteredItems["Update"].concat(...$scope.clonedArray);
      // $scope.clonedArray.forEach(item => {
      //   dupfilteredItems.splice(dupfilteredItems.findIndex(e => e.rowIndex === item.rowIndex),1);
      // });
      // $scope.filteredItems["Update"] = dupfilteredItems;

      // for(var i=0; i < $scope.filteredItems["Update"].length-1;i++){
      //   if(dupfilteredItems[i].rowIndex == dupfilteredItems[i+1].rowIndex){
      //     $scope.selectedRowItem.splice($scope.selectedRowItem.findIndex(e => e.rowIndex === data.rowIndex),1);
      //   }
      // }
      // console.log(dupfilteredItems, $scope.filteredItems["Update"],"dup filtered items");     
      // console.log($scope.clonedArray, "selected array", $scope.filteredItems["Update"]);
    }
    $scope.toggleAll = function () {
      if ($scope.selectAll) {
        $scope.selectedArray = [];
        // $scope.selectedRowItem = [];
        $scope.clonedArray = [];
        for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
          $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
          $scope.clonedArray.push($scope.filteredItems[$scope.selectedSheetName][i]);
        }
      }
      else {
        $scope.selectedArray = [];
        $scope.clonedArray = [];
        $scope.selectedRowItem = [];
      }
    }

    $scope.applySearch = function () {
      if ($scope.selectAll) {
        $scope.selectedArray = [];
        $scope.clonedArray = [];
        for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
          $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
          $scope.clonedArray.push($scope.filteredItems[$scope.selectedSheetName][i]);
        }
      }
    }

    $scope.checkType = function (item) {
      if (item.length > 1) {
        return true;
      }
      else {
        return false;
      }
      // return Array.isArray(item);
    }

    $scope.generateOutput = function () {
      console.log(JSON.stringify($scope.transData));
      var flag = [];
      var flag1 = [];
      for (var sheet in $scope.transData.data) {
        for (var i = 0; i < $scope.transData.data[sheet].length; i++) {
          if ($scope.transData.data[sheet][i].TransactionType.length > 1) {
            flag.push(sheet + $scope.transData.data[sheet][i].rowIndex);
          };
          if ($scope.transData.data[sheet][i].TransactionType.length == 0) {
            flag1.push(sheet + $scope.transData.data[sheet][i].rowIndex);
          }
        }
      }
      if (flag.length > 0) {
        swal({
          type: 'error',
          title: 'Multiple Transaction types',
          text: 'exist for some columns...',
        })
      }
      else if (flag1.length > 0) {
        swal({
          type: 'error',
          title: 'No Transaction type Found',
          text: 'exist for some columns...',
        })
      }
      else {
        $('#mydiv').show()
        mainService.generateOutput($scope.transData, $scope.mvMappingResponse, function (response) {
          if (response == 'success') {
            console.log("Output Generated:", response);
            $('#mydiv').hide()
            $state.go("review");
          }
          else {
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
                fn(scope, { $event: e });
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
            fn(scope, { $data: data, $event: e });
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
