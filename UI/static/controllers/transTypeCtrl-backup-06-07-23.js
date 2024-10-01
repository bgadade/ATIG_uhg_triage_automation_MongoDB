angular.module("myapp")
  .controller("TransTypeController", function ($scope, mainService, $state) {
    $('#mydiv').show();
    $scope.allTypes = {};
    $scope.filteredItems = {};
    $scope.selectedArray = [];
    $scope.selectedcloneArray = [];
    $scope.selecteddecloneArray = [];
    $scope.selectedRowItem = [];
    $scope.selectAll = false;
    $scope.savedData = []
    $scope.redirect = true;
    $scope.cloneData = [];
    $scope.getTranstypes = function () {
      mainService.getTransactionData(function (response) {
        console.log("TransactionType:", JSON.stringify(response.data.sysActionsJson), response.data);
        $scope.savedData = response.data;
        $scope.transData = response.data.sysActionsJson;
        $scope.transDataDup = response.data.sysActionsJson;
        $scope.mvMappingResponse = response.data.previousScrData;
        $scope.selectedSheetName = Object.keys($scope.transData.data)[0];
        $scope.transDataDup.data[$scope.selectedSheetName] = $scope.transDataDup.data[$scope.selectedSheetName].map(element => {
          return obj = {
            ...element,
            row: "original"
          }
          // element["row"] = "original";
        })
        console.log($scope.transDataDup.data[$scope.selectedSheetName], "transDataDup")
        console.log($scope.transData.data[$scope.selectedSheetName], "transData")

        //   // if(element["cloned"] == true){}
        //   // else {
        //   element["cloned"] = false;
        //   // }
        // });
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
      $scope.selectedRowItem = [];
      $scope.selectAll = false;
    }

    $scope.dropSuccessHandler = function ($event, selectedItems, array, selectedRow) {
      if (selectedItems.length != 0) {
        for (var i = 0; i < selectedItems.length; i++) {
          if ($scope.allTypes[array[selectedItems[i]].TransactionType] > 0) {
            $scope.allTypes[array[selectedItems[i]].TransactionType] -= 1;
          }
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
      $scope.selectedRowItem = [];
      $scope.selectAll = false;
    };

    $scope.onDrop = function ($event, $data, array) {
      $scope.selectedType = array;
    };

    $scope.highlightDragRow = function (rowIndex, selectedArray, sheetData, data) {
      console.log(data, rowIndex);

      if ($scope.selectAll) {
        console.log($scope.selectedArray, rowIndex, "select all true", $scope.selectedArray.indexOf(rowIndex));
        if ($scope.selectedArray.indexOf(rowIndex) == -1) {
          // $scope.selecteddecloneArray.push(rowIndex);
          // $scope.selectedcloneArray.push(rowIndex);
          $scope.selectedArray.push(rowIndex);
          $scope.selectedRowItem.push(data);
        }
        else {
          // $scope.selecteddecloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          // $scope.selectedcloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selectedArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selectedRowItem.splice($scope.selectedRowItem.findIndex(e => e.rowIndex === data.rowIndex), 1);
          console.log($scope.selectedRowItem, "arr");
        }
      }
      else {
        console.log($scope.selectedArray, rowIndex, "select all false", $scope.selectedArray.indexOf(rowIndex));
        if ($scope.selectedArray.indexOf(rowIndex) == -1) {
          // $scope.selecteddecloneArray.push(rowIndex);
          // $scope.selectedcloneArray.push(rowIndex);
          $scope.selectedArray.push(rowIndex);
          $scope.selectedRowItem.push(data);
        }
        else {
          // $scope.selecteddecloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          // $scope.selectedcloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selectedArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selectedRowItem.splice($scope.selectedRowItem.findIndex(e => e.rowIndex === data.rowIndex), 1);
          console.log($scope.selectedRowItem, "arr");
        }
      }
      console.log($scope.transData.data[$scope.selectedSheetName], $scope.selectedRowItem, "cloned original arr");
    }
    $scope.cloneSelectedRow = function () {
      $scope.dataPayload = {};
      $scope.cloneData.push('clone');
      console.log($scope.transData.data[$scope.selectedSheetName], $scope.transDataDup.data[$scope.selectedSheetName], $scope.selectedArray, "cloned original arr");
      let selecterArr = [];
      selecterArr = $scope.selectedRowItem;
      console.log(selecterArr, "selected array before clone");
      selecterArr = selecterArr.map(item => {
        return obj = {
          ...item,
          row: "cloned"
        }
      })
      $scope.dupData = $scope.filteredItems[$scope.selectedSheetName];
      console.log($scope.mvMappingResponse, $scope.transData, $scope.filteredItems[$scope.selectedSheetName], selecterArr);
      console.log($scope.transData.data[$scope.selectedSheetName], "$scope.filteredItems")
      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].concat(...selecterArr);
      console.log($scope.transData.data[$scope.selectedSheetName], "after concat")
      $scope.transData.data[$scope.selectedSheetName].sort((a, b) => a.rowIndex - b.rowIndex);
      console.log($scope.filteredItems[$scope.selectedSheetName], "sorted array");
      $scope.selectedcloneArray = $scope.selectedcloneArray.concat(...$scope.selectedArray);
      $scope.selectedcloneArray = [...new Set($scope.selectedcloneArray)];
      console.log($scope.selectAll)
      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].map((ele, i) => {
        return obj = {
          ...ele,
          rowIndex: i
        };
      })

      // console.log($scope.transData.data[$scope.selectedSheetName], "$scope.filteredItems update row index");
      // $scope.filteredItems[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName];
      $scope.sheetData = $scope.transData.data[$scope.selectedSheetName];
      console.log($scope.selectedcloneArray, $scope.selectedArray)
    
      if ($scope.selectedArray.length == 0) {
        $scope.redirect = true;
      } else {
        $scope.redirect = false;
      }

      console.log($scope.transData.data[$scope.selectedSheetName], "updated array");
      let dataHeaders = $scope.mvMappingResponse.headers;
      let clonedDataHeaders = {
        "token": dataHeaders.token,
        "fNmUniq": dataHeaders.fNmUniq,
        "delegateCode": dataHeaders.delegateCode,
        "delegateStatus": dataHeaders.delegateStatus,
        "filename": dataHeaders.filename,
        "fileType": dataHeaders.fileType,
        "delType": dataHeaders.delType
      }
      let cloneData = {
        "cloneflag": true,
        "fNmUniq": dataHeaders.fNmUniq,
        "sheetname": $scope.selectedSheetName,
        "rowindex": $scope.selectedcloneArray
      }
      $scope.clonePayload = {
        "username": $scope.mvMappingResponse.username,
        "headers": clonedDataHeaders,
        "cloningData": cloneData
      }

      $scope.selectedRowItem = [];
      $scope.selectedArray = [];
      // $scope.selecteddecloneArray = [];
      $scope.viewSearch = '';
      console.log($scope.selectedcloneArray, $scope.selectedArray)
    }
    $scope.deleteSelectedRow = function () {
      $scope.dataPayload = {};
      $scope.cloneData.push('declone');
      $scope.dupData = $scope.filteredItems[$scope.selectedSheetName];
      console.log($scope.selectedRowItem, $scope.transData.data[$scope.selectedSheetName], "delete row arr");
      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].filter(function (objFromA) {
        return !$scope.selectedRowItem.find(function (objFromB) {
          return objFromA.rowIndex === objFromB.rowIndex && objFromB.row != 'original'
        })
      })
      $scope.selecteddecloneArray = $scope.selecteddecloneArray.concat(...$scope.selectedArray);
      $scope.selecteddecloneArray = [...new Set($scope.selecteddecloneArray)];

      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].map((ele, i) => {
        return obj = {
          ...ele,
          rowIndex: i
        };
      })
      console.log($scope.transData.data[$scope.selectedSheetName], "deleted array");
      console.log($scope.transData.data[$scope.selectedSheetName].length, $scope.dupData.length)
    
      if ($scope.selectedArray.length == 0 || $scope.transData.data[$scope.selectedSheetName].length == $scope.dupData.length) {
        $scope.redirect = true;
      } else {
        $scope.redirect = false;
      }
      // $scope.filteredItems[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName];
      $scope.sheetData = $scope.transData.data[$scope.selectedSheetName];

      console.log($scope.transData.data[$scope.selectedSheetName], "updated delete array");
      let dataHeaders = $scope.mvMappingResponse.headers;
      let clonedDataHeaders = {
        "token": dataHeaders.token,
        "fNmUniq": dataHeaders.fNmUniq,
        "delegateCode": dataHeaders.delegateCode,
        "delegateStatus": dataHeaders.delegateStatus,
        "filename": dataHeaders.filename,
        "fileType": dataHeaders.fileType,
        "delType": dataHeaders.delType
      }
      let cloneData = {
        "cloneflag": false,
        "fNmUniq": dataHeaders.fNmUniq,
        "sheetname": $scope.selectedSheetName,
        "rowindex": $scope.selecteddecloneArray
      }
      $scope.declonePayload = {
        "username": $scope.mvMappingResponse.username,
        "headers": clonedDataHeaders,
        "cloningData": cloneData
      }
      $scope.selectedArray = [];
      // $scope.selectedcloneArray = [];
      $scope.selectedRowItem = [];
      $scope.viewSearch = '';
    }
    $scope.resetChanges = function () {
      console.log($scope.dupData, "filteredItems",$scope.selectAll,$scope.selectedArray)
      if($scope.selectAll == true) {
        console.log("select all")
        $scope.toggleAll();
      }
      if ($scope.selectedArray.length == 0) {
        $scope.redirect = true;
      } else {
        $scope.redirect = false;
      }
      $scope.transData.data[$scope.selectedSheetName] = $scope.dupData;
      console.log($scope.transData.data[$scope.selectedSheetName], "reset data");
    }
    $scope.fetchSOTData = function () {
      let dataHeaders = $scope.mvMappingResponse.headers;
      let clonedDataHeaders = {
        "token": dataHeaders.token,
        "fNmUniq": dataHeaders.fNmUniq,
        "delegateCode": dataHeaders.delegateCode,
        "delegateStatus": dataHeaders.delegateStatus,
        "filename": dataHeaders.filename,
        "fileType": dataHeaders.fileType,
        "delType": dataHeaders.delType
      }
      let fetchData = {
        "cloneflag": "reset_clone",
        "fNmUniq": dataHeaders.fNmUniq,
        "sheetname": $scope.selectedSheetName,
        "rowindex": []
      }
      let fetchPayload = {
        "username": $scope.mvMappingResponse.username,
        "headers": clonedDataHeaders,
        "cloningData": fetchData
      }
      console.log(fetchPayload, "fetchPayload");
      mainService.cloneSelectedRows(fetchPayload, function (response) {
        if (response == 'success') {
          console.log("DeCloned Output Generated:", response);
        }
        else {
        }
      });
    }
    $scope.toggleAll = function () {
      if ($scope.selectAll) {
        $scope.selectedArray = [];
        $scope.selectedRowItem = [];
        for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
          $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
          $scope.selectedRowItem.push($scope.filteredItems[$scope.selectedSheetName][i]);
        }
      }
      else {
        $scope.selectedArray = [];
        $scope.selectedRowItem = [];
      }
    }
    $scope.showPopup = function () {
      let options = {
        backdrop: true,
        Keyboard: false
      }
      $('#myModal').modal('show', options);
      // $scope.generateOutput();
    }
    $scope.applySearch = function () {
      if ($scope.selectAll) {
        $scope.selectedRowItem = [];
        $scope.selectedArray = [];
        for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
          $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
          $scope.selectedRowItem.push($scope.filteredItems[$scope.selectedSheetName][i]);
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
    $scope.deCloneData = function() {
      console.log($scope.declonePayload,"declone payload");
      mainService.cloneSelectedRows($scope.declonePayload, function (response) {
        if (response == 'success') {
          console.log("DeCloned Output Generated:", response);
        }
        else {
        }
      });
    }
    $scope.proceedToOutput = function () {
      $scope.cloneData = [...new Set($scope.cloneData)];
      console.log($scope.clonePayload, "$scope.dataPayload", $scope.declonePayload, $scope.cloneData)
      if ($scope.cloneData.length > 1) {
        $scope.cloneData.forEach(key => {
          if (key == "clone") {
            mainService.cloneSelectedRows($scope.clonePayload, function (response) {
              if (response == 'success') {
                console.log("Cloned Output Generated:", response);
                $scope.deCloneData();
              }
            });
          }
        });
      } else {
        if ($scope.cloneData[0] == 'clone') {
          mainService.cloneSelectedRows($scope.clonePayload, function (response) {
            if (response == 'success') {
              console.log("Cloned Output Generated:", response);
            }
          });
        }
        else if ($scope.cloneData[0] = 'declone') {
         $scope.deCloneData();
        }
      }
      $('#myModal').modal('hide');
      // $scope.generateOutput();
    }
    $scope.generateOutput = function () {
      console.log($scope.transData, $scope.mvMappingResponse, "generate output responses");
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
