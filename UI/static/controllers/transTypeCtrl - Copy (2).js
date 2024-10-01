angular.module("myapp")
  .controller("TransTypeController", function ($scope, mainService, $state, $location) {
    $('#mydiv').show();
    $scope.allTypes = {};
    $scope.filteredItems = {};
    $scope.selectedArray = [];
    $scope.selecteddelArray = [];
    $scope.selectedcloneArray = [];
    $scope.selecteddecloneArray = [];
    $scope.selectedRowItem = [];
    $scope.selectAll = false;
    $scope.savedData = [];
    $scope.originalMapData = [];
    $scope.redirect = true;
    $scope.cloneData = [];
    $scope.toggleFirst = true;
    $scope.backbtn = false;
    $scope.selectedSheetData = {};
    $scope.decloneSheetData = {};
    $scope.updatedData = {};
    $scope.originalData = false;
    $scope.resetData = false;
    $scope.rowcount = 0;
    $scope.getTranstypes = function () {
      mainService.getTransactionData(function (response) {
        console.log("TransactionType:", JSON.stringify(response.data.sysActionsJson), response.data);
        $scope.savedData = response.data;
        $scope.originalMapData = response.data;
        $scope.transData = response.data.sysActionsJson;
        $scope.transDataDup = response.data.sysActionsJson;
        $scope.mvMappingResponse = response.data.previousScrData;
        $scope.selectedSheetName = Object.keys($scope.transData.data)[0];
        // Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: $scope.selectedcloneArray });
        // Object.assign($scope.decloneSheetData, { [$scope.selectedSheetName]: $scope.selecteddecloneArray });
        $scope.transDataDup.data[$scope.selectedSheetName] = $scope.transDataDup.data[$scope.selectedSheetName].map((element, i) => {
          console.log(element, "element on load");
          if (element.row == 'Cloned') {
            obj = {
              ...element,
              row: "Cloned",
              updatedrowIndex: i
            }
          } else {
            obj = {
              ...element,
              row: "Original",
              updatedrowIndex: i
            }
          }
          return obj;
        })
        $scope.rowcount = $scope.transDataDup.data[$scope.selectedSheetName].length;
        console.log($scope.transData.data);
        console.log(localStorage.getItem('updatedJson'), "updated json in load", JSON.parse(localStorage.getItem('updatedJson')));
        if (localStorage.getItem('updatedJson') != undefined || localStorage.getItem('updatedJson') != null) {
          $scope.updatedData = JSON.parse(localStorage.getItem('updatedJson'));
        }
        let clonedExists = false;
        let clonedExistsData = [];
        Object.values($scope.transData.data).forEach(sheetval => {
          console.log(sheetval);
          // clonedExists = sheetval.some(ele => (ele.row == 'Cloned'));
          clonedExistsData.push(sheetval.some(ele => (ele.row == 'Cloned')));
        });
        console.log(clonedExistsData);
        let clonedRows = clonedExistsData.some(ele => (ele == true));
        // let clonedRows = $scope.transData.data[$scope.selectedSheetName].some(ele => (ele.row == 'Cloned'));
        console.log(clonedRows);
        if (clonedRows) {
          console.log("clonedExists true");
          $scope.originalData = true
        }
        console.log($scope.transDataDup.data[$scope.selectedSheetName], "transDataDup")
        console.log($scope.transData.data[$scope.selectedSheetName], $scope.selectedSheetName, "transData")

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
      console.log($scope.selectedSheetData, $scope.decloneSheetData, $scope.selectedcloneArray, $scope.cloneData, "$scope.selectedSheetData show sheet");
      // Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: $scope.selectedcloneArray });
      // Object.assign($scope.decloneSheetData, { [$scope.selectedSheetName]: $scope.selecteddecloneArray });
      $scope.transDataDup.data[$scope.selectedSheetName] = $scope.transDataDup.data[$scope.selectedSheetName].map((element, i) => {
        if (element.row == 'Cloned') {
          obj = {
            ...element,
            row: "Cloned",
            updatedrowIndex: i
          }
        } else {
          obj = {
            ...element,
            row: "Original",
            updatedrowIndex: i
          }
        }
        return obj;
      })
      $scope.rowcount = $scope.transDataDup.data[$scope.selectedSheetName].length;
      console.log($scope.transDataDup.data[$scope.selectedSheetName], "show sheet after", $scope.selectedcloneArray, $scope.selecteddecloneArray)
      console.log($scope.selectedSheetData, $scope.decloneSheetData, " sheet object")

      $scope.selectedArray = [];
      $scope.selecteddelArray = [];
      $scope.selectedRowItem = [];
      $scope.selectAll = false;
      $scope.viewSearch = '';
      let resetClone = false;
      let resetDeclone = false;

      // if ($scope.cloneData.length > 0) {
      Object.entries($scope.selectedSheetData).forEach(([key, value]) => {
        console.log(key, value, $scope.selectedSheetName, "$scope.selectedSheetData key value pair");
        if ($scope.selectedSheetData.hasOwnProperty($scope.selectedSheetName)) {
          if (key == $scope.selectedSheetName && value.length > 0) {
            resetClone = true;
          } else if (key == $scope.selectedSheetName && value.length == 0) {
            resetClone = false;
          } else {
          }
        } else {
          resetClone = false;
        }
      });
      Object.entries($scope.decloneSheetData).forEach(([declonekey, declonevalue]) => {
        if ($scope.decloneSheetData.hasOwnProperty($scope.selectedSheetName)) {
          if (declonekey == $scope.selectedSheetName && declonevalue.length > 0) {
            resetDeclone = true;
          } else if (declonekey == $scope.selectedSheetName && declonevalue.length == 0) {
            resetDeclone = false;
          } else { }
        } else {
          resetDeclone = false;
        }
      });
      console.log(resetClone, resetDeclone, "check reset booleans");
      if (resetClone == true && resetDeclone == true) {
        console.log("check reset booleans true,true");
        $scope.resetData = true;
      } else if (resetClone == true && resetDeclone == false) {
        console.log("check reset booleans true,false");
        $scope.resetData = true;
      } else if (resetClone == false && resetDeclone == true) {
        console.log("check reset booleans false,true");
        $scope.resetData = true;
      } else {
        console.log("check reset booleans false,false");
        $scope.resetData = false;
      }
      // } else {
      //   console.log("clonedata 0");
      //   $scope.resetData = false;
      // }

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
      console.log($scope.allTypes, "all types");
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
      console.log(array, "array on drop");

      $scope.selectedType = array;
    };

    $scope.highlightDragRow = function (rowIndex, updatedrowIndex, selectedArray, sheetData, data) {
      console.log(data, rowIndex, updatedrowIndex, $scope.transData.data[$scope.sheetname]);

      if ($scope.selectAll) {
        console.log($scope.selectedArray, rowIndex, "select all true", $scope.selectedArray.indexOf(rowIndex));
        console.log($scope.selectedArray, updatedrowIndex, "updatedrowIndex select all true", $scope.selectedArray.indexOf(updatedrowIndex));
        if ($scope.selectedArray.indexOf(updatedrowIndex) == -1) {
          // $scope.selecteddecloneArray.push(rowIndex);
          // $scope.selectedcloneArray.push(rowIndex);
          $scope.selecteddelArray.push(updatedrowIndex);
          $scope.selectedArray.push(rowIndex);
          $scope.selectedRowItem.push(data);
        }
        else {
          // $scope.selecteddecloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          // $scope.selectedcloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selecteddelArray.splice($scope.selecteddelArray.indexOf(updatedrowIndex), 1);
          $scope.selectedArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selectedRowItem.splice($scope.selectedRowItem.findIndex(e => e.updatedrowIndex === data.updatedrowIndex), 1);
          console.log($scope.selectedRowItem, "arr", $scope.selecteddelArray, $scope.selectedArray);
        }
      }
      else {
        console.log($scope.selectedArray, rowIndex, "select all false", $scope.selectedArray.indexOf(rowIndex));
        console.log($scope.selecteddelArray, updatedrowIndex, "updatedrowIndex select all true", $scope.selecteddelArray.indexOf(updatedrowIndex));
        if ($scope.selecteddelArray.indexOf(updatedrowIndex) == -1) {
          // $scope.selecteddecloneArray.push(rowIndex);
          // $scope.selectedcloneArray.push(rowIndex);
          $scope.selecteddelArray.push(updatedrowIndex);
          $scope.selectedArray.push(rowIndex);
          $scope.selectedRowItem.push(data);
          console.log($scope.selectedRowItem, "if", $scope.selecteddelArray, $scope.selectedArray);
        }
        else {
          // $scope.selecteddecloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          // $scope.selectedcloneArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selecteddelArray.splice($scope.selecteddelArray.indexOf(updatedrowIndex), 1);
          $scope.selectedArray.splice($scope.selectedArray.indexOf(rowIndex), 1);
          $scope.selectedRowItem.splice($scope.selectedRowItem.findIndex(e => e.updatedrowIndex === data.updatedrowIndex), 1);
          console.log($scope.selectedRowItem, "else", $scope.selecteddelArray, $scope.selectedArray);
        }
      }
      console.log($scope.selectedRowItem.length, "length");
      if ($scope.selectedRowItem.length > 0) {
        $scope.rowcount = $scope.selectedRowItem.length;
      } else {
        console.log("check highlight else");
        $scope.rowcount = $scope.transData.data[$scope.selectedSheetName].length;
      }
      console.log($scope.transData.data[$scope.selectedSheetName], $scope.selectedRowItem, $scope.rowcount, "cloned original arr");
    }
    $scope.cloneSelectedRow = function () {
      console.log($scope.selectedSheetData, "$scope.selectedSheetData in clone")
      $scope.dataPayload = {};
      $scope.cloneData.push('clone');
      console.log($scope.transData.data[$scope.selectedSheetName], $scope.transDataDup.data[$scope.selectedSheetName], $scope.selectedArray, "cloned original arr");
      let selecterArr = [];
      selecterArr = $scope.selectedRowItem;
      console.log(selecterArr, "selected array before clone");
      selecterArr = selecterArr.map(item => {
        return obj = {
          ...item,
          row: "Cloned",
          updatedrowIndex: item.rowIndex
        }
      })
      $scope.dupData = $scope.filteredItems[$scope.selectedSheetName];
      console.log($scope.mvMappingResponse, $scope.transData, $scope.filteredItems[$scope.selectedSheetName], selecterArr);
      console.log($scope.transData.data[$scope.selectedSheetName], "$scope.filteredItems")
      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].concat(...selecterArr);
      console.log($scope.transData.data[$scope.selectedSheetName], "after concat")
      $scope.transData.data[$scope.selectedSheetName].sort((a, b) => a.rowIndex - b.rowIndex);
      console.log($scope.filteredItems[$scope.selectedSheetName], "sorted array");
      console.log(Object.keys($scope.selectedSheetData).length, Object.keys($scope.selectedSheetData), $scope.selectedcloneArray, $scope.selectedArray, "checking sheet data in clone")
      if (Object.keys($scope.selectedSheetData).length == 0) {
        $scope.selectedcloneArray = $scope.selectedcloneArray.concat(...$scope.selecteddelArray);
        Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: $scope.selectedcloneArray });
      } else {
        Object.entries($scope.selectedSheetData).forEach(([key, value]) => {
          console.log(key, value, $scope.selectedSheetName, "$scope.selectedSheetData key value pair");
          if ($scope.selectedSheetData.hasOwnProperty($scope.selectedSheetName)) {
            if (key == $scope.selectedSheetName) {
              console.log(value, value.concat(...$scope.selecteddelArray), "select name same if")
              value = value.concat(...$scope.selecteddelArray);
              $scope.selectedSheetData[key] = value;
            } else {
              console.log(key, value, $scope.selectedcloneArray, "select name same else")
              $scope.selectedcloneArray = [];
              value = value;
              $scope.selectedcloneArray = value;
              $scope.selectedSheetData[key] = value;
            }
          } else {
            Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: $scope.selecteddelArray });
            console.log($scope.selectedSheetData, "$scope.selectedSheetData in false property")
          }
        });
      }
      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].map((ele, i) => {
        return obj = {
          ...ele,
          updatedrowIndex: i
        };
      })
      $scope.rowcount = $scope.transData.data[$scope.selectedSheetName].length;
      $scope.redirect = false;
      $scope.originalData = true;
      $scope.resetData = true;
      $scope.sheetData = $scope.transData.data[$scope.selectedSheetName];
      console.log($scope.selectedcloneArray, $scope.selectedArray)
      console.log($scope.transData.data[$scope.selectedSheetName], "updated array");
      let dataHeaders = $scope.mvMappingResponse.headers;
      let sheetlen = $scope.transData.data[$scope.selectedSheetName].length;
      console.log($scope.selectedSheetData, $scope.savedData.previousScrData.mappings[$scope.selectedSheetName].dfLen, sheetlen)
      $scope.savedData.previousScrData.mappings[$scope.selectedSheetName].dfLen = sheetlen;
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
        "sheetdata": $scope.selectedSheetData,
        "req_data": $scope.savedData
        // "sheetname": $scope.selectedSheetName,
        // "rowindex": $scope.selectedcloneArray
      }
      $scope.clonePayload = {
        "username": $scope.mvMappingResponse.username,
        "headers": clonedDataHeaders,
        "cloningData": cloneData
      }

      $scope.selectedRowItem = [];
      $scope.selectedArray = [];
      $scope.selecteddelArray = [];
      if ($scope.selectAll) {
        $scope.toggleFirst = false;
      }
      $scope.selectAll = false;

      $scope.viewSearch = '';
      console.log($scope.selectedcloneArray, $scope.selectedArray, $scope.clonePayload)
    }
    $scope.deleteSelectedRow = function () {
      $scope.dataPayload = {};
      $scope.cloneData.push('declone');
      $scope.dupData = $scope.filteredItems[$scope.selectedSheetName];
      console.log($scope.selectedRowItem, $scope.transData.data[$scope.selectedSheetName], "delete row arr");
      let clonedArray = $scope.transData.data[$scope.selectedSheetName].some(ele => (ele.row == 'Cloned'));
      console.log(clonedArray)
      if (clonedArray) {
        let selectedRow = $scope.selectedRowItem.some(ele => (ele.row != 'Original'))
        console.log(clonedArray, selectedRow, "clonedArray if");
        if (selectedRow) {
          console.log(selectedRow, "selectedRow if");
          $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].filter(function (objFromA) {
            return !$scope.selectedRowItem.find(function (objFromB) {
              // return objFromA.rowIndex === objFromB.rowIndex && objFromB.row != 'Original'
              return objFromA.updatedrowIndex === objFromB.updatedrowIndex && objFromB.row != 'Original'

            })
          })
          if (Object.keys($scope.decloneSheetData).length == 0) {
            // $scope.selecteddecloneArray = $scope.selecteddecloneArray.concat(...$scope.selectedArray);
            $scope.selecteddecloneArray = $scope.selecteddecloneArray.concat(...$scope.selecteddelArray);
            $scope.selecteddecloneArray = [...new Set($scope.selecteddecloneArray)];
            Object.assign($scope.decloneSheetData, { [$scope.selectedSheetName]: $scope.selecteddecloneArray });
          } else {
            if ($scope.decloneSheetData.hasOwnProperty($scope.selectedSheetName)) {
              Object.entries($scope.decloneSheetData).forEach(([key, value]) => {
                console.log(key, value, "$scope.selectedSheetData key value pair");
                if (key == $scope.selectedSheetName) {
                  value = value.concat(...$scope.selecteddelArray);
                  value = [...new Set(value)];
                  $scope.decloneSheetData[key] = value;
                  console.log(value, value.concat(...$scope.selecteddelArray), "select name same")
                } else {
                  console.log(value, value.concat(...$scope.selecteddelArray), $scope.selecteddecloneArray, "select name same")
                  $scope.selecteddecloneArray = [];
                  value = value;
                  value = [...new Set(value)];
                  $scope.selecteddecloneArray = value;
                  $scope.decloneSheetData[key] = value;
                }
              });
            } else {
              // Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: $scope.selectedArray });
              Object.assign($scope.decloneSheetData, { [$scope.selectedSheetName]: $scope.selecteddelArray });
              console.log($scope.decloneSheetData, "$scope.decloneSheetData in false property")
            }
          }
          console.log(selectedRow, "selectedRow if", $scope.transData.data[$scope.selectedSheetName]);

          $scope.sheetData = $scope.transData.data[$scope.selectedSheetName];
          let sheetlen = $scope.transData.data[$scope.selectedSheetName].length;
          $scope.savedData.previousScrData.mappings[$scope.selectedSheetName].dfLen = sheetlen;
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
            "sheetdata": $scope.decloneSheetData,
            "req_data": $scope.savedData
            // "fNmUniq": dataHeaders.fNmUniq,
            // "sheetname": $scope.selectedSheetName,
            // "rowindex": $scope.selecteddecloneArray
          }
          $scope.declonePayload = {
            "username": $scope.mvMappingResponse.username,
            "headers": clonedDataHeaders,
            "cloningData": cloneData
          }

        }
        else {
          console.log(selectedRow, "selectedRow else");
          alert("Original rows can't be deleted.Please select cloned rows to delete");
        }

      } else {
        console.log(clonedArray, "clonedArray else");
        alert("Original rows can't be deleted.Please select cloned rows to delete");
        $scope.selectAll = false;
      }
      // $scope.selecteddecloneArray = $scope.selecteddecloneArray.concat(...$scope.selectedArray);
      // $scope.selecteddecloneArray = [...new Set($scope.selecteddecloneArray)];
      console.log($scope.selecteddecloneArray, "selected decloneArray");
      $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].map((ele, i) => {
        return obj = {
          ...ele,
          updatedrowIndex: i
          // rowIndex: i
        };
      })
      $scope.rowcount = $scope.transData.data[$scope.selectedSheetName].length;
      $scope.originalData = true;
      $scope.resetData = true;
      console.log($scope.transData.data[$scope.selectedSheetName], "deleted array");
      console.log($scope.transData.data[$scope.selectedSheetName].length, $scope.dupData.length)
      $scope.redirect = false;
      // $scope.filteredItems[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName];


      console.log($scope.transData.data[$scope.selectedSheetName], "updated delete array");
      console.log($scope.selectedSheetData, $scope.savedData.previousScrData.mappings[$scope.selectedSheetName].dfLen)

      console.log($scope.declonePayload, "declone payload in declone method");
      $scope.selectedArray = [];
      $scope.selecteddelArray = [];
      // $scope.selectedcloneArray = [];
      $scope.selectedRowItem = [];
      $scope.viewSearch = '';
    }
    $scope.resetChanges = function () {
      Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: [] });
      Object.assign($scope.decloneSheetData, { [$scope.selectedSheetName]: [] });
      console.log($scope.selectedSheetData, "$scope.selectedSheetData in reset")
      Object.entries($scope.selectedSheetData).forEach(([key, value]) => {
        console.log(key, value, $scope.selectedSheetName, "reset *******");
        if ($scope.selectedSheetData.hasOwnProperty($scope.selectedSheetName)) {
          if (key == $scope.selectedSheetName && value.length == 0) {
            $scope.resetData = false;
          } else { }
        } else { }
      });
      Object.entries($scope.decloneSheetData).forEach(([declonekey, declonevalue]) => {
        console.log(declonekey, declonevalue, $scope.selectedSheetName, "reset *******");
        if ($scope.selectedSheetData.hasOwnProperty($scope.selectedSheetName)) {
          if (declonevalue == $scope.selectedSheetName && declonevalue.length == 0) {
            $scope.resetData = false;
          } else { }
        } else { }
      });

      let result = Object.values($scope.selectedSheetData).every(item => item.length == 0);
      console.log(result, "object keys");
      if (result == true) {
        $scope.redirect = true;
        $scope.originalData = false;
      }
      $scope.backbtn = false;
      // $scope.resetData = false;
      $scope.cloneData = [];
      console.log(localStorage.getItem('updatedJson'), "$scope.transData.data[$scope.selectedSheetName] in reset");
      if (Object.keys($scope.updatedData).length == 0) {
        $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].filter(ele => ele.row == 'Original')
        console.log($scope.transData.data[$scope.selectedSheetName], "reset length 0");
        $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].map((ele, i) => {
          return obj = {
            ...ele,
            updatedrowIndex: i
            // rowIndex: i
          };
        })
      }
      else {
        $scope.transData.data[$scope.selectedSheetName] = $scope.updatedData.data[$scope.selectedSheetName];
        $scope.transData.data[$scope.selectedSheetName] = $scope.transData.data[$scope.selectedSheetName].map((ele, i) => {
          return obj = {
            ...ele,
            updatedrowIndex: i
            // rowIndex: i
          };
        })
        console.log($scope.transData.data[$scope.selectedSheetName], "reset data");
      }
      $scope.rowcount = $scope.transData.data[$scope.selectedSheetName].length;
    }
    $scope.fetchSOTData = function () {
      $scope.cloneData = [];
      $scope.selectedArray = [];
      $scope.selectedcloneArray = [];
      $scope.selecteddecloneArray = [];
      $scope.selectedSheetData = {};
      $scope.decloneSheetData = {};
      $scope.resetData = false;
      $scope.viewSearch = '';
      // Object.assign($scope.selectedSheetData, { [$scope.selectedSheetName]: $scope.selectedcloneArray });
      // Object.assign($scope.decloneSheetData, { [$scope.selectedSheetName]: $scope.selecteddecloneArray }); 
      console.log($scope.selectedSheetData, $scope.decloneSheetData, "sheet data in fetch sot")
      let dataHeaders = $scope.mvMappingResponse.headers;
      console.log($scope.savedData, dataHeaders, $scope.cloneData, $scope.originalMapData)
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
        "req_data": $scope.originalMapData
        // "req_data": {
        //   "username": $scope.mvMappingResponse.username,
        //   "headers": dataHeaders,
        //   "mappings": dataHeaders.mappingData
        // }
      }
      let fetchPayload = {
        "username": $scope.mvMappingResponse.username,
        "headers": clonedDataHeaders,
        "cloningData": fetchData
      }
      console.log(fetchPayload, "fetchPayload");

      mainService.cloneSelectedRows(fetchPayload, function (response) {
        if (response == 'success') {
          $scope.redirect = true;
          $scope.originalData = false;
          console.log("fetch SOT Data Output Generated:", response.data);
          mainService.fetchSotData(function (response) {
            console.log(response.data, "fetchSotData res");
            $scope.transData = response.data.sysActionsJson;
            $scope.transDataDup = response.data.sysActionsJson;
            $scope.mvMappingResponse = response.data.previousScrData;
            $scope.selectedSheetName = Object.keys($scope.transData.data)[0];
            $scope.transDataDup.data[$scope.selectedSheetName] = $scope.transDataDup.data[$scope.selectedSheetName].map(element => {
              return obj = {
                ...element,
                updatedrowIndex: element.rowIndex,
                row: "Original"
              }
            })
            console.log($scope.transData, "fetch sot data")
            $scope.rowcount = $scope.transDataDup.data[$scope.selectedSheetName].length;
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
          })
        }
        else {
        }
      });
    }
    $scope.toggleAll = function () {
      console.log($scope.selectAll, "select all toggle");
      let clonedRows = $scope.transData.data[$scope.selectedSheetName].some(ele => (ele.row == 'Cloned'));
      console.log($scope.transData.data[$scope.selectedSheetName], clonedRows, $scope.resetData)
      if ($scope.selectAll) {
        console.log($scope.toggleFirst, "if", $scope.cloneData.length, $scope.filteredItems[$scope.selectedSheetName])
        if ($scope.cloneData.length > 0 && $scope.resetData) {
          alert("Please save or reset the changes to select all rows");
          $scope.selectAll = false;
        } else {
          console.log($scope.transData.data[$scope.selectedSheetName], $scope.filteredItems[$scope.selectedSheetName]);
          $scope.selectedArray = [];
          $scope.selectedRowItem = [];
          $scope.selecteddelArray = [];
          for (var i = 0; i < $scope.filteredItems[$scope.selectedSheetName].length; i++) {
            $scope.selecteddelArray.push($scope.filteredItems[$scope.selectedSheetName][i].updatedrowIndex);
            $scope.selectedArray.push($scope.filteredItems[$scope.selectedSheetName][i].rowIndex);
            $scope.selectedRowItem.push($scope.filteredItems[$scope.selectedSheetName][i]);
          }
        }
      }
      else {
        console.log($scope.toggleFirst, "else")
        $scope.selectedArray = [];
        $scope.selectedRowItem = [];
        $scope.selecteddelArray = [];
      }
      if ($scope.selectedRowItem.length > 0) {
        $scope.rowcount = $scope.selectedRowItem.length;
      } else {
        $scope.rowcount = $scope.transData.data[$scope.selectedSheetName].length;
      }
    }
    $scope.showPopup = function () {
      let options = {
        backdrop: true,
        Keyboard: false
      }
      $('#myModal').modal('show', options);
    }
    $scope.applySearch = function () {
      console.log($scope.filteredItems[$scope.selectedSheetName], $scope.transData.data[$scope.selectedSheetName], "search apply");
      if ($scope.viewSearch == '') {
        $scope.rowcount = $scope.transData.data[$scope.selectedSheetName].length;
      } else {
        $scope.rowcount = $scope.filteredItems[$scope.selectedSheetName].length;
      }
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
    $scope.getMappingFields = function () {
      let sheetname = $scope.selectedSheetName;
      mainService.setHeaderRows(function (response) {
        console.log(response, "getmv mapping fields response");
        if (response == 'success') {
          console.log(response);

        }
      });
    }
    $scope.deCloneData = function () {
      console.log($scope.declonePayload, "declone payload");
      mainService.cloneSelectedRows($scope.declonePayload, function (response) {
        if (response == 'success') {
          $scope.redirect = true;
          $scope.originalData = true;
          $scope.resetData = false;
          console.log("DeCloned Output Generated:", response);
          mainService.fetchSotData(function (response) {
            console.log(response.data.sysActionsJson, "decloned data");
            if (response.data.sysActionsJson != undefined) {
              // $scope.transData = response.data.sysActionsJson;
              $scope.updatedData = response.data.sysActionsJson;
              console.log(JSON.stringify($scope.updatedData));
              let updatedData = JSON.stringify($scope.updatedData);
              localStorage.setItem('updatedJson', updatedData);
            }
            if (response.data == 'Select only clonned rows') {
              alert('Select only cloned rows');
            } else if (response.data == 'There is no sheet name and index list to perform declone') {
              alert('Select atleast one cloned row to declone');
            } else {
              console.log(response.data.sysActionsJson, "decloned data");
              if ($scope.backbtn) {
                console.log("back save changes");
                $state.go('mapping');
                $('#myModal').modal('hide');
                $('.modal-backdrop').hide();
              }
            }
          })
        }
      });
    }
    $scope.proceedToOutput = function () {
      $scope.cloneData = [...new Set($scope.cloneData)];
      console.log($scope.clonePayload, "$scope.dataPayload", $scope.declonePayload, $scope.cloneData, $scope.cloneData[0])
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
            console.log('success', response);
            if (response == 'success') {
              console.log("Cloned Output Generated:", response);
              mainService.fetchSotData(function (response) {
                console.log(response.data.sysActionsJson, "cloned data");
                if (response.data.sysActionsJson != undefined) {
                  $scope.updatedData = response.data.sysActionsJson;
                  console.log(JSON.stringify($scope.updatedData));
                  let updatedData = JSON.stringify($scope.updatedData);
                  localStorage.setItem('updatedJson', updatedData);
                }
              })
              $scope.redirect = true;
              $scope.resetData = false;
              if ($scope.backbtn) {
                console.log("back save changes");
                $state.go('mapping');
                $('#myModal').modal('hide');
                $('.modal-backdrop').hide();
              }
            }
          });
        }
        else if ($scope.cloneData[0] == 'declone') {
          $scope.deCloneData();
        }
      }
      $scope.cloneData = [];
      $scope.selectedSheetData = {};
      $scope.selectedcloneArray = [];
      $scope.decloneSheetData = {};
      $scope.selecteddecloneArray = [];
      $scope.viewSearch = '';
    }
    $scope.generateOutput = function () {
      console.log($scope.transData, $scope.mvMappingResponse, "generate output responses");
     
      var flag = [];
      var flag1 = [];
      for (var sheet in $scope.transData.data) {
        for (var i = 0; i < $scope.transData.data[sheet].length; i++) {
          if ($scope.transData.data[sheet][i].TransactionType.length > 1) {
            // flag.push(sheet + $scope.transData.data[sheet][i].rowIndex);
            flag.push(sheet + $scope.transData.data[sheet][i].updatedrowIndex);
          };
          if ($scope.transData.data[sheet][i].TransactionType.length == 0) {
            // flag1.push(sheet + $scope.transData.data[sheet][i].rowIndex);
            flag1.push(sheet + $scope.transData.data[sheet][i].updatedrowIndex);
          }
        }
      }
      // let payloadTransData =  $scope.transData;
      // for (var sheet in payloadTransData.data) {
      //   console.log(sheet,"trans data sheet");
      //   payloadTransData.data[sheet] = payloadTransData.data[sheet].map(element => {
      //     console.log(element,"trans data ele");
      //     return obj = {
      //       ...element,
      //       rowIndex: element.updatedrowIndex
      //     }
      //   });
      // }
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
        $('#mydiv').show();
        console.log($scope.transData);
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
    $scope.proceedToBack = function () {
      $scope.backbtn = true;
      $scope.cloneData = [...new Set($scope.cloneData)];
      console.log("back", $scope.cloneData.length);
      if ($scope.cloneData.length > 0) {
        let options = {
          backdrop: true,
          Keyboard: false
        }
        $('#myModal').modal('show', options);
      } else {
        $state.go('mapping');
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
