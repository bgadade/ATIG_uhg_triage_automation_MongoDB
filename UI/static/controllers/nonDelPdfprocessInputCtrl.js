angular.module("myapp")
  .controller("nonDelPdfProcessInputController", function ($scope, $stateParams, mainService, $state, authService) {

    // $scope.filename = $stateParams.attachmentFile;
    // $scope.delegate = JSON.parse($stateParams.delegate);
    $scope.selectedHeader = {};
    $scope.generatedTaxIdJson = {};
    $scope.verifiedSheet = {};
    $scope.userName = authService.getUser().name;

    $scope.strtProcess = function () {
      $('#mydiv').show();
      console.log("start processing.....!");
      headerFields = mainService.getHeaders();
      $scope.filename = headerFields.filename;
      $scope.fileType = headerFields.fileType;
      $scope.delegate = {
        "code": headerFields.delegateCode,
        "status": headerFields.delegateStatus
      }
      console.log(headerFields, "headerFields", $scope.fileType);
      $scope.headerValues = null;
      if ($scope.fileType == 'attachmentFile') {
        mainService.getInputFile($scope.userName, $scope.filename, function (response) {
          if (response.status >= 200 && response.status <= 299) {
            $scope.results = {};
            $scope.results = response.data.excelData;
            // mainService.getHeaderValues($scope.filename).then(function(res){
            if (response.data.headers) {
              $scope.processingDone = true;
              $('#mydiv').hide();
            }
            // console.log("headerValues:",JSON.stringify($scope.results));
            if (headerFields.headervalues) {
              $scope.headerValues = headerFields.headervalues;
            }
            else {
              $scope.headerValues = response.data.headers;
            }
            $scope.selectedSheetName = $scope.headerValues.tabDict[0].sheetname;
            for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
              if ($scope.headerValues.tabDict[i].startIndex == null) {
                $scope.headerValues.tabDict[i].startIndex = 1;
              }
              else {
                $scope.headerValues.tabDict[i].startIndex = $scope.headerValues.tabDict[i].startIndex;
              }
              $scope.selectedHeader[$scope.headerValues.tabDict[i].sheetname] = $scope.headerValues.tabDict[i].startIndex;
              $scope.verifiedSheet[$scope.headerValues.tabDict[i].sheetname] = true;
            }
            $scope.token = response.data.unique_id;
            $scope.processingDone = false;
          }
          else {
            $state.go("error");
          }
        });
      }
      else if ($scope.fileType == 'pdf') {
        mainService.extractPDF($scope.userName, function (response) {
          if (response.status >= 200 && response.status <= 299) {
            $scope.results = {};
            $scope.results = response.data.pdfData;
            $scope.checkNullCon = $scope.results === undefined || $scope.results === null || Object.keys($scope.results).length === 0
            if (!$scope.checkNullCon) {
              // mainService.getHeaderValues($scope.filename).then(function(res){
              if (response.data.headers) {
                $scope.processingDone = true;
                $('#mydiv').hide();
              }
              // console.log("headerValues:",JSON.stringify($scope.results));
              if (headerFields.headervalues) {
                $scope.headerValues = headerFields.headervalues;
              }
              else {
                $scope.headerValues = response.data.headers;
              }
              $scope.selectedSheetName = $scope.headerValues.tabDict[0].sheetname;
              for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
                if ($scope.headerValues.tabDict[i].startIndex == null) {
                  $scope.headerValues.tabDict[i].startIndex = 1;
                }
                else {
                  $scope.headerValues.tabDict[i].startIndex = $scope.headerValues.tabDict[i].startIndex;
                }
                $scope.selectedHeader[$scope.headerValues.tabDict[i].sheetname] = $scope.headerValues.tabDict[i].startIndex;
                $scope.verifiedSheet[$scope.headerValues.tabDict[i].sheetname] = true;
              }
              $scope.token = response.data.unique_id;
              $scope.processingDone = false;
            }
            else if ($scope.checkNullCon) {
              $state.go("pdfInfoJson");
            }
            else {
              swal({
                text: 'Selected Delegate does not support PDF, Please contact Administrator',
                type: 'warning',
                confirmButtonText: 'OK'
              })
              $state.go("home");
            }
          }
          else {
            $state.go("pdfInfoJson");
          }
        });
      }
      else if ($scope.fileType == 'nonDelPdf') {
        console.log("nonDelPdf getinput file");
        mainService.getInputFile($scope.userName, $scope.filename, function (response) {
          console.log(response, "response for nondelpdf");
          if (response.status >= 200 && response.status <= 299) {
            $scope.results = {};
            $scope.results = response.data.excelData;
            // mainService.getHeaderValues($scope.filename).then(function(res){
            if (response.data.headers) {
              $scope.processingDone = true;
              $('#mydiv').hide();
            }
            // console.log("headerValues:",JSON.stringify($scope.results));
            if (headerFields.headervalues) {
              $scope.headerValues = headerFields.headervalues;
            }
            else {
              $scope.headerValues = response.data.headers;
            }
            $scope.selectedSheetName = $scope.headerValues.tabDict[0].sheetname;
            for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
              if ($scope.headerValues.tabDict[i].startIndex == null) {
                $scope.headerValues.tabDict[i].startIndex = 1;
              }
              else {
                $scope.headerValues.tabDict[i].startIndex = $scope.headerValues.tabDict[i].startIndex;
              }
              $scope.selectedHeader[$scope.headerValues.tabDict[i].sheetname] = $scope.headerValues.tabDict[i].startIndex;
              $scope.verifiedSheet[$scope.headerValues.tabDict[i].sheetname] = true;
            }
            $scope.token = response.data.unique_id;
            $scope.processingDone = false;
          }
          else {
            $state.go("error");
          }
        });
      }

    }

    $scope.selectedHeaderContent = function () {
      for (var sheet in $scope.verifiedSheet) {
        if ($scope.verifiedSheet[sheet] == false) {
          mainService.clearMappings(sheet, true);
        }
      }
      var setHeaders = {
        "headervalues": $scope.headerValues,
        "delegateCode": $scope.delegate['code'],
        "delegateStatus": $scope.delegate['status'],
        "filename": $scope.filename
      }
      mainService.setHeaderValues(setHeaders, $scope.headerValues.tabDict[0].sheetname);
    }

    $scope.next = function () {
      $('#mydiv').show();
      $scope.fileType = headerFields.fileType;
      if ($scope.fileType == 'attachmentFile') {
        mainService.setHeaderRows(function (response) {
          if (response.status >= 200 && response.status <= 299) {
            $('#mydiv').hide();
            $state.go("mapping", { map: 'map' });
          }
          else {
            $state.go("error");
          }
        });
      }
      else if ($scope.fileType == 'pdf') {
        $scope.mapData = {}
        mainService.setMvMappingFields($scope.userName, $scope.mapData, function (response) {
          if (response == 'success') {
            $state.go("transType");
          }
          else {
            $state.go("error");
          }
        });
      } else if ($scope.fileType == 'nonDelPdf') {
        console.log("nonDelPdf");
        mainService.setHeaderRows(function (response) {
          console.log(response, "response set headers non del");
          if (response.status >= 200 && response.status <= 299) {
            $('#mydiv').hide();
            $state.go("mapping", { map: 'map' });
          }
          else {
            $state.go("error");
          }
        });
      }
    }

    $scope.showSheet = function (selectedSheet) {
      $scope.selectedSheetName = selectedSheet;
    }

    $scope.setHeaders = function (index, sheetname) {
      for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
        if ($scope.headerValues.tabDict[i].sheetname == sheetname) {
          $scope.headerValues.tabDict[i].startIndex = index + 1;
          $scope.selectedHeader[sheetname] = $scope.headerValues.tabDict[i].startIndex;
          // $scope.headerValues.tabDict[i]['TaxIDColumnName'] = "";
          $scope.verifiedSheet[sheetname] = false;
        }
      }
      // for (var x in $scope.delegate) if ($scope.delegate.hasOwnProperty(x)) delete $scope.delegate[x];
    }

    $scope.setTaxID = function (selecteditem, selectedSheetName) {
      $scope.selectedTaxId = selecteditem;
      $scope.generatedTaxIdJson[selectedSheetName] = selecteditem;
      for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
        if ($scope.headerValues.tabDict[i].sheetname == selectedSheetName) {
          $scope.headerValues.tabDict[i]['TaxIDColumnName'] = selecteditem;
          $scope.verifiedSheet[selectedSheetName] = false;
        }
      }
    }

    $scope.selectionVerified = function (sheetname) {
      for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
        if ($scope.headerValues.tabDict[i].sheetname == sheetname) {
          var index = i;
          var matches = [null, undefined];
          if ($scope.headerValues.tabDict[i]['TaxIDColumnName'] == "") {
            $scope.verifiedSheet[sheetname] = true;
          }
          else if (matches.indexOf($scope.headerValues.tabDict[i]['TaxIDColumnName']) != -1) {
            swal("Select the TaxId column")
          }
          else {
            if ($scope.results[sheetname][$scope.selectedHeader[sheetname]].indexOf($scope.headerValues.tabDict[i]['TaxIDColumnName']) == -1) {
              swal("Mismatch of TaxID Column and Header Selected")
            }
            else {
              $scope.verifiedSheet[sheetname] = true;
            }
          }
        }
      }
      if ($scope.verifiedSheet[sheetname] == true) {
        if (index < $scope.headerValues.tabDict.length - 1) {
          index = index + 1;
        }
        else {
          index = 0;
        }
        $scope.selectedSheetName = $scope.headerValues.tabDict[index].sheetname;
      }
    }

    $scope.getDelegates = function () {
      var flag = true;
      for (var key in $scope.verifiedSheet) {
        if (!$scope.verifiedSheet[key]) {
          flag = false;
        }
      }
      if (flag) {
        $('#mydiv').show();
        for (var i = 0; i < $scope.headerValues.tabDict.length; i++) {
          $scope.headerValues.tabDict[i].startIndex = $scope.headerValues.tabDict[i].startIndex + 1;
        }
        mainService.getDelegates($scope.headerValues, $scope.filename, function (response) {
          if (response.status >= 200 && response.status <= 299) {
            $('#mydiv').hide();
            $scope.allDelegates = [];
            for (var key in response.data.AllDelegates) {
              var delegateName = {};
              delegateName = {
                "code": key,
                "name": response.data.AllDelegates[key].name,
                "status": response.data.AllDelegates[key].status
              };
              $scope.allDelegates.push(delegateName);
            }
            $scope.delegate = response.data.delegates;
            if ($scope.delegate['status'] == "existing") {
              $scope.providerFlag = true;
            }
            $scope.processingDone = false;
            flag = false;
          }
          else {
            $state.go("error");
          }
        });
      }
      else {
        swal("Verify the TaxID Columns for all sheets")
      }
    }

    $scope.selectDelegate = function (delegateSelected) {
      $scope.delegate = delegateSelected;
      if ($scope.delegate['status'] == "existing") {
        $scope.providerFlag = true;
      }
      else {
        $scope.providerFlag = false;
      }
    }

    $scope.viewMapping = function () {
      mainService.viewMapping($scope.delegate['code'], function (response) {
        if (response.status >= 200 && response.status <= 299) {
          console.log("View Mapping:", response.data);
          $scope.providerMappingList = response.data;
        }
        else {
          $state.go("error");
        }
      });
    }

    $scope.generateOutput = function () {
      $('#mydiv').show();
      mainService.getProviderOutput($scope.userName, $scope.delegate['code'], function (response) {
        if (response == 'success') {
          console.log("generated provider output:", response);
          $state.go("transType");
        }
        else if (response == "false") {
          $('#mydiv').hide();
          console.log("validation failed......");
          swal({
            title: 'Error!',
            text: 'Validation with SOT columns Failed!',
            type: 'error',
            confirmButtonText: 'Close'
          })
        }
        else {
          $state.go("error");
        }
      });
    }
  });
