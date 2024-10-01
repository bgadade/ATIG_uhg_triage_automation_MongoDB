angular.module("myapp")
  .controller("pdfMapController", function($scope, $window, $http, mainService, $sce, $state) {
    $('#mydiv').hide();

    // configure PDF Info
    $scope.c = 0;
    $scope.tableData = null;
    $scope.startText = null;
    $scope.mvMapFlag = false;
    $scope.openPDFFile = function() {
      $scope.c++;
      mainService.openPDFFile(function(response) {
        if (response.status >= 200 && response.status <= 299) {
          console.log(response);
          console.log($scope.c);
          var file = new Blob([response.data], {
            type: 'application/pdf'
          });
          var fileURL = URL.createObjectURL(file);
          $window.open(fileURL, 'C-Sharpcorner' + $scope.c, 'width=700,height=600');
        }
      });
    }
    //        $scope.singleValueData = null;

    $scope.pdfInfo = {};
    $scope.header = null;
    $scope.getPdfData = function() {
      pdfconfData = mainService.getPDFInfo().pdfInfo.pdfInfoData[0];
      $scope.pdfInfo = pdfconfData;
      $scope.svFlag = $scope.pdfInfo.svFlag
      $scope.identifierTextCheck = "None"
      debugger;
      if ($scope.pdfInfo) {
        if ($scope.pdfInfo.pdfSections.header == "None") {
          $scope.header = $scope.pdfInfo.pdfSections.header;
        } else {
          $scope.header = Object.keys($scope.pdfInfo.pdfSections.header)[0];
          $scope.headerCount = $scope.pdfInfo.pdfSections.header.lineCount;
        }
        if ($scope.pdfInfo.pdfSections.footer == "None") {
          $scope.footer = $scope.pdfInfo.pdfSections.footer;
        } else {
          $scope.footer = Object.keys($scope.pdfInfo.pdfSections.footer)[0];
          $scope.footerCount = $scope.pdfInfo.pdfSections.footer.lineCount;
        }
        $scope.recordType = $scope.pdfInfo.recordDelimitation.recordType;
        $scope.ruleType = $scope.pdfInfo.recordDelimitation.ruleType;
        if ($scope.ruleType == 'pageRule') {
          $scope.ruleFlag = true
        } else {
          $scope.ruleFlag = false
        }
        $scope.pageCount = $scope.pdfInfo.recordDelimitation.extractDetails.pageCount;
        $scope.identifierTextCheck = $scope.pdfInfo.recordDelimitation.extractDetails.identifierTextCheck;

        if (!$scope.ruleFlag) {
          $scope.identifierLine = 0
          $scope.location = "top"
        }
        $scope.identifierLine = $scope.pdfInfo.recordDelimitation.extractDetails.identifierLine;
        $scope.location = $scope.pdfInfo.recordDelimitation.extractDetails.location;
      }
    }

    $scope.svFlag = true;
    $scope.createPDFInfo = function(header, footer, headerCount, footerCount, recordType, ruleType, pageCount, identifierTextCheck, identifierLine, location) {
      $('#mydiv').show();
      $scope.recordTypeFlag = false;
      $scope.pdfSections = {
        "header": "",
        "footer": ""
      }
      $scope.recordDelimitation = {}
      if (header != "None" && headerCount) {
        $scope.pdfSections.header = {
          "lineCount": headerCount
        }
      } else {
        $scope.pdfSections.header = "None"
      }
      if (footer != "None" && footerCount) {
        $scope.pdfSections.footer = {
          "lineCount": footerCount
        }
      } else {
        $scope.pdfSections.footer = "None"
      }

      if (recordType == 'single') {
        $scope.recordDelimitation = {
          "recordType": recordType,
          "ruleType": "None",
          "extractDetails": {
            "pageCount": "None",
            "identifierTextCheck": "None",
            "identifierLine": "None",
            "location": "None"
          }
        }
        $scope.recordTypeFlag = true;
      } else {
        if (recordType == 'multiple') {
          if (ruleType == 'pageRule' && pageCount) {
            $scope.recordTypeFlag = true;
          } else if (ruleType == 'textlineRule' && identifierTextCheck && identifierLine && location) {
            $scope.recordTypeFlag = true;
          }
        } else {
          $scope.recordTypeFlag = false;
        }
        if ($scope.recordTypeFlag) {
          $scope.getValues = [identifierTextCheck]

          for (var i = 0; i < $scope.getValues.length - 1; i++) {
            if ($scope.getValues[i].toLowerCase() == "none") {
              $scope.getValues[i] = "None"
            }
          }
          $scope.recordDelimitation = {
            "recordType": recordType,
            "ruleType": ruleType,
            "extractDetails": {
              "pageCount": pageCount,
              "identifierTextCheck": identifierTextCheck,
              "identifierLine": identifierLine,
              "location": location
            }
          }
        }
      }

      $scope.pdfInfo = {
        "pdfSections": $scope.pdfSections,
        "recordDelimitation": $scope.recordDelimitation,
        "yMargin": 0,
        "xMargin": 0
      }
      $scope.pdfInfoFlag = true;
      if ($scope.recordTypeFlag) {
        mainService.sendPDFInfo($scope.pdfInfo, function(response) {
          if (response.status >= 200 && response.status <= 299) {
            console.log(response.data)
            $scope.recordCount = Object.keys(response.data["recordDi"]).length;
            $('#mydiv').hide();
            swal({
              text: 'PDF info added succesfully',
              type: 'success',
              confirmButtonText: 'OK'
            })
            $scope.svFlag = false;
          }
        });
      } else {
        swal("please fill all the fields");
        $('#mydiv').hide();
      }
    }

    // SingleValue
    $scope.svMapObj = [{
      "minXTextVal": [],
      "maxXTextVal": [],
      "minYTextVal": [],
      "maxYTextVal": [],
      "minXInputFields": [{}],
      "maxXInputFields": [{}],
      "minYInputFields": [{}],
      "maxYInputFields": [{}],
       startText: ""
    }];

    $scope.minXInputFields = [{}];
    $scope.maxXInputFields = [{}];
    $scope.minYInputFields = [{}];
    $scope.maxYInputFields = [{}];

    $scope.minXTextVal = [];
    $scope.maxXTextVal = [];
    $scope.minYTextVal = [];
    $scope.maxYTextVal = [];

    $scope.selectOutItem = null;
    $scope.singleValueJson = {};
    $scope.minXaddText = function() {
      $scope.minXInputFields.push({});
    }
    $scope.maxXaddText = function() {
      $scope.maxXInputFields.push({});
    }

    $scope.minYaddText = function() {
      $scope.minYInputFields.push({});
    }
    $scope.maxYaddText = function() {
      $scope.maxYInputFields.push({});
    }

    $scope.minXaddSvText = function(index) {
      $scope.svMapObj[index].minXInputFields.push({})
    }

    $scope.maxXaddSvText = function(index) {
      $scope.svMapObj[index].maxXInputFields.push({});
    }

    $scope.minYaddSvText = function(index) {
      $scope.svMapObj[index].minYInputFields.push({});
    }
    $scope.maxYaddSvText = function(index) {
      $scope.svMapObj[index].maxYInputFields.push({});
    }

    $scope.selectOutField = function(selectedOutItem) {
      $scope.selectItemFlag = 0;
      for (var i = 0; i < $scope.svMapData.unmappedCols.length; i++) {
        if (selectedOutItem == $scope.svMapData.unmappedCols[i]) {
          $scope.selectOutItem = selectedOutItem;
          $scope.selectItemFlag = 1;
        }
        $scope.svMapObj = [{
         "minXInputFields": [{}],
        "maxXInputFields": [{}],
        "minYInputFields": [{}],
        "maxYInputFields": [{}],
        "minXTextVal": [],
        "maxXTextVal": [],
        "minYTextVal": [],
        "maxYTextVal": []
        }];
      }

      if ($scope.selectItemFlag == 0) {
        for (var i = 0; i < $scope.svMapData.mapped.length; i++) {
          if (selectedOutItem == $scope.svMapData.mapped[i].outField) {
            $scope.selectOutItem = selectedOutItem;
            $scope.selectItemFlag = 1;
          }
        }
      }
    }

    $scope.getSvMappedData = function() {
      $scope.svMapData = mainService.getPDFInfo().singleValue;
      console.log($scope.svMapData);
    }

    $scope.multiMapObj = []
    $scope.saveSingleValueMap = function() {
      if ($scope.selectOutItem) {
        $scope.mappedFlag = 0
        $scope.referenceJson = angular.copy($scope.svMapData.referenceJson);
        for (var i = 0; i < $scope.svMapData.mapped.length; i++) {
          if ($scope.svMapData.mapped[i].outField == $scope.selectOutItem){
            if ($scope.svMapData.mapped[i].mapping.multipleMap || $scope.multipleMap) {
              $scope.multiMapObj = $scope.setMultiMapVal()
              $scope.svMapData.mapped[i].mapping = {
                "map": $scope.multiMapObj,
                "multipleMap": "True"
              }
              $scope.multiFlag = true;
              $scope.mappedFlag = 1;
            } else {
              $scope.svMapObj = $scope.setSvMapval();
              $scope.svMapData.mapped[i].mapping = $scope.svMapObj;
              $scope.mappedFlag = 1;
            }
            break;
          }
        }
        if (!$scope.mappedFlag && $scope.multipleMap) {
          $scope.multiMapObj = $scope.setMultiMapVal();
          $scope.multiFlag = true;
        } else if (!$scope.mappedFlag) {
          $scope.referenceJson = $scope.setSvMapval();
        }
        if ($scope.mappedFlag == 0) {
          if ($scope.multiFlag) {
            $scope.singleValueMap = {
              "outField": $scope.selectOutItem,
              "mapping": {
                "map": $scope.multiMapObj,
                "multipleMap": "True"
              }
            }
          } else {
            $scope.singleValueMap = {
              "outField": $scope.selectOutItem,
              "mapping": $scope.referenceJson
            }
          }
            $scope.svMapData.mapped.push($scope.singleValueMap);
          }
//        } else if ($scope.mappedFlag) {
//          for (var i = 0; i < $scope.svMapData.mapped.length; i++) {
//            if ($scope.selectOutItem == $scope.svMapData.mapped[i].outField) {
//            debugger
//              if($scope.multipleMap) {
//                $scope.svMapData.mapped[i].mapping = {
//                  "map": $scope.multiMapObj,
//                  "multipleMap": "True"
//                }
//                }else {
//                $scope.svMapData.mapped[i].mapping = $scope.referenceJson
//              }
//            }
//          }
//        }
        for (var i = 0; i < $scope.svMapData.unmappedCols.length; i++) {
          if ($scope.selectOutItem == $scope.svMapData.unmappedCols[i]) {
            $scope.svMapData.unmappedCols.splice(i, 1);
          }
        }
        $scope.selectOutItem = null;
        $scope.multiMapObj=[];
        $scope.multipleMap = false;
        $scope.multiFlag = false;
        swal({
          text: 'Single Value data added succesfully',
          type: 'success',
          confirmButtonText: 'OK'
        })

        $scope.svMapObj = [{
          "minXInputFields": [{}],
          "maxXInputFields": [{}],
          "minYInputFields": [{}],
          "maxYInputFields": [{}],
          "minXTextVal": [],
          "maxXTextVal": [],
          "minYTextVal": [],
          "maxYTextVal": [],
          startText: ""
        }];
      } else {
        swal("Please select the outfield");
      }
      $scope.svMapObj = [{
        "minXInputFields": [{}],
        "maxXInputFields": [{}],
        "minYInputFields": [{}],
        "maxYInputFields": [{}],
        "minXTextVal": [],
        "maxXTextVal": [],
        "minYTextVal": [],
        "maxYTextVal": [],
        startText: ""
      }];
    }

    $scope.reset = function() {
      $scope.startText = "";
      $scope.minXTextVal = [];
      $scope.minXOccurence = ""
      $scope.minXPoint = []
      $scope.maxXTextVal = []
      $scope.maxXOccurence = ""
      $scope.maxXPoint = []
      $scope.minYTextVal = []
      $scope.minYOccurence = ""
      $scope.minYPoint = []
      $scope.maxYTextVal = []
      $scope.maxYOccurence = ""
      $scope.maxYPoint = []
      $scope.minXInputFields = [{}];
      $scope.maxXInputFields = [{}];
      $scope.minYInputFields = [{}];
      $scope.maxYInputFields = [{}];
    }

    $scope.fillSingleValueData = function(mappedOutField) {
      $scope.svMapFlag = false;
      for (var i = 0; i < $scope.svMapData.mapped.length; i++) {
        if (mappedOutField == $scope.svMapData.mapped[i].outField) {
          if ($scope.svMapData.mapped[i].mapping) {
            if ($scope.svMapData.mapped[i].mapping.multipleMap) {
              for (var k = 0; k < $scope.svMapData.mapped[i].mapping.map.length; k++) {
                if ($scope.svMapFlag) {
                  $scope.svMapObj.push({
                    "minXInputFields": [{}],
                    "maxXInputFields": [{}],
                    "minYInputFields": [{}],
                    "maxYInputFields": [{}],
                    "minXTextVal": [],
                    "maxXTextVal": [],
                    "minYTextVal": [],
                    "maxYTextVal": []
                  })
                }
                if ($scope.svMapData.mapped[i].mapping.map[k].start) {
                  $scope.svMapObj[k].startText = $scope.svMapData.mapped[i].mapping.map[k].start.text[0];
                }
                $scope.svMapObj[k].minXTextVal = $scope.svMapData.mapped[i].mapping.map[k].min_X.text;
                if ($scope.svMapObj[k].minXTextVal == "None") {
                  $scope.svMapObj[k].minXTextVal = ["None"]
                }
                for (var j = 0; j < $scope.svMapObj[k].minXTextVal.length; j++) {
                  if ($scope.svMapObj[k].minXInputFields.length == $scope.svMapObj[k].minXTextVal.length) {
                    break;
                  }
                  $scope.svMapObj[k].minXInputFields.push({});
                }
                $scope.svMapObj[k].minXOccurence = $scope.svMapData.mapped[i].mapping.map[k].min_X.occurence
                $scope.svMapObj[k].minXPoint = $scope.svMapData.mapped[i].mapping.map[k].min_X.point

                $scope.svMapObj[k].maxXTextVal = $scope.svMapData.mapped[i].mapping.map[k].max_X.text;
                if ($scope.svMapObj[k].maxXTextVal == "None") {
                  $scope.svMapObj[k].maxXTextVal = ["None"]
                }
                for (var j = 0; j < $scope.svMapObj[k].maxXTextVal.length; j++) {
                  if ($scope.svMapObj[k].maxXInputFields.length == $scope.svMapObj[k].maxXTextVal.length) {
                    break;
                  }
                  $scope.svMapObj[k].maxXInputFields.push({});
                }
                $scope.svMapObj[k].maxXOccurence = $scope.svMapData.mapped[i].mapping.map[k].max_X.occurence
                $scope.svMapObj[k].maxXPoint = $scope.svMapData.mapped[i].mapping.map[k].max_X.point

                $scope.svMapObj[k].minYTextVal = $scope.svMapData.mapped[i].mapping.map[k].min_Y.text;
                if ($scope.svMapObj[k].minYTextVal == "None") {
                  $scope.svMapObj[k].minYTextVal = ["None"]
                }
                for (var j = 0; j < $scope.svMapObj[k].minYTextVal.length; j++) {
                  if ($scope.svMapObj[k].minYInputFields.length == $scope.svMapObj[k].minYTextVal.length) {
                    break;
                  }
                  $scope.svMapObj[k].minYInputFields.push({});
                }
                $scope.svMapObj[k].minYOccurence = $scope.svMapData.mapped[i].mapping.map[k].min_Y.occurence
                $scope.svMapObj[k].minYPoint = $scope.svMapData.mapped[i].mapping.map[k].min_Y.point

                $scope.svMapObj[k].maxYTextVal = $scope.svMapData.mapped[i].mapping.map[k].max_Y.text;
                if ($scope.svMapObj[k].maxYTextVal == "None") {
                  $scope.svMapObj[k].maxYTextVal = ["None"]
                }
                for (var j = 0; j < $scope.svMapObj[k].maxYTextVal.length; j++) {
                  if ($scope.svMapObj[k].maxYInputFields.length == $scope.svMapObj[k].maxYTextVal.length) {
                    break;
                  }
                  $scope.svMapObj[k].maxYInputFields.push({});
                }
                $scope.svMapObj[k].maxYOccurence = $scope.svMapData.mapped[i].mapping.map[k].max_Y.occurence
                $scope.svMapObj[k].maxYPoint = $scope.svMapData.mapped[i].mapping.map[k].max_Y.point
                $scope.svMapFlag = true;
              }
            } else {
              if ($scope.svMapData.mapped[i].mapping.start) {
                if ($scope.svMapData.mapped[i].mapping.start.text.length != 0) {
                  $scope.svMapObj[0].startText = $scope.svMapData.mapped[i].mapping.start.text;
                }
              }
              $scope.svMapObj[0].minXTextVal = $scope.svMapData.mapped[i].mapping.min_X.text;

              if ($scope.svMapObj[0].minXTextVal == "None") {
                $scope.svMapObj[0].minXTextVal = ["None"]
              }
              for (var j = 0; j < $scope.svMapObj[0].minXTextVal.length; j++) {
                if ($scope.svMapObj[0].minXInputFields.length == $scope.svMapObj[0].minXTextVal.length) {
                  break;
                }
                $scope.svMapObj[0].minXInputFields.push({});
              }
              $scope.svMapObj[0].minXOccurence = $scope.svMapData.mapped[i].mapping.min_X.occurence
              $scope.svMapObj[0].minXPoint = $scope.svMapData.mapped[i].mapping.min_X.point

              $scope.svMapObj[0].maxXTextVal = $scope.svMapData.mapped[i].mapping.max_X.text;
              if ($scope.svMapObj[0].maxXTextVal == "None") {
                $scope.svMapObj[0].maxXTextVal = ["None"]
              }
              for (var j = 0; j < $scope.svMapObj[0].maxXTextVal.length; j++) {
                if ($scope.svMapObj[0].maxXInputFields.length == $scope.svMapObj[0].maxXTextVal.length) {
                  break;
                }
                $scope.svMapObj[0].maxXInputFields.push({});
              }
              $scope.svMapObj[0].maxXOccurence = $scope.svMapData.mapped[i].mapping.max_X.occurence
              $scope.svMapObj[0].maxXPoint = $scope.svMapData.mapped[i].mapping.max_X.point

              $scope.svMapObj[0].minYTextVal = $scope.svMapData.mapped[i].mapping.min_Y.text;
              if ($scope.svMapObj[0].minYTextVal == "None") {
                $scope.svMapObj[0].minYTextVal = ["None"]
              }
              for (var j = 0; j < $scope.svMapObj[0].minYTextVal.length; j++) {
                if ($scope.svMapObj[0].minYInputFields.length == $scope.svMapObj[0].minYTextVal.length) {
                  break;
                }
                $scope.svMapObj[0].minYInputFields.push({});
              }
              $scope.svMapObj[0].minYOccurence = $scope.svMapData.mapped[i].mapping.min_Y.occurence
              $scope.svMapObj[0].minYPoint = $scope.svMapData.mapped[i].mapping.min_Y.point

              $scope.svMapObj[0].maxYTextVal = $scope.svMapData.mapped[i].mapping.max_Y.text;
              if ($scope.svMapObj[0].maxYTextVal == "None") {
                $scope.svMapObj[0].maxYTextVal = ["None"]
              }
              for (var j = 0; j < $scope.svMapObj[0].maxYTextVal.length; j++) {
                if ($scope.svMapObj[0].maxYInputFields.length == $scope.svMapObj[0].maxYTextVal.length) {
                  break;
                }
                $scope.svMapObj[0].maxYInputFields.push({});
              }
              $scope.svMapObj[0].maxYOccurence = $scope.svMapData.mapped[i].mapping.max_Y.occurence
              $scope.svMapObj[0].maxYPoint = $scope.svMapData.mapped[i].mapping.max_Y.point
            }
            $scope.svMapFlag = false;
            break;
          }
        }

      }
    }

    $scope.checkSvNoneCond = function(i) {
      if ($scope.svMapObj[i].minXTextVal[0].toLowerCase() == 'none') {
        $scope.svMapObj[i].minXTextVal = "None";
        $scope.svMapObj[i].minXOccurence = "None";
      }

      if ($scope.svMapObj[i].maxXTextVal[0].toLowerCase() == 'none') {
        $scope.svMapObj[i].maxXTextVal = "None";
        $scope.svMapObj[i].maxXOccurence = "None";
      }

      if ($scope.svMapObj[i].minYTextVal[0].toLowerCase() == 'none') {
        $scope.svMapObj[i].minYTextVal = "None";
        $scope.svMapObj[i].minYOccurence = "None";
      }

      if ($scope.svMapObj[i].maxYTextVal[0].toLowerCase() == 'none') {
        $scope.svMapObj[i].maxYTextVal = "None";
        $scope.svMapObj[i].maxYOccurence = "None";
      }
    }

    $scope.checkNoneCond = function() {

      if ($scope.minXTextVal[0].toLowerCase() == 'none') {
        $scope.minXTextVal = "None";
        $scope.minXOccurence = "None";
      }

      if ($scope.maxXTextVal[0].toLowerCase() == 'none') {
        $scope.maxXTextVal = "None";
        $scope.maxXOccurence = "None";
      }

      if ($scope.minYTextVal[0].toLowerCase() == 'none') {
        $scope.minYTextVal = "None";
        $scope.minYOccurence = "None";
      }

      if ($scope.maxYTextVal[0].toLowerCase() == 'none') {
        $scope.maxYTextVal = "None";
        $scope.maxYOccurence = "None";
      }
    }

    $scope.svRunSampleData = {};
    $scope.checkSVOccurence = function(i) {

      if ($scope.svMapObj[i].minXOccurence != "None" && (/^\d+$/.test($scope.svMapObj[i].minXOccurence))) {
        $scope.svMapObj[i].minXOccurence = parseInt($scope.svMapObj[i].minXOccurence)
      }
      if ($scope.svMapObj[i].maxYOccurence != "None" && (/^\d+$/.test($scope.svMapObj[i].maxYOccurence))) {
        $scope.svMapObj[i].maxYOccurence = parseInt($scope.svMapObj[i].maxYOccurence)
      }
      if ($scope.svMapObj[i].minYOccurence != "None" && (/^\d+$/.test($scope.svMapObj[i].minYOccurence))) {
        $scope.svMapObj[i].minYOccurence = parseInt($scope.svMapObj[i].minYOccurence)
      }
      if ($scope.svMapObj[i].maxXOccurence != "None" && (/^\d+$/.test($scope.svMapObj[i].maxYOccurence))) {
        $scope.svMapObj[i].maxXOccurence = parseInt($scope.svMapObj[i].maxXOccurence)
      }
    }
    $scope.checkOccurence = function() {

      if ($scope.minXOccurence != "None" && (/^\d+$/.test($scope.minXOccurence))) {
        $scope.minXOccurence = parseInt($scope.minXOccurence)
      }
      if ($scope.maxYOccurence != "None" && (/^\d+$/.test($scope.maxYOccurence))) {
        $scope.maxYOccurence = parseInt($scope.maxYOccurence)
      }
      if ($scope.minYOccurence != "None" && (/^\d+$/.test($scope.minYOccurence))) {
        $scope.minYOccurence = parseInt($scope.minYOccurence)
      }
      if ($scope.maxXOccurence != "None" && (/^\d+$/.test($scope.maxYOccurence))) {
        $scope.maxXOccurence = parseInt($scope.maxXOccurence)
      }
    }
    $scope.svRunSampleRes = true;
    $scope.svSampleRun = function() {
      $('#mydiv').show();
      mainService.svSampleRun($scope.svMapData, function(response) {
        $('#mydiv').hide();
        svRunData = response.data;
        $scope.svRunSampleData = svRunData
        $scope.svRunSampleRes = false;
      });
    }

    $scope.isReadonly = true;
    $scope.updateVal = function($index) {
      $scope.isReadonly = false;
    }
    $scope.retune = false;
    $scope.editedData = []
    $scope.saveData = function(key, val) {
      $scope.editedData.push({
        "key": key,
        "val": val
      })
      $scope.retune = true;
    }

    $scope.retuneData = function() {
      $('#mydiv').show();
      mainService.setMargin($scope.editedData, function(response) {
        $('#mydiv').hide();
        $scope.retunedData = [];
        if (response.status >= 200 && response.status <= 299) {
          $scope.retunedData = response.data
          swal("Retuned successfully");
        } else {
          $state.go("error");
        }
      });
    }


    // Table Info
    $scope.getTableInfo = function() {
      $state.go("tableInfo");
    }

    $scope.tableInfoJson = {};
    $scope.getTableData = function() {
      $scope.tableData = mainService.getPDFInfo().tableInfo;
      $scope.parentCategory = mainService.getPDFInfo().parentCategories;
    }

    $scope.showTableFlag = false;
    $scope.showTableId = false;
    $scope.createTableId = function(pCat) {
      if (pCat) {
        if ($scope.tableData.tableInfo.length == 0) {
          $scope.tableId = 1;
          $scope.showTableFlag = true;
        } else {
          for (var i = 0; i < $scope.tableData.tableInfo.length; i++) {
            if ($scope.tableData.tableInfo[i].pCat == pCat) {
              $scope.tableId = $scope.tableData.tableInfo[i].mapping.length + 1;
              $scope.showTableId = true;
              $scope.showTableFlag = true;
              break;
            } else {
              $scope.tableId = 1;
              $scope.showTableFlag = true;
            }
          }
        }
      } else {
        swal("Please select Parent Category")
      }

    }
    $scope.addTableMappings = function(pCat) {
      $scope.tableRefJson = angular.copy($scope.tableData.referenceJson);
      if ($scope.tableHeader != "None") {
        $scope.tableRefJson.tableHeader = Number($scope.tableHeader);
      } else {
        $scope.tableRefJson.tableHeader = $scope.tableHeader;
      }
      $scope.tableRefJson.type = $scope.type;
      $scope.tableRefJson.xMargin = Number($scope.tableXMargin);
      $scope.tableRefJson.tableId = Number($scope.tableId)

      $scope.checkNoneCond($scope.minXTextVal[0], $scope.maxXTextVal[0], $scope.minYTextVal[0], $scope.maxYTextVal[0]);
      $scope.checkOccurence();

      if ($scope.startText) {
        $scope.tableRefJson.parentBB.start.text[0] = $scope.startText;
      }

      $scope.tableRefJson.parentBB.min_X.text = $scope.minXTextVal;
      $scope.tableRefJson.parentBB.min_X.point = $scope.minXPoint;
      $scope.tableRefJson.parentBB.min_X.occurence = $scope.minXOccurence;

      $scope.tableRefJson.parentBB.max_X.text = $scope.maxXTextVal;
      $scope.tableRefJson.parentBB.max_X.point = $scope.maxXPoint;
      $scope.tableRefJson.parentBB.max_X.occurence = $scope.maxXOccurence;

      $scope.tableRefJson.parentBB.min_Y.text = $scope.minYTextVal;
      $scope.tableRefJson.parentBB.min_Y.point = $scope.minYPoint;
      $scope.tableRefJson.parentBB.min_Y.occurence = $scope.minYOccurence;

      $scope.tableRefJson.parentBB.max_Y.text = $scope.maxYTextVal;
      $scope.tableRefJson.parentBB.max_Y.point = $scope.maxYPoint;
      $scope.tableRefJson.parentBB.max_Y.occurence = $scope.maxYOccurence;

      // $scope.tableData
      $scope.tablePcatFlag = 0
      $scope.tableIdFlag = 0
      for (var i = 0; i < $scope.tableData.tableInfo.length; i++) {
        if ($scope.tableData.tableInfo[i].pCat == pCat) {
          for (var j = 0; j < $scope.tableData.tableInfo[i].mapping.length; j++) {
            if ($scope.tableData.tableInfo[i].mapping[j].tableId == $scope.tableId) {
              $scope.tableData.tableInfo[i].mapping[j] = $scope.tableRefJson;
              $scope.tableIdFlag = 1
            }
          }
          if ($scope.tableIdFlag == 0) {
            $scope.tableData.tableInfo[i].mapping.push($scope.tableRefJson);
            $scope.tableIDs.push($scope.tableId)
          }
          $scope.tablePcatFlag = 1;
          swal("Table Data added successfully")
          break;
        }
      }

      if ($scope.tablePcatFlag == 0) {
        $scope.tableMap = {
          "pCat": pCat,
          "mapping": [$scope.tableRefJson]
        }
        $scope.tableData.tableInfo.push($scope.tableMap);
        swal("Table Data added successfully")
        $scope.tableIDs.push($scope.tableId)
        //        $scope.tableIDs.push($scope.tableData.tableInfo[i].mapping[j].tableId)
      }
      $scope.reset();
      $scope.tableId = "";
      $scope.type = "";
      $scope.tableHeader = "";
      $scope.tableXMargin = "";
    }

    $scope.getTableIdDetails = function(selectedParentCat) {
      $scope.showTableFlag = false;
      $scope.tableIDs = [];
      for (var i = 0; i < $scope.tableData.tableInfo.length; i++) {
        if ($scope.tableData.tableInfo[i].pCat == selectedParentCat) {
          for (var j = 0; j < $scope.tableData.tableInfo[i].mapping.length; j++) {
            $scope.tableIDs.push($scope.tableData.tableInfo[i].mapping[j].tableId)
          }
        }
      }
    }
    $scope.fillTableDetails = function(tid, pCat) {
      $scope.showTableFlag = true;
      for (var i = 0; i < $scope.tableData.tableInfo.length; i++) {
        if ($scope.tableData.tableInfo[i].pCat == pCat) {
          for (var j = 0; j < $scope.tableData.tableInfo[i].mapping.length; j++) {
            if ($scope.tableData.tableInfo[i].mapping[j].tableId == tid) {
              $scope.type = $scope.tableData.tableInfo[i].mapping[j].type;
              $scope.tableId = $scope.tableData.tableInfo[i].mapping[j].tableId;
              $scope.tableHeader = $scope.tableData.tableInfo[i].mapping[j].tableHeader;
              $scope.tableXMargin = $scope.tableData.tableInfo[i].mapping[j].xMargin;

              if ($scope.tableData.tableInfo[i].mapping[j].parentBB.start) {
                $scope.startText = $scope.tableData.tableInfo[i].mapping[j].parentBB.start.text;
              }

              $scope.minXTextVal = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_X.text;
              $scope.minXPoint = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_X.point;
              $scope.minXOccurence = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_X.occurence;

              $scope.minXTextVal = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_X.text;
              if ($scope.minXTextVal == "None") {
                $scope.minXTextVal = ["None"]
              }
              for (var k = 0; k < $scope.minXTextVal.length; k++) {
                if ($scope.minXInputFields.length == $scope.minXTextVal.length) {
                  break;
                }
                $scope.minXInputFields.push({});
              }


              $scope.minXPoint = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_X.point
              $scope.minXOccurence = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_X.occurence;

              $scope.maxXTextVal = $scope.tableData.tableInfo[i].mapping[j].parentBB.max_X.text;
              if ($scope.maxXTextVal == "None") {
                $scope.maxXTextVal = ["None"]
              }
              for (var k = 0; k < $scope.maxXTextVal.length; k++) {
                if ($scope.maxXInputFields.length == $scope.maxXTextVal.length) {
                  break;
                }
                $scope.maxXInputFields.push({});
              }

              $scope.maxXPoint = $scope.tableData.tableInfo[i].mapping[j].parentBB.max_X.point;
              $scope.maxXOccurence = $scope.tableData.tableInfo[i].mapping[j].parentBB.max_X.occurence;

              $scope.minYTextVal = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_Y.text;
              if ($scope.minYTextVal == "None") {
                $scope.minYTextVal = ["None"]
              }
              for (var k = 0; k < $scope.minYTextVal.length; k++) {
                if ($scope.minYInputFields.length == $scope.minYTextVal.length) {
                  break;
                }
                $scope.minYInputFields.push({});
              }

              $scope.minYPoint = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_Y.point;
              $scope.minYOccurence = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_Y.occurence;

              $scope.maxYTextVal = $scope.tableData.tableInfo[i].mapping[j].parentBB.max_Y.text;
              if ($scope.maxYTextVal == "None") {
                $scope.maxYTextVal = ["None"]
              }
              for (var k = 0; k < $scope.maxYTextVal.length; k++) {
                if ($scope.maxYInputFields.length == $scope.maxYTextVal.length) {
                  break;
                }
                $scope.maxYInputFields.push({});
              }

              $scope.maxYPoint = $scope.tableData.tableInfo[i].mapping[j].parentBB.max_Y.point;
              $scope.maxYOccurence = $scope.tableData.tableInfo[i].mapping[j].parentBB.max_Y.occurence;

            }

          }
        }
      }

    }



    $scope.resetTableDetails = function() {
      $scope.type = "";
      $scope.tableId = "";
      $scope.tableHeader = "";
      $scope.tableXMargin = "";
      $scope.reset();

    }


    //loop Info
    $scope.loopData = null;
    $scope.loopRefenceJson = {};
    $scope.delTextVal_1 = [];
    $scope.delInputfields_1 = [{}];
    $scope.delGrpFileds = [{}];
    $scope.delGrpVal = [];

    $scope.delTextVal_2 = [];
    $scope.delInputfields_2 = [{}];

    $scope.addDelText_1 = function() {
      $scope.delInputfields_1.push({});
    }

    $scope.delimiterFlag = false;
    $scope.addDelText_2 = function() {
      $scope.delInputfields_2.push({});
    }

    $scope.removeDel = function() {
      $scope.delInputfields_2 = [{}];
      $scope.delTextVal_2 = [];
      $scope.delimiterFlag = false;
      $scope.showDelFlag = false;
    }

    $scope.showDelFlag = false;
    $scope.addDelGrp = function() {
      $scope.delimiterFlag = true;
      $scope.showDelFlag = true;
    }

    $scope.getLoopInfo = function() {
      $scope.loopData = mainService.getPDFInfo().loopInfo;
      $scope.loopRefenceJson = $scope.loopData.referenceJson;
      $scope.parentCategory = mainService.getPDFInfo().parentCategories;
    }

    $scope.showLoppId = false;
    $scope.showLoopFlag = false;
    $scope.createLoopId = function(pCat) {
      if (pCat) {
        if ($scope.loopData.loopInfo.length == 0) {
          $scope.loopId = 1;
          $scope.showLoopFlag = true;
        } else {
          for (var i = 0; i < $scope.loopData.loopInfo.length; i++) {
            if ($scope.loopData.loopInfo[i].pCat == pCat) {
              $scope.loopId = $scope.loopData.loopInfo[i].mapping.length + 1;
              $scope.showLoopId = true;
              $scope.showLoopFlag = true;
              break;
            } else {
              $scope.loopId = 1;
              $scope.showLoopFlag = true;
            }
          }
        }
      } else {
        swal("Please select Parent Category")
      }
    }

    $scope.resetLoopDetails = function() {
      $scope.loopType = "";
      $scope.loopId = "";
      $scope.loopDirection = "";
      $scope.delTextVal_1 = [];
      $scope.delTextVal_2 = [];
      $scope.delInputfields_1 = [{}];
      $scope.delInputfields_2 = [{}];
      $scope.showFillLoopFlag = false;
      $scope.reset();
    }

    $scope.getLoopIdDetails = function(selectedParentCat) {
      $scope.showLoopFlag = false;
      $scope.loopIDs = [];
      for (var i = 0; i < $scope.loopData.loopInfo.length; i++) {
        if ($scope.loopData.loopInfo[i].pCat == selectedParentCat) {
          for (var j = 0; j < $scope.loopData.loopInfo[i].mapping.length; j++) {
            $scope.loopIDs.push($scope.loopData.loopInfo[i].mapping[j].loopId)
          }
        }
      }
    }

    $scope.showFillLoopFlag = false;
    $scope.fillLoopDetails = function(lId, pCat) {
      $scope.resetLoopDetails();
      $scope.showLoopFlag = true;
      for (var i = 0; i < $scope.loopData.loopInfo.length; i++) {
        if ($scope.loopData.loopInfo[i].pCat == pCat) {
          for (var j = 0; j < $scope.loopData.loopInfo[i].mapping.length; j++) {
            if ($scope.loopData.loopInfo[i].mapping[j].loopId == lId) {
              $scope.loopType = $scope.loopData.loopInfo[i].mapping[j].delimiter.type;
              $scope.loopId = $scope.loopData.loopInfo[i].mapping[j].loopId;
              $scope.loopDirection = $scope.loopData.loopInfo[i].mapping[j].direction;

              if ($scope.loopData.loopInfo[i].mapping[j].parentBB.start) {
                $scope.startText = $scope.loopData.loopInfo[i].mapping[j].parentBB.start.text;
              }

              $scope.delTextVal_1 = $scope.loopData.loopInfo[i].mapping[j].delimiter[1];
              for (var k = 0; k < $scope.delTextVal_1.length; k++) {
                if ($scope.delInputfields_1.length == $scope.delTextVal_1.length) {
                  break;
                }
                $scope.delInputfields_1.push({});
              }

              if ($scope.loopData.loopInfo[i].mapping[j].delimiter[2]) {
                $scope.delTextVal_2 = $scope.loopData.loopInfo[i].mapping[j].delimiter[2];
                for (var k = 0; k < $scope.delTextVal_2.length; k++) {
                  $scope.showFillLoopFlag = true;
                  if ($scope.delInputfields_2.length == $scope.delTextVal_2.length) {
                    break;
                  }
                  $scope.delInputfields_2.push({});
                }
              }


              $scope.minXTextVal = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_X.text;
              $scope.minXPoint = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_X.point;
              $scope.minXOccurence = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_X.occurence;

              $scope.minXTextVal = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_X.text;
              if ($scope.minXTextVal == "None") {
                $scope.minXTextVal = ["None"]
              }
              for (var k = 0; k < $scope.minXTextVal.length; k++) {
                if ($scope.minXInputFields.length == $scope.minXTextVal.length) {
                  break;
                }
                $scope.minXInputFields.push({});
              }


              $scope.minXPoint = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_X.point
              $scope.minXOccurence = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_X.occurence;

              $scope.maxXTextVal = $scope.loopData.loopInfo[i].mapping[j].parentBB.max_X.text;
              if ($scope.maxXTextVal == "None") {
                $scope.maxXTextVal = ["None"]
              }
              for (var k = 0; k < $scope.maxXTextVal.length; k++) {
                if ($scope.maxXInputFields.length == $scope.maxXTextVal.length) {
                  break;
                }
                $scope.maxXInputFields.push({});
              }

              $scope.maxXPoint = $scope.loopData.loopInfo[i].mapping[j].parentBB.max_X.point;
              $scope.maxXOccurence = $scope.loopData.loopInfo[i].mapping[j].parentBB.max_X.occurence;

              $scope.minYTextVal = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_Y.text;
              if ($scope.minYTextVal == "None") {
                $scope.minYTextVal = ["None"]
              }
              for (var k = 0; k < $scope.minYTextVal.length; k++) {
                if ($scope.minYInputFields.length == $scope.minYTextVal.length) {
                  break;
                }
                $scope.minYInputFields.push({});
              }

              $scope.minYPoint = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_Y.point;
              $scope.minYOccurence = $scope.loopData.loopInfo[i].mapping[j].parentBB.min_Y.occurence;

              $scope.maxYTextVal = $scope.loopData.loopInfo[i].mapping[j].parentBB.max_Y.text;
              if ($scope.maxYTextVal == "None") {
                $scope.maxYTextVal = ["None"]
              }
              for (var k = 0; k < $scope.maxYTextVal.length; k++) {
                if ($scope.maxYInputFields.length == $scope.maxYTextVal.length) {
                  break;
                }
                $scope.maxYInputFields.push({});
              }

              $scope.maxYPoint = $scope.loopData.loopInfo[i].mapping[j].parentBB.max_Y.point;
              $scope.maxYOccurence = $scope.loopData.loopInfo[i].mapping[j].parentBB.max_Y.occurence;

            }

          }
        }
      }

    }

    $scope.addLoopMappings = function(pCat) {
      $scope.loopRefJson = angular.copy($scope.loopData.referenceJson);
      $scope.loopRefJson.loopId = Number($scope.loopId);
      $scope.loopRefJson.delimiter.type = $scope.loopType;
      if ($scope.startText) {
        $scope.loopRefJson.parentBB.start.text[0] = $scope.startText;
      }
      $scope.loopRefJson.direction = $scope.loopDirection;
      $scope.checkNoneCond($scope.minXTextVal[0], $scope.maxXTextVal[0], $scope.minYTextVal[0], $scope.maxYTextVal[0]);
      $scope.checkOccurence();
      if ($scope.delTextVal_1) {
        $scope.loopRefJson.delimiter["1"] = $scope.delTextVal_1;
      }
      if ($scope.delTextVal_2) {
        $scope.loopRefJson.delimiter["2"] = $scope.delTextVal_2;
      }

      $scope.loopRefJson.parentBB.min_X.text = $scope.minXTextVal;
      $scope.loopRefJson.parentBB.min_X.point = $scope.minXPoint;
      $scope.loopRefJson.parentBB.min_X.occurence = $scope.minXOccurence;

      $scope.loopRefJson.parentBB.max_X.text = $scope.maxXTextVal;
      $scope.loopRefJson.parentBB.max_X.point = $scope.maxXPoint;
      $scope.loopRefJson.parentBB.max_X.occurence = $scope.maxXOccurence;

      $scope.loopRefJson.parentBB.min_Y.text = $scope.minYTextVal;
      $scope.loopRefJson.parentBB.min_Y.point = $scope.minYPoint;
      $scope.loopRefJson.parentBB.min_Y.occurence = $scope.minYOccurence;

      $scope.loopRefJson.parentBB.max_Y.text = $scope.maxYTextVal;
      $scope.loopRefJson.parentBB.max_Y.point = $scope.maxYPoint;
      $scope.loopRefJson.parentBB.max_Y.occurence = $scope.maxYOccurence;

      // $scope.loopData
      $scope.loopPcatFlag = 0
      $scope.loopIdFlag = 0
      for (var i = 0; i < $scope.loopData.loopInfo.length; i++) {
        if ($scope.loopData.loopInfo[i].pCat == pCat) {
          for (var j = 0; j < $scope.loopData.loopInfo[i].mapping.length; j++) {
            if ($scope.loopData.loopInfo[i].mapping[j].loopId == $scope.loopId) {
              $scope.loopData.loopInfo[i].mapping[j] = $scope.loopRefJson;
              $scope.loopIdFlag = 1
              $scope.loopRefJson = {}
              swal("Loop Data added successfully")
            }
          }
          if ($scope.loopIdFlag == 0) {
            $scope.loopData.loopInfo[i].mapping.push($scope.loopRefJson);
            $scope.loopIDs.push($scope.loopId)
            swal("Loop Data added successfully")
          }
          $scope.loopPcatFlag = 1;
          $scope.loopRefJson = {}
          break;
        }
      }

      if ($scope.loopPcatFlag == 0) {
        $scope.loopMap = {
          "pCat": pCat,
          "mapping": [$scope.loopRefJson]
        }
        $scope.loopData.loopInfo.push($scope.loopMap);
        $scope.loopRefJson = {}
        swal("Loop Data added successfully");
        $scope.loopIDs.push($scope.loopId)
      }
      $scope.reset();
      $scope.loopId = "";
      $scope.loopDirection = "";
      $scope.loopType = "";
      $scope.delTextVal_1 = []
    }



    //save all the Json
    $scope.saveSingleValueJson = function() {
      mainService.pdfUserCommittedSvMappings($scope.svMapData);
      $state.go("tableInfo");
    }

    $scope.saveTableMapJson = function() {
      mainService.pdfUserCommittedTableMappings($scope.tableData);
      $state.go("loopInfo");
    }

    $scope.saveLoopMapJson = function() {
      mainService.pdfUserCommittedLoopMappings($scope.loopData);
      $state.go("pdfMvMap");
    }

    $scope.removeMinX = function(index) {
      //Remove the item from inputFields using Index.
      if ($scope.minXInputFields.length > 1) {
        $scope.minXInputFields.splice(index, 1);
        $scope.minXTextVal.splice(index, 1)
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.removeMaxX = function(index) {
      //Remove the item from inputFields using Index.
      if ($scope.maxXInputFields.length > 1) {
        $scope.maxXInputFields.splice(index, 1);
        $scope.maxXTextVal.splice(index, 1)
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.removeMinY = function(index) {
      //Remove the item from inputFields using Index.
      if ($scope.minYInputFields.length > 1) {
        $scope.minYInputFields.splice(index, 1);
        $scope.minYTextVal.splice(index, 1)
      } else {
        swal("There should be minimum one text");
      }

    }
    $scope.removeMaxY = function(index) {
      //Remove the item from inputFields using Index.
      if ($scope.maxYInputFields.length > 1) {
        $scope.maxYInputFields.splice(index, 1);
        $scope.maxYTextVal.splice(index, 1)
      } else {
        swal("There should be minimum one text");
      }
    }

    $scope.removeSvMinX = function(inputIdx, parentIdx) {
      debugger;
      //Remove the item from inputFields using Index.
      if ($scope.svMapObj[parentIdx].minXInputFields.length > 1) {
        $scope.svMapObj[parentIdx].minXInputFields.splice(inputIdx, 1)
        delete $scope.svMapObj[parentIdx].minXTextVal[inputIdx]
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.removeSvMaxX = function(inputIdx, parentIdx) {
      //Remove the item from inputFields using Index.
      if ($scope.svMapObj[parentIdx].maxXInputFields.length > 1) {
        $scope.svMapObj[parentIdx].maxXInputFields.splice(idx, 1)
        delete $scope.svMapObj[parentIdx].maxXTextVal[inputIdx]
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.removeSvMinY = function(inputIdx, parentIdx) {
      //Remove the item from inputFields using Index.
      if ($scope.svMapObj[parentIdx].minYInputFields.length > 1) {
        $scope.svMapObj[parentIdx].minYInputFields.splice(inputIdx, 1);
        delete $scope.svMapObj[parentIdx].minYTextVal[inputIdx]
      } else {
        swal("There should be minimum one text");
      }

    }
    $scope.removeSvMaxY = function(inputIdx, parentIdx) {
      //Remove the item from inputFields using Index.
      if ($scope.$scope.svMapObj[parentIdx].maxYInputFields.length > 1) {
        $scope.$scope.svMapObj[parentIdx].maxYInputFields.splice(inputIdx, 1);
        delete $scope.svMapObj[parentIdx].maxYTextVal[inputIdx]
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.removeDelimter1 = function(index) {
      //Remove the item from inputFields using Index.
      if ($scope.delInputfields_1.length > 1) {
        $scope.delInputfields_1.splice(index, 1);
        $scope.delTextVal_1.splice(index, 1)
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.removeDelimter2 = function(index) {
      //Remove the item from inputFields using Index.
      if ($scope.delInputfields_2.length > 1) {
        $scope.delInputfields_2.splice(index, 1);
        $scope.delTextVal_2.splice(index, 1)
      } else {
        swal("There should be minimum one text");
      }
    }
    $scope.ruleFlag = false;
    $scope.setRuleFlag = function() {
      if ($scope.ruleType == 'pageRule') {
        $scope.ruleFlag = true;
      } else {
        $scope.ruleFlag = false;
        $scope.identifierTextCheck = "None"
        $scope.identifierLine = 0;
        $scope.location = "top"
      }
    }

    $scope.savePdfInfo = function() {
      $scope.pdfInfo["svFlag"] = false;
      mainService.pdfUserCommittedPdfMappings($scope.pdfInfo);
      $state.go("pdfSVMap");
    }
    $scope.setMvMap = function() {
      $scope.mvMapFlag = true;
    }

    $scope.deleteMap = function() {
      if ($scope.selectOutItem) {
        for (var i = 0; i < $scope.svMapData.mapped.length; i++) {
          if ($scope.svMapData.mapped[i].outField == $scope.selectOutItem) {
            $scope.svMapData.unmappedCols.push($scope.selectOutItem)
            $scope.svMapData.mapped.splice(i, 1);
             $scope.svMapObj = [{
              "minXTextVal": [],
              "maxXTextVal": [],
              "minYTextVal": [],
              "maxYTextVal": [],
              "minXInputFields": [{}],
              "maxXInputFields": [{}],
              "minYInputFields": [{}],
              "maxYInputFields": [{}],
               startText: ""
            }];
            swal($scope.selectOutItem + " deleted successfully")
          }
        }
        $scope.reset();
        $scope.selectOutItem = "";
      } else {
        swal("select item to delete")
      }
    }

    $scope.deleteTableMap = function(pCat) {
      debugger;
      for (var i = 0; i < $scope.tableData.tableInfo.length; i++) {
        if ($scope.tableData.tableInfo[i].pCat == pCat) {
          for (var j = 0; j < $scope.tableData.tableInfo[i].mapping.length; j++) {
            if ($scope.tableData.tableInfo[i].mapping[j].tableId == $scope.tableId) {
              $scope.tableData.tableInfo[i].mapping.splice(j,1)
              const index = $scope.tableIDs.indexOf($scope.tableId);
                if (index > -1) {
                  $scope.tableIDs.splice(index, 1);
                }
              $scope.tableId = "";
              $scope.type = "";
              $scope.tableHeader = "";
              $scope.tableXMargin = "";
              $scope.showTableFlag = false;
              $scope.reset();
              swal("Table data removed successfully")
              break;
            }

          }
        }

      }
      if ($scope.tableIDs.length > 1) {
        $scope.tableId = $scope.tableIDs[0]
      }

    }

    $scope.deleteLoopMap = function(pCat) {
      for (var i = 0; i < $scope.loopData.loopInfo.length; i++) {
        if ($scope.loopData.loopInfo[i].pCat == pCat) {
          for (var j = 0; j < $scope.loopData.loopInfo[i].mapping.length; j++) {
            if ($scope.loopData.loopInfo[i].mapping[j].loopId == $scope.loopId) {
              $scope.loopData.loopInfo[i].mapping.splice(j,1)
              $scope.loopId = "";
              $scope.loopDirection = "Select Direction"
              $scope.loopType = ""
              $scope.delTextVal_1 = [];
              $scope.reset();
              $scope.loopIDs.splice(i, 1);
              swal("Loop data removed successfully")
               const index = $scope.loopIDs.indexOf($scope.loopId);
                if (index > -1) {
                  $scope.loopIDs.splice(index, 1);
                }
                break;
            }

          }
        }
      }
      if ($scope.tableIDs.length > 1) {
        $scope.loopId = $scope.loopIDs[0]
      }
    }

    $scope.multipleMap = false;
    $scope.addSvMap = function() {
      $scope.multipleMap = true;
      $scope.svMapObj.push({
        "minXInputFields": [{}],
        "maxXInputFields": [{}],
        "minYInputFields": [{}],
        "maxYInputFields": [{}],
        "minXTextVal": [],
        "maxXTextVal": [],
        "minYTextVal": [],
        "maxYTextVal": []
      })
    }


    $scope.setSvMapval = function() {
      $scope.checkSvNoneCond(0);
      $scope.checkSVOccurence(0);
      if ($scope.svMapObj[0].startText) {
        $scope.referenceJson.start.text[0] = $scope.svMapObj[0].startText;
      }
      $scope.referenceJson.min_X.text = $scope.svMapObj[0].minXTextVal;
      $scope.referenceJson.min_X.point = $scope.svMapObj[0].minXPoint;
      $scope.referenceJson.min_X.occurence = $scope.svMapObj[0].minXOccurence;

      $scope.referenceJson.min_Y.text = $scope.svMapObj[0].minYTextVal;
      $scope.referenceJson.min_Y.point = $scope.svMapObj[0].minYPoint;
      $scope.referenceJson.min_Y.occurence = $scope.svMapObj[0].minYOccurence;

      $scope.referenceJson.max_X.text = $scope.svMapObj[0].maxXTextVal;
      $scope.referenceJson.max_X.point = $scope.svMapObj[0].maxXPoint;
      $scope.referenceJson.max_X.occurence = $scope.svMapObj[0].maxXOccurence;

      $scope.referenceJson.max_Y.text = $scope.svMapObj[0].maxYTextVal;
      $scope.referenceJson.max_Y.point = $scope.svMapObj[0].maxYPoint;
      $scope.referenceJson.max_Y.occurence = $scope.svMapObj[0].maxYOccurence;
      return $scope.referenceJson
    }

    $scope.setMultiMapVal = function() {

      for (var j = 0; j < $scope.svMapObj.length; j++) {
        var mapObj;
        $scope.checkSvNoneCond(j);
        $scope.checkSVOccurence(j);
        if($scope.svMapObj[j].startText){
            $scope.referenceJson.start.text[0] = $scope.svMapObj[j].startText;
        }
        $scope.referenceJson.min_X.text = $scope.svMapObj[j].minXTextVal;
        $scope.referenceJson.min_X.point = $scope.svMapObj[j].minXPoint;
        $scope.referenceJson.min_X.occurence = $scope.svMapObj[j].minXOccurence;

        $scope.referenceJson.min_Y.text = $scope.svMapObj[j].minYTextVal;
        $scope.referenceJson.min_Y.point = $scope.svMapObj[j].minYPoint;
        $scope.referenceJson.min_Y.occurence = $scope.svMapObj[j].minYOccurence;

        $scope.referenceJson.max_X.text = $scope.svMapObj[j].maxXTextVal;
        $scope.referenceJson.max_X.point = $scope.svMapObj[j].maxXPoint;
        $scope.referenceJson.max_X.occurence = $scope.svMapObj[j].maxXOccurence;

        $scope.referenceJson.max_Y.text = $scope.svMapObj[j].maxYTextVal;
        $scope.referenceJson.max_Y.point = $scope.svMapObj[j].maxYPoint;
        $scope.referenceJson.max_Y.occurence = $scope.svMapObj[j].maxYOccurence;
        mapObj = angular.copy($scope.referenceJson)
        $scope.multiMapObj.push(mapObj)
      }
      return $scope.multiMapObj
    }

  });