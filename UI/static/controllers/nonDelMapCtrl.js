angular.module("myapp")
.controller("nonDelMapController", function($scope,mainService,$state,$sce,$interpolate,authService) {
    //Intialization

    $scope.userName = authService.getUser().name;
    var headers = mainService.getHeaders();
    $scope.fileList = mainService.getHeaders()['filename'];
    $scope.headerValues = headers.headervalues;
    $scope.isPDF = false;
    $scope.tab = 1;
    $scope.svMapJson = null;
    $scope.mvMapJson=null;
    $scope.recordResult = [];
    $scope.selectedInObject = {};
    $scope.selectedOutObject = {};
    $scope.parentMapTable = {};
    $scope.asisSelected = {};
    $scope.typeSelected = {};
    $scope.showType = {};
    $scope.selectedCategory = null;
    $scope.records = [];
    $scope.htmlDataContent = null;
    $scope.filesFlagList  = headers.filesFlag;
    $scope.backFlag = mainService.getHeaders()['backFlag'];
    $scope.selectedFile =null;
    $scope.enableNext = true;
    $scope.getPdfData = function(){
        $('#mydiv').show();
        if($scope.backFlag){
            $scope.filename = headers.selFileName;
            $scope.selectedFile = headers.selFileName;
            $scope.pdfDataList = mainService.getHeaders().pdfData;
            for(fNm in $scope.pdfDataList){
                if($scope.pdfDataList[fNm].pdfFNm == $scope.filename){
                    var pdfData = $scope.pdfDataList[fNm].pdfData;
                    var file = new Blob([pdfData], {type: 'application/pdf'});
                    var fileURL = URL.createObjectURL(file);
                    $scope.content = $sce.trustAsResourceUrl(fileURL);
                    break;
                }
            $('#mydiv').hide();
            }
        }else{
            $scope.selectedFile = $scope.fileList[0];
            mainService.saveSelFileName($scope.selectedFile);
            mainService.getPdfData(function(response){
             if (response.status >= 200 && response.status <= 299){
                $scope.setFileFlags();
                var file = new Blob([response.data], {type: 'application/pdf'});
                var fileURL = URL.createObjectURL(file);
                $scope.content = $sce.trustAsResourceUrl(fileURL);
             }
             else{
                $state.go("error");
             }
        })
        }
    }
    $scope.getHtmlData = function(){
        $('#mydiv').show();
        if($scope.backFlag){
            $scope.filename = headers.selFileName;
            $scope.selectedFile = headers.selFileName;
            $scope.htmlDataList = headers.htmlData;
            $scope.htmlData = $interpolate($scope.htmlDataList[$scope.filename])($scope)
            $scope.htmlData = $scope.htmlData.replace(/\r/gm,' ');
            $scope.htmlDataContent= $sce.trustAsHtml($scope.htmlData);
            $('#mydiv').hide();
        }
        else{
            mainService.getHtmlData($scope.userName,function(response){
             if (response.status >= 200 && response.status <= 299){
                $('#mydiv').hide();
                $scope.htmlData = $interpolate( response.data.htmlData[$scope.selectedFile])($scope)
                $scope.htmlData = $scope.htmlData.replace(/\r/gm,' ');
                $scope.htmlDataContent= $sce.trustAsHtml($scope.htmlData);
             }
             else{
                $state.go("error")
             }
        });
        }
    }

    $scope.nonDelMapSavedData = {}
    $scope.getSvMvColumns = function(){
        $scope.isPDF = mainService.getHeaders().fileFormat;
        $scope.filename = headers.selFileName;
        $scope.htmlDataList = headers.htmlData;
        $scope.htmlData = $interpolate($scope.htmlDataList[$scope.filename])($scope)
        $scope.htmlData = $scope.htmlData.replace(/\r/gm,' ');
        $scope.htmlDataContent= $sce.trustAsHtml($scope.htmlData);
        $scope.pdfDataList = mainService.getHeaders().pdfData;
        for(fNm in $scope.pdfDataList){
            if($scope.pdfDataList[fNm].pdfFNm == $scope.filename){
                var pdfData = $scope.pdfDataList[fNm].pdfData;
                var file = new Blob([pdfData], {type: 'application/pdf'});
                var fileURL = URL.createObjectURL(file);
                $scope.content = $sce.trustAsResourceUrl(fileURL);
                break;
            }
        }
        $scope.nonDelMapSavedData = mainService.getUsrCmtdNonDelData();
        if($scope.nonDelMapSavedData.records){
            $scope.records = $scope.nonDelMapSavedData.records;
            $scope.sampleSvMvJsonOrg = $scope.nonDelMapSavedData.sampleData;
            $scope.svMapJson = $scope.records[0].sv[0].unmapOtFields;
            $scope.mvMapJson = $scope.nonDelMapSavedData.mvMapJson;
            $scope.selectedCategory = $scope.mvMapJson[0].mapped[0].parentCategory;
            $scope.parentMapTable = $scope.records[0].parentMapTable;
            $scope.sampleSvMvJson = angular.copy($scope.sampleSvMvJsonOrg);
            $('#mydiv').hide();
        }else{
             mainService.getNonDelData(function(response){
                    $('#mydiv').hide();
                    if (response.status >= 200 && response.status <= 299){
                        $scope.records.push(response.data.records);
                        $scope.sampleSvMvJsonOrg = response.data.sampleSvMvMapJson;
                        $scope.svMapJson = $scope.records[0].sv[0].unmapOtFields;
                        $scope.mvMapJson = $scope.records[0]["mv"];
                        $scope.records[0]["parentMapTable"] = $scope.getMvFields();
                        $scope.parentMapTable = $scope.records[0]["parentMapTable"];
                        $scope.sampleSvMvJsonOrg["parentMapTable"] = $scope.parentMapTable;
                        $scope.sampleSvMvJson = angular.copy($scope.sampleSvMvJsonOrg);
                    }
                    else{
                        $state.go("error")
                    }
            });
        }
    }
    $scope.setTab = function (tabId) {
        $scope.tab = tabId;
    };
    $scope.isSet = function (tabId) {
        return $scope.tab === tabId;
    };
     $scope.getMvFields = function(){
        for(var i=0;i<$scope.mvMapJson[0].mapped.length;i++){
        if($scope.mvMapJson[0].mapped[i].type!='AsIs'){
            for(var j=0;j<$scope.mvMapJson[0].mapped[i].map.length;j++){
          if(!$scope.parentMapTable[$scope.mvMapJson[0].mapped[i].parentCategory]){
            $scope.parentMapTable[$scope.mvMapJson[0].mapped[i].parentCategory] = [];
          }
          if($scope.mvMapJson[0].mapped[i].map[j].inputField!=null && $scope.mvMapJson[0].mapped[i].map[j].inputField!=""){
            var length = $scope.parentMapTable[$scope.mvMapJson[0].mapped[i].parentCategory].length;
            var obj1 = {};
            obj1[length] = angular.copy($scope.mvMapJson[0].mapped[i]);
            obj1[length].type = obj1[length].type + (length + 1);
            $scope.parentMapTable[$scope.mvMapJson[0].mapped[i].parentCategory].push(obj1);
            break;
          }
        }
        }
    }
        $scope.selectedCategory = $scope.mvMapJson[0].mapped[0].parentCategory;
        return $scope.parentMapTable
  }

    $scope.selectedRecord = 1;
    $scope.selectRecord = function(recordId){
     $scope.selectedRecord =  recordId+1;
    }

    $scope.getSelectedRecord = function(index){
        $scope.selectedOutField = null;
        $scope.selectedOutObject = {};
        $scope.svMapJson = $scope.records[index].sv[0].unmapOtFields;
        $scope.mvMapJson = $scope.records[index].mv;
        $scope.parentMapTable = $scope.records[index].parentMapTable;
        $scope.selectedCategory = $scope.mvMapJson[0].mapped[0].parentCategory;
    }

  $scope.addRecord = function(){
    $scope.records.push(angular.copy($scope.sampleSvMvJson));
    swal("record added successfully");
    $scope.getSelectedRecord($scope.records.length-1);
    $scope.selectedRecord = $scope.records.length;
  }

  $scope.removeRecord = function(index){
     $scope.records.splice(index, 1)
     $scope.getSelectedRecord($scope.records.length-1);
     $scope.selectedRecord = $scope.records.length;
  }

    $scope.showMappings = function(selectedItem){
      $scope.selectedCategory = selectedItem;
    }
    $scope.setMvMappingFields = function(){
  var mvMappedFields = {};
  var value=[];
  for(var key in $scope.parentMapTable){
      mvMappedFields[key]={};
      for(var i=0; i<$scope.parentMapTable[key].length;i++){
        var innerkey=Object.keys($scope.parentMapTable[key][i])[0];
        var str = $scope.parentMapTable[key][i][innerkey].type;
        if (!isNaN($scope.parentMapTable[key][i][innerkey].type.charAt($scope.parentMapTable[key][i][innerkey].type.length-1))) {
          str = str.replace(/[0-9]/g,"");
        }
        for(var j=0;j<$scope.parentMapTable[key][i][innerkey].map.length;j++){
          if(($scope.parentMapTable[key][i][innerkey].map[j].inputField == null) || ($scope.parentMapTable[key][i][innerkey].map[j].inputField == "")){
          }
          else if ($scope.parentMapTable[key][i][innerkey].map[j].groupName) {
            $scope.keyOutField = str+"@"+
            $scope.parentMapTable[key][i][innerkey].map[j].outputField+'#'+
            $scope.parentMapTable[key][i][innerkey].map[j].groupOrder+"@"+i;
            mvMappedFields[key][$scope.keyOutField]=[$scope.parentMapTable[key][i][innerkey].map[j].inputField,$scope.parentMapTable[key][i][innerkey].map[j].trainData,
                                                    $scope.parentMapTable[key][i][innerkey].map[j].filename,
                                                    $scope.parentMapTable[key][i][innerkey].map[j].fileFormat]
          }
          else{
            $scope.keyOutField = str+"@"+$scope.parentMapTable[key][i][innerkey].map[j].outputField+"@"+i;
            mvMappedFields[key][$scope.keyOutField] = [$scope.parentMapTable[key][i][innerkey].map[j].inputField,
            $scope.parentMapTable[key][i][innerkey].map[j].trainData,
            $scope.parentMapTable[key][i][innerkey].map[j].filename,
            $scope.parentMapTable[key][i][innerkey].map[j].fileFormat
            ]
            }
          }
        }
  }
  $('#mydiv').show();
  for (var key in mvMappedFields){
      if(Object.keys(mvMappedFields[key]).length==0){
      delete mvMappedFields[key];
    }
  }
  console.log(JSON.stringify(mvMappedFields));
  return mvMappedFields;
};

    //sv and mv manipulation
    $scope.removeSvMapping = function(col,index){
      if(col){
        var div = document.getElementById($scope.svMapJson[index].selectedTextId);
        $scope.svMapJson[index].inputField = ""
        delete  $scope.svMapJson[index].trainData;
      }
    }
    $scope.singleValueMap = function(svMapVal,isFocus) {
        if($scope.tab==1){
            var index;
            if($scope.selectedOutField && (svMapVal || $scope.selectedText)){
                index = $scope.svMapJson.findIndex( col => col.outputField === $scope.selectedOutField );
            }
            if($scope.isPDF && svMapVal && $scope.selectedOutField){
                $scope.svMapJson[index].inputField = svMapVal;
                $scope.svMapJson[index].trainData = [];
                $scope.svMapJson[index].filename = $scope.filename;;
                $scope.svMapJson[index].fileFormat = 'PDF';
            }
            else if($scope.selectedText && $scope.selectedOutField){
                $scope.svMapJson[index].inputField = $scope.selectedText;
                $scope.svMapJson[index].trainData = $scope.trainObj;
                $scope.svMapJson[index].filename = $scope.filename;
                $scope.svMapJson[index].fileFormat = 'HTML';
            }
            console.log($scope.svMapJson);
            $scope.selectedOutField = "";
            return $scope.selectedText;
        }
    };

    $scope.concatSvSelText = function(index){
        $scope.svMapJson[index].inputField= $scope.svMapJson[index].inputField.concat(" " +$scope.selectedText)
        $scope.svMapJson[index].trainData = $scope.svMapJson[index].trainData.concat($scope.trainObj);
    }

     $scope.concatMvSelText = function(item,indexKey){
     console.log($scope.parentMapTable)
     $scope.mappedData  = $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map
     for(var i=0;i<$scope.mappedData.length;i++){
        if($scope.mappedData[i].outputField ==item.outputField){
            $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map[i].inputField = $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map[i].inputField.concat(" " +$scope.selectedText);
            $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map[i].trainData = $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map[i].trainData.concat($scope.trainObj);
        }
     }
    }

    // input filed selection

    $scope.getSelectedText =  function(){
      $scope.selectionObj = window.getSelection()
      if($scope.selectionObj){
        $scope.selectedText = "";
        $scope.trainObj = [];
        $scope.cloneContents = $scope.selectionObj.getRangeAt(0).cloneContents();
        $scope.selectedText = $scope.cloneContents.textContent;
        $scope.selectedText = $scope.selectedText.replace(/(?:\r\n|\r|\n)/g, '');
        if($scope.cloneContents.children.length!=0){
          for(let i = 0; i < $scope.cloneContents.children.length; i++) {
              orgWrdLen = document.getElementById($scope.cloneContents.children[i].id).textContent.length
              if (i==0)
                    $scope.trainObj.push([$scope.cloneContents.children[i].id, $scope.selectionObj.anchorOffset,orgWrdLen,$scope.cloneContents.children[i].textContent])
              else if (i== $scope.cloneContents.children.length-1)
                   $scope.trainObj.push([$scope.cloneContents.children[i].id, 0, $scope.selectionObj.focusOffset,$scope.cloneContents.children[i].textContent])
              else
                   $scope.trainObj.push([$scope.cloneContents.children[i].id, 0, orgWrdLen, $scope.cloneContents.children[i].textContent]);
           }
        }
        else{
          $scope.cloneContents = window.getSelection().getRangeAt(0);
         $scope.trainObj.push([$scope.cloneContents.endContainer.parentElement.id,
                $scope.selectionObj.anchorOffset, $scope.selectionObj.focusOffset,
                $scope.cloneContents.toString()]);
        }
         console.log($scope.trainObj);
      }
      // OCR selection
      // if(window.getSelection){
      //   $scope.selectedText = window.getSelection().getRangeAt(0).toString();
      //   $scope.selectedText = $scope.selectedText.replace(/\r?\n|\r/g,"");
      //   $scope.selectedText = $scope.selectedText.replace(/\r?\n|\r/g,"");
      //   labObj[$scope.selectedText] = [window.getSelection().baseNode.parentNode.id,window.getSelection().focusNode.parentNode.id]
      // }
      // else if (document.selection && document.selection.type != "Control") {
      //   $scope.selectedText = document.selection.createRange().$scope.selectedText;
      // }
    }

    //asis functions
    $scope.selectAsisItem = function(itemSelected,index){
      if ($scope.asisSelected == itemSelected){ //reference equality should be sufficient
          $scope.asisSelected = {}; //de-select if the same object was re-clicked
          $scope.asisSelectedIndex = null;
      }
      else{
        $scope.selectedIndexKey = null;
        $scope.selectedOutObject = {};
        $scope.asisSelected = itemSelected;
        $scope.asisSelectedIndex = index;
      }
    }
    $scope.selectAddType = function(mapSelected,index,typeSelect){
  if (!isNaN(typeSelect)) {
    for(var key in $scope.parentMapTable){
      if(Object.keys($scope.parentMapTable[key])[0]==$scope.selectedCategory){
        var innerkey = Object.keys($scope.parentMapTable[key][$scope.selectedCategory][typeSelect])[0]
        typeSelect = $scope.parentMapTable[key][$scope.selectedCategory][typeSelect][innerkey].type
      }
    }
  }
    var flag=0;
    for(var key in $scope.typeSelected){
      if(key==$scope.selectedCategory){
        if ($scope.typeSelected[$scope.selectedCategory][index]==index) {
          $scope.typeSelected[$scope.selectedCategory][index]=typeSelect;
          $scope.showType[$scope.selectedCategory][index]=true;
        }
        else {
          $scope.typeSelected[$scope.selectedCategory][index]=typeSelect;
          $scope.showType[$scope.selectedCategory][index]=true;
        }
        flag=1;
      }
    }
    if(flag==0){
      $scope.typeSelected[$scope.selectedCategory]=[];
      $scope.typeSelected[$scope.selectedCategory][index]=typeSelect;
      $scope.showType[$scope.selectedCategory]=[];
      $scope.showType[$scope.selectedCategory][index]=true;
      }
      for(var i=0;i<$scope.parentMapTable[$scope.selectedCategory].length;i++){
        var innerkey = Object.keys($scope.parentMapTable[$scope.selectedCategory][i])[0];
        for (var z = 0; z < $scope.parentMapTable[$scope.selectedCategory][i][innerkey].map.length; z++) {
          if ($scope.parentMapTable[$scope.selectedCategory][i][innerkey].map[z].outputField == mapSelected.outputField) {
            $scope.parentMapTable[$scope.selectedCategory][i][innerkey].map.splice(z,1);
          }
        }

        if($scope.parentMapTable[$scope.selectedCategory][i][innerkey].type == typeSelect){
          for (var z = 0; z < $scope.parentMapTable[$scope.selectedCategory][i][innerkey].map.length; z++) {
            if ($scope.parentMapTable[$scope.selectedCategory][i][innerkey].map[z].outputField == mapSelected.outputField) {
              $scope.parentMapTable[$scope.selectedCategory][i][innerkey].map.splice(z,1);
            }
          }
          mapSelected.type = "AsIs";
          $scope.parentMapTable[$scope.selectedCategory][i][innerkey].map.push(mapSelected);
          delete mapSelected['assocSet'];
        }
      }
  }

$scope.rejectAsisMap = function(selectedItem,typeSelected){
    if (typeSelected==undefined) {
      $scope.mvMapJson[0].suggested.push({"inputField":selectedItem.inputField,"parentCategory":$scope.selectedCategory});
      for(var j=0;j<$scope.mvMapJson[0].mapped.length;j++){
        if($scope.mvMapJson[0].mapped[j].type=="AsIs" && $scope.mvMapJson[0].mapped[j].parentCategory==$scope.selectedCategory){
          for(var k=0;k<$scope.mvMapJson[0].mapped[j].map.length;k++){
            if($scope.mvMapJson[0].mapped[j].map[k].outputField==selectedItem.outputField){
              $scope.mvMapJson[0].mapped[j].map[k].inputField="";
            }
          }
        }
      }
    }
    if(typeSelected!=null && typeSelected!="" && typeSelected!=undefined){
      $scope.mvMapJson[0].suggested.push({"inputField":selectedItem.inputField,"parentCategory":$scope.selectedCategory});
      for(var j=0;j<$scope.mvMapJson[0].mapped.length;j++){
        if($scope.mvMapJson[0].mapped[j].type=="AsIs" && $scope.mvMapJson[0].mapped[j].parentCategory==$scope.selectedCategory){
          for(var k=0;k<$scope.mvMapJson[0].mapped[j].map.length;k++){
            if($scope.mvMapJson[0].mapped[j].map[k].outputField==selectedItem.outputField){
              $scope.mvMapJson[0].mapped[j].map[k].inputField="";
            }
          }
        }
      }
      for(var i=0;i<$scope.parentMapTable[$scope.selectedCategory].length;i++){
        if($scope.parentMapTable[$scope.selectedCategory][i][i].type == typeSelected.typeSelected){
          for(var j=0;j<$scope.parentMapTable[$scope.selectedCategory][i][i].map.length;j++){
            if($scope.parentMapTable[$scope.selectedCategory][i][i].map[j].outputField == selectedItem.outputField){
              $scope.parentMapTable[$scope.selectedCategory][i][i].map.splice(j,1);
            }
          }
        }
      }
    }
  }


  $scope.selectInItem = function (value) {
    if ($scope.selectedInObject ==$scope.selectedText) //reference equality should be sufficient
        $scope.selectedInObject = {}; //de-select if the same object was re-clicked
    else if($scope.isPDF){
        $scope.selectedInObject ={
          "inputField":value,
          "parentCategory":$scope.selectedCategory
        }
    }
    else{
        $scope.selectedInObject ={
          "inputField":$scope.selectedText,
          "parentCategory":$scope.selectedCategory
        }
    }
    };

    $scope.createMap = function(mvMapVal){
//      if($scope.selectedInObject.inputField && $scope.typeSelected)
//      {
     if(Object.keys($scope.asisSelected).length!=0){
      if ($scope.asisSelected.inputField == null || $scope.asisSelected.inputField == "") {
        for(var i=0;i<$scope.parentMapTable[$scope.selectedCategory].length;i++){
          if($scope.parentMapTable[$scope.selectedCategory][i][i].type == $scope.typeSelected[$scope.selectedCategory][$scope.asisSelectedIndex]){
            for(var j=0;j<$scope.parentMapTable[$scope.selectedCategory][i][i].map.length;j++){
              if($scope.parentMapTable[$scope.selectedCategory][i][i].map[j].outputField == $scope.asisSelected.outputField){
                if($scope.isPDF){
                    $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].inputField=mvMapVal;
                    $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].trainData = [];
                    $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].filename = $scope.filename;
                    $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].fileFormat = 'PDF';
                }
                else{
                   $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].inputField=$scope.selectedText;;
                   $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].trainData = $scope.trainObj;
                   $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].filename = $scope.filename;
                   $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].fileFormat = 'HTML';
                }
//                for(var i=0; i<$scope.mvMapJson[0].suggested.length;i++){
//                  if($scope.mvMapJson[0].suggested[i].inputField == $scope.selectedInObject.inputField){
//                    $scope.mvMapJson[0].suggested.splice(i,1);
//                  }
//                }
              }
            }
          }
        }
      }
    }
        else if(Object.keys($scope.selectedOutObject).length!=0) {
            if ($scope.selectedOutObject.inputField == null || $scope.selectedOutObject.inputField == "" || $scope.selectedOutObject.inputField) {
            if($scope.isPDF){
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].inputField = mvMapVal;
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].trainData = [];
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].filename = $scope.filename;
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].fileFormat = 'PDF'
            }
            else{
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].inputField = $scope.selectedText;
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].trainData = $scope.trainObj;
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].filename = $scope.filename;
                $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].fileFormat = "HTML"
            }
            for(var i=0; i<$scope.mvMapJson[0].suggested.length;i++){
              if($scope.mvMapJson[0].suggested[i].inputField == $scope.selectedInObject.inputField){
                $scope.mvMapJson[0].suggested.splice(i,1);
              }
        }
      }
        }
        $scope.selectedIndexKey = null;
        $scope.selectedMapIndex = null;
        $scope.selectedInObject = {};
        $scope.selectedOutObject = {};
        $scope.asisSelected = {};
//  }
}
    $scope.collectType = function(typeSelected){
  for (var i = 0; i < $scope.mvMapJson[0].mapped.length; i++) {
    if ($scope.mvMapJson[0].mapped[i].parentCategory === $scope.selectedCategory && $scope.mvMapJson[0].mapped[i].type === typeSelected) {
      var obj = angular.copy($scope.mvMapJson[0].mapped[i]);
      for(var j=0; j<obj.map.length; j++){
        obj.map[j].inputField = "";
      }
      var obj1 = {};
      var length = $scope.parentMapTable[$scope.selectedCategory].length;
      obj1[length] = obj
      obj1[length].type = obj1[length].type + (length + 1);
      $scope.parentMapTable[$scope.selectedCategory].push(obj1);
      break;
    }
  }
};
    $scope.remove = function(indexSelected){
  for (var i = 0; i < $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.length; i++) {
    if($scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map[i].inputField){
      $scope.mvMapJson[0].suggested.push({inputField:$scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map[i].inputField,parentCategory:$scope.selectedCategory});
    }
  }
  $scope.parentMapTable[$scope.selectedCategory].splice(indexSelected,1);
  for(var i = Number(indexSelected); i<$scope.parentMapTable[$scope.selectedCategory].length;i++){
    $scope.parentMapTable[$scope.selectedCategory][i][i] =   $scope.parentMapTable[$scope.selectedCategory][i][i+1];
    delete $scope.parentMapTable[$scope.selectedCategory][i][i+1];
  }
}


    //groups
    $scope.addGroup = function(groupSelectd,indexSelected){
  var obj = angular.copy(groupSelectd);
  var d = obj[obj.length-1].orderWithinGroup;
  for(var i=0;i<obj.length;i++){
    obj[i].inputField = null;
    obj[i].groupOrder++;
    obj[i].orderWithinGroup = d+1;
    d++;
    $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.push(obj[i]);
  }
}
    $scope.removeGroup = function(groupSelectd,indexSelected){
  for (var i = 0; i < groupSelectd.length; i++) {
    var index = $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.indexOf(groupSelectd[i])
    if (groupSelectd[i].inputField) {
      $scope.mvMapJson[0].suggested.push({inputField:groupSelectd[i].inputField,parentCategory:$scope.selectedCategory});
    }
    $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.splice(index,1);
  }
}

    //    outField slction
    $scope.selectOutItem = function (item,indexKey) {
        var selectedObj = $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map
        if ($scope.selectedOutObject == item){ //reference equality should be sufficient
          $scope.selectedOutObject = {}; //de-select if the same object was re-clicked
            $scope.selectedIndexKey = null;
        }
        else{
            $scope.asisSelectedIndex = null;
            $scope.asisSelected = {}; //de-select if the same object was re-clicked
            $scope.selectedOutObject = item;
            $scope.selectedIndexKey = indexKey;
        }
        for (var i = 0; i < selectedObj.length; i++) {
            if(_.isEqual(selectedObj[i],item))
            {
              $scope.selectedMapIndex = i;
              break;
            }
        }
}

    //    reject map
    $scope.rejectMap = function(item,indexSelected){
  var index = $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.indexOf(item)
  $scope.mvMapJson[0].suggested.push({inputField:item.inputField,parentCategory:$scope.selectedCategory});
  $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map[index].inputField = "";
}


    //output json

    $scope.sendSvMvMappedJson = function(){
    for(var i=0;i<$scope.records.length;i++){
        $scope.finalSvOutput = {};
        $scope.finalMvOutput = {};
          var unmapOutfields = $scope.records[i].sv[0].unmapOtFields;
          for(var j=0;j<unmapOutfields.length;j++){
            if("inputField" in unmapOutfields[j]){
                $scope.finalSvOutput[unmapOutfields[j].outputField] = [unmapOutfields[j].inputField,unmapOutfields[j].trainData,
                unmapOutfields[j].filename,unmapOutfields[j].fileFormat];
            }
          }
      $scope.parentMapTable = $scope.records[i].parentMapTable;
      $scope.finalMvOutput = $scope.setMvMappingFields();
      $scope.recordObj={"sv":$scope.finalSvOutput,"mv":$scope.finalMvOutput}
      $scope.recordResult.push($scope.recordObj);
    }
        mainService.setMvMappingFields($scope.userName,$scope.recordResult,function(response){
            if (response=="success"){
                $state.go("transType");
            }else{
                $state.go("error");
            }
         })
    }
    $scope.selectedOutField = null
    $scope.selectOutField = function(outCol,index){
      $scope.selectedOutField = outCol;
    }
    $scope.updateSvMapping = function(inputValue,index){
        $scope.svMapJson[index].inputField = inputValue;
        $scope.svMapJson[index].edit = true;
    }
    $scope.updateMvMapping = function(item,indexKey){
        $scope.mappedData  = $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map
        for(var i=0;i<$scope.mappedData.length;i++){
        if($scope.mappedData[i].outputField ==item.outputField){
            $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map[i].inputField =item.inputField;
            $scope.parentMapTable[$scope.selectedCategory][indexKey][indexKey].map[i].edit = true;
        }
        }
    }

    $scope.processMapping = function(){
        mainService.saveFileType($scope.isPDF);
        $state.go("nonDelMap");
    }


    $scope.saveUserCmtdNonDelData =  function(){
        mainService.saveUserCmtdNonDelData($scope.records,$scope.sampleSvMvJson);
        mainService.saveSelFileName($scope.filename);
        $state.go('nonDelMapData');
    }

    $scope.copyRecord = function(){
        $scope.records.push(angular.copy($scope.records[$scope.selectedRecord-1]))
        swal("record copied successfully");
        $scope.getSelectedRecord($scope.records.length-1);
        $scope.selectedRecord = $scope.records.length;
    }

     $scope.getSelectedFileData = function(filename){
        $scope.selectedFile = filename;
        $scope.pdfDataList = headers.pdfData;
        mainService.saveSelFileName($scope.selectedFile);
        $scope.htmlDataList = headers.htmlData;
        for(key in $scope.htmlDataList){
            if(key==filename){
                $scope.htmlData = $interpolate($scope.htmlDataList[key])($scope)
                $scope.htmlData = $scope.htmlData.replace(/\r/gm,' ');
                $scope.htmlDataContent= $sce.trustAsHtml($scope.htmlData);
                break;
            }
        }
        for(file in $scope.filesFlagList){
            if($scope.filesFlagList[file].filename==$scope.selectedFile && $scope.filesFlagList[file].tabFlag==false){
              mainService.getSelectedPdfFile(filename,function(response){
                var file = new Blob([response.data], {type: 'application/pdf'});
                var fileURL = URL.createObjectURL(file);
                $scope.content = $sce.trustAsResourceUrl(fileURL);
                $scope.setFileFlags();
              });
            }
            else{
                for(file in $scope.pdfDataList){
                    if($scope.pdfDataList[file].pdfFNm==$scope.selectedFile){
                    var file = new Blob([$scope.pdfDataList[file].pdfData], {type: 'application/pdf'});
                    var fileURL = URL.createObjectURL(file);
                    $scope.content = $sce.trustAsResourceUrl(fileURL);
                }
                }
            }
        }
    }

    $scope.convertHtmlData = function(){

    }

    $scope.convertPDFData = function(){

    }


    $scope.setFileFlags = function(){
        $scope.enableNext = true;
        for(i in $scope.filesFlagList){
            if($scope.filesFlagList[i].filename==$scope.selectedFile){
                $scope.filesFlagList[i].tabFlag = true;
            }
        }
       for( j in $scope.filesFlagList){
            if (!$scope.filesFlagList[j].tabFlag){
               $scope.enableNext = false;
            }
        }
        mainService.saveFileFlags($scope.filesFlagList);
        mainService.saveNextFlag($scope.enableNext);
    }

    $scope.getSelNonDelFileData = function(filename){
    $scope.filename = filename;
    $scope.htmlDataList = headers.htmlData;
    var currnetScale = 1;
    if($scope.isPDF){
        for(fNm in $scope.pdfDataList){
            if($scope.pdfDataList[fNm].pdfFNm == filename){
                var pdfData = $scope.pdfDataList[fNm].pdfData;
                var file = new Blob([pdfData], {type: 'application/pdf'});
                var fileURL = URL.createObjectURL(file);
                $scope.content = $sce.trustAsResourceUrl(fileURL);
                break;
            }
        }
    }
    else{
        for(key in $scope.htmlDataList){
            if(key==filename){
                var myHtml = document.getElementById("htmlContent");
                myHtml.style.transformOrigin = '0 0'
                myHtml.style.transform = `scale(${currnetScale})`
                $scope.htmlData = $interpolate($scope.htmlDataList[key])($scope)
                $scope.htmlData = $scope.htmlData.replace(/\r/gm,' ');
                $scope.htmlDataContent= $sce.trustAsHtml($scope.htmlData);
                break;
            }
        }
    }

    }
})

.filter('unique', function() {
  return function(collection, keyname) {
     var output = [],
         keys = [];

     angular.forEach(collection, function(item) {
         var key = item[keyname];
         if(keys.indexOf(key) === -1) {
             keys.push(key);
             output.push(item);
         }
     });
     return output;
  };
});