angular.module("myapp")

.controller("MultivalueControllerAlter", function($scope,mainService,$uibModal,authService,$state,$stateParams) {
  $scope.userName = authService.getUser().name;
  $scope.sheetName =  $stateParams.sheet;
  $scope.selectedInObject = {};
  $scope.selectedOutObject = {};
  $scope.parentMapTable = {};
  $scope.asisSelected = {};
  $scope.typeSelected = {};
  $scope.showType = {};
  $scope.selectedCategory = null;
  $('#mydiv').show();

  $scope.getMVFields = function(){
    $scope.headerValues = mainService.getHeaders().headervalues;
     mainService.getMvMappingFields($scope.sheetName,function(response){
       if (response.status >= 200 && response.status <= 299){
        // console.log("Multivalue Mapping:",JSON.stringify(response.data));
        $scope.multivalueMap = response.data.multiValueMap;
        $scope.smapleInputData = JSON.parse(response.data.sampleData);
        if (response.data) {
          $('#mydiv').hide();
        }
        if (response.data.parentMapTable) {
          $scope.parentMapTable = response.data.parentMapTable;
          $scope.typeSelected = response.data.typeSelected;
          $scope.showType = response.data.showType;
        }
        else{
          for(var i=0;i<$scope.multivalueMap[0].mapped.length;i++){
            if($scope.multivalueMap[0].mapped[i].type!='AsIs'){
              for(var j=0;j<$scope.multivalueMap[0].mapped[i].map.length;j++){
                if(!$scope.parentMapTable[$scope.multivalueMap[0].mapped[i].parentCategory]){
                  $scope.parentMapTable[$scope.multivalueMap[0].mapped[i].parentCategory] = [];
                }
                if($scope.multivalueMap[0].mapped[i].map[j].inputField!=null && $scope.multivalueMap[0].mapped[i].map[j].inputField!=""){
                  var length = $scope.parentMapTable[$scope.multivalueMap[0].mapped[i].parentCategory].length;
                  var obj1 = {};
                  obj1[length] = angular.copy($scope.multivalueMap[0].mapped[i]);
                  obj1[length].type = obj1[length].type + (length + 1);
                  $scope.parentMapTable[$scope.multivalueMap[0].mapped[i].parentCategory].push(obj1);
                  break;
                }
              }
            }
          }
        }
        $scope.selectedCategory = $scope.multivalueMap[0].mapped[0].parentCategory;
       }
       else {
         $state.go("error");
       }
     });
  }

  $scope.showMappings = function(selectedItem){
    $scope.selectedCategory = selectedItem;
  }

  $scope.clearMappings = function(){
    mainService.clearMappings($scope.sheetName);
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
                mvMappedFields[key][$scope.parentMapTable[key][i][innerkey].map[j].inputField]=str+"@"+
                    $scope.parentMapTable[key][i][innerkey].map[j].outputField+'#'+
                    $scope.parentMapTable[key][i][innerkey].map[j].groupOrder+"@"+i;
              }
              else{
                mvMappedFields[key][$scope.parentMapTable[key][i][innerkey].map[j].inputField]=str+"@"+
                    $scope.parentMapTable[key][i][innerkey].map[j].outputField+"@"+i;
                // var obj={};
                // obj[$scope.parentMapTable[key][keyvalue][i][innerkey].map[j].inputField]=$scope.parentMapTable[key][keyvalue][i][innerkey].type+"@"+
                //   $scope.parentMapTable[key][keyvalue][i][innerkey].map[j].outputField+"@"+i;
                // mvMappedFields[$scope.parentMapTable[key][keyvalue][i][innerkey].parentCategory].push(obj);
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
      mainService.transformMvMappings(mvMappedFields,$scope.sheetName);
      mainService.userCommittedMvMappings($scope.multivalueMap,$scope.parentMapTable,$scope.typeSelected,$scope.showType,$scope.sheetName);
  };

  $scope.userCommittedMvMappings = function(){
    mainService.userCommittedMvMappings($scope.multivalueMap,$scope.parentMapTable,$scope.typeSelected,$scope.showType,$scope.sheetName);
  };

  $scope.openAddMoreWindow = function (size) {
    $scope.parentCategory = size;
    var modalInstance = $uibModal.open({
      templateUrl: 'addMoreFields.html',
      controller: 'AddMoreCtrl',
      scope: $scope,
      resolve: {
        items: function () {
          return $scope.multivalueMap;
        }
      }
    });
  };

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
        $scope.multivalueMap[0].suggested.push({"inputField":selectedItem.inputField,"parentCategory":$scope.selectedCategory});
        for(var j=0;j<$scope.multivalueMap[0].mapped.length;j++){
          if($scope.multivalueMap[0].mapped[j].type=="AsIs" && $scope.multivalueMap[0].mapped[j].parentCategory==$scope.selectedCategory){
            for(var k=0;k<$scope.multivalueMap[0].mapped[j].map.length;k++){
              if($scope.multivalueMap[0].mapped[j].map[k].outputField==selectedItem.outputField){
                $scope.multivalueMap[0].mapped[j].map[k].inputField="";
              }
            }
          }
        }
      }
      if(typeSelected!=null && typeSelected!="" && typeSelected!=undefined){
        $scope.multivalueMap[0].suggested.push({"inputField":selectedItem.inputField,"parentCategory":$scope.selectedCategory});
        for(var j=0;j<$scope.multivalueMap[0].mapped.length;j++){
          if($scope.multivalueMap[0].mapped[j].type=="AsIs" && $scope.multivalueMap[0].mapped[j].parentCategory==$scope.selectedCategory){
            for(var k=0;k<$scope.multivalueMap[0].mapped[j].map.length;k++){
              if($scope.multivalueMap[0].mapped[j].map[k].outputField==selectedItem.outputField){
                $scope.multivalueMap[0].mapped[j].map[k].inputField="";
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

  $scope.selectInItem = function (item) {
      if ($scope.selectedInObject == item) //reference equality should be sufficient
          $scope.selectedInObject = {}; //de-select if the same object was re-clicked
      else
          $scope.selectedInObject = item;
  };

  $scope.createMap = function(slectedField){
    if(slectedField==$scope.selectedInObject.inputField && $scope.typeSelected){
      if(Object.keys($scope.asisSelected).length!=0){
        if ($scope.asisSelected.inputField == null || $scope.asisSelected.inputField == "") {
          for(var i=0;i<$scope.parentMapTable[$scope.selectedCategory].length;i++){
            if($scope.parentMapTable[$scope.selectedCategory][i][i].type == $scope.typeSelected[$scope.selectedCategory][$scope.asisSelectedIndex]){
              for(var j=0;j<$scope.parentMapTable[$scope.selectedCategory][i][i].map.length;j++){
                if($scope.parentMapTable[$scope.selectedCategory][i][i].map[j].outputField == $scope.asisSelected.outputField){
                  $scope.parentMapTable[$scope.selectedCategory][i][i].map[j].inputField=$scope.selectedInObject.inputField;
                  for(var i=0; i<$scope.multivalueMap[0].suggested.length;i++){
                    if($scope.multivalueMap[0].suggested[i].inputField == $scope.selectedInObject.inputField){
                      $scope.multivalueMap[0].suggested.splice(i,1);
                    }
                  }
                }
              }
            }
          }
        }
      }
      else if (Object.keys($scope.selectedOutObject).length!=0) {
        if ($scope.selectedOutObject.inputField == null || $scope.selectedOutObject.inputField == "") {
          $scope.parentMapTable[$scope.selectedCategory][$scope.selectedIndexKey][$scope.selectedIndexKey].map[$scope.selectedMapIndex].inputField = slectedField;
          for(var i=0; i<$scope.multivalueMap[0].suggested.length;i++){
            if($scope.multivalueMap[0].suggested[i].inputField == $scope.selectedInObject.inputField){
              $scope.multivalueMap[0].suggested.splice(i,1);
            }
          }
        }
      }
      $scope.selectedIndexKey = null;
      $scope.selectedMapIndex = null;
      $scope.selectedInObject = {};
      $scope.selectedOutObject = {};
      $scope.asisSelected = {};
    }
  }

  $scope.collectType = function(typeSelected){
    for (var i = 0; i < $scope.multivalueMap[0].mapped.length; i++) {
      if ($scope.multivalueMap[0].mapped[i].parentCategory === $scope.selectedCategory && $scope.multivalueMap[0].mapped[i].type === typeSelected) {
        var obj = angular.copy($scope.multivalueMap[0].mapped[i]);
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
        $scope.multivalueMap[0].suggested.push({inputField:$scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map[i].inputField,parentCategory:$scope.selectedCategory});
      }
    }
    $scope.parentMapTable[$scope.selectedCategory].splice(indexSelected,1);
    for(var i = Number(indexSelected); i<$scope.parentMapTable[$scope.selectedCategory].length;i++){
      $scope.parentMapTable[$scope.selectedCategory][i][i] =   $scope.parentMapTable[$scope.selectedCategory][i][i+1];
      delete $scope.parentMapTable[$scope.selectedCategory][i][i+1];
    }
  }

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
        $scope.multivalueMap[0].suggested.push({inputField:groupSelectd[i].inputField,parentCategory:$scope.selectedCategory});
      }
      $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.splice(index,1);
    }
  }

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

  $scope.rejectMap = function(item,indexSelected){
    var index = $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map.indexOf(item)
    $scope.multivalueMap[0].suggested.push({inputField:item.inputField,parentCategory:$scope.selectedCategory});
    $scope.parentMapTable[$scope.selectedCategory][indexSelected][indexSelected].map[index].inputField = "";
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
})

.controller("AddMoreCtrl",function($scope, $uibModal, $uibModalInstance, items){
  $scope.items = items;
  $scope.formData={};
  $scope.addFields = function(itemsSelected,parentCategory){
    var flag=true;
    var values=[];
    console.log("itemsSelected:",itemsSelected);
    var values = Object.keys(itemsSelected).map(function(key) {
        return itemsSelected[key];
    })
    for(var i=0;i<Object.keys(itemsSelected).length;i++){
      flag=true;
      if(values[i]==true){
        for(var j=0;j<$scope.items[0].suggested.length;j++){
          if($scope.items[0].suggested[j].inputField==Object.keys(itemsSelected)[i]){
            $scope.items[0].suggested[j].parentCategory=parentCategory;
            flag=false;
          }
        }
        if(flag) {
            $scope.items[0].suggested.push({inputField:Object.keys(itemsSelected)[i],parentCategory:parentCategory});
            for (var j = 0; j < $scope.items[0].unmappedInpCols.length; j++) {
              if ($scope.items[0].unmappedInpCols[j]==Object.keys(itemsSelected)[i]) {
                $scope.items[0].unmappedInpCols.splice(j,1);
              }
            }
        }
      }
    }
    $scope.parentCategory = null;
  }
  $scope.cancel = function () {
    $uibModalInstance.dismiss('cancel');
  };
});
