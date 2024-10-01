angular.module("myapp")

.controller("MultivalueController", function($scope,mainService,$uibModal,authService,$state,$stateParams) {
  $scope.userName = authService.getUser().name;
  $scope.sheetName =  $stateParams.sheet;
  $scope.selectedInObject = {};
  $scope.selectedOutObject = {};
  $scope.parentMapTable = [];
  $scope.asisSelected = {};
  $scope.defaultMapTable = [];
  $scope.loading = true;
  $scope.typeSelected = {};
  $scope.showType = {};
  $scope.selectedCategory = null;
  $('#mydiv').show();

  $scope.getMVFields = function(){
    $scope.headerValues = mainService.getHeaders().headervalues;
     mainService.getMvMappingFields($scope.sheetName,function(response){
       if (response.status >= 200 && response.status <= 299){
        console.log("Multivalue Mapping:",JSON.stringify(response.data));
        if (response.data.parentMapTable) {
          $scope.typeSelected = response.data.typeSelected;
          $scope.showType = response.data.showType;
        }
        $scope.multivalueMap = response.data.multiValueMap;
        // console.log(JSON.stringify(response.data.multiValueMap));
        $scope.smapleInputData = JSON.parse(response.data.sampleData);
        if (response.data) {
          $('#mydiv').hide();
        }
        for(var i=0;i<$scope.multivalueMap[0].mapped.length;i++){
          if($scope.multivalueMap[0].mapped[i].type!='AsIs'){
            for(var j=0;j<$scope.multivalueMap[0].mapped[i].map.length;j++){
              if($scope.multivalueMap[0].mapped[i].map[j].inputField!=null && $scope.multivalueMap[0].mapped[i].map[j].inputField!=""){
                $scope.defaultMapTable.push($scope.multivalueMap[0].mapped[i]);
                break;
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
        if(Object.keys($scope.parentMapTable[key]))
        {
          var keyvalue=Object.keys($scope.parentMapTable[key])[0];
          mvMappedFields[keyvalue]={};
          for(var i=0; i<$scope.parentMapTable[key][keyvalue].length;i++){
            var innerkey=Object.keys($scope.parentMapTable[key][keyvalue][i])[0];
            var str = $scope.parentMapTable[key][keyvalue][i][innerkey].type;
            if (!isNaN($scope.parentMapTable[key][keyvalue][i][innerkey].type.charAt($scope.parentMapTable[key][keyvalue][i][innerkey].type.length-1))) {
              str = str.replace(/[0-9]/g,"");
            }
            for(var j=0;j<$scope.parentMapTable[key][keyvalue][i][innerkey].map.length;j++){
              if(($scope.parentMapTable[key][keyvalue][i][innerkey].map[j].inputField == null) || ($scope.parentMapTable[key][keyvalue][i][innerkey].map[j].inputField == "")){
              }
              else if ($scope.parentMapTable[key][keyvalue][i][innerkey].map[j].groupName) {
                mvMappedFields[keyvalue][$scope.parentMapTable[key][keyvalue][i][innerkey].map[j].inputField]=str+"@"+
                    $scope.parentMapTable[key][keyvalue][i][innerkey].map[j].outputField+'#'+
                    $scope.parentMapTable[key][keyvalue][i][innerkey].map[j].groupOrder+"@"+i;
              }
              else{
                mvMappedFields[keyvalue][$scope.parentMapTable[key][keyvalue][i][innerkey].map[j].inputField]=str+"@"+
                    $scope.parentMapTable[key][keyvalue][i][innerkey].map[j].outputField+"@"+i;
                // var obj={};
                // obj[$scope.parentMapTable[key][keyvalue][i][innerkey].map[j].inputField]=$scope.parentMapTable[key][keyvalue][i][innerkey].type+"@"+
                //   $scope.parentMapTable[key][keyvalue][i][innerkey].map[j].outputField+"@"+i;
                // mvMappedFields[$scope.parentMapTable[key][keyvalue][i][innerkey].parentCategory].push(obj);
                }
              }
            }
        }
      }
      $('#mydiv').show();
      console.log(JSON.stringify(mvMappedFields));
      mainService.transformMvMappings(mvMappedFields,$scope.sheetName);
      mainService.userCommittedMvMappings($scope.multivalueMap,$scope.parentMapTable,$scope.typeSelected,$scope.showType,$scope.sheetName);
      // mainService.setMvMappingFields(mvMappedFields,$scope.userName,function(response){
      //   if(response == 'success'){
      //     console.log(" setting 1:M mapping Fields:",response);
      //     $state.go("review");
      //   }
      //   else{
      //     $state.go("error");
      //   }
      // });
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

  $scope.selectAsisItem = function(itemSelected,index,selectedCategory){
    // $scope.asisSelected=itemSelected;
    // $scope.asisSelectedIndex=index;
    if ($scope.asisSelected == itemSelected){ //reference equality should be sufficient
        $scope.asisSelected = {}; //de-select if the same object was re-clicked
        $scope.asisSelectedIndex = null;
        $scope.parentCategory = null;
    }
    else{
      $scope.selectedOutObject = {};
      $scope.asisSelected = itemSelected;
      $scope.asisSelectedIndex = index;
      $scope.category = selectedCategory;
    }
  }

  $scope.selectAddType = function(mapSelected,index,typeSelect,categorySlected){
    if (!isNaN(typeSelect)) {
      for(var key in $scope.parentMapTable){
        if(Object.keys($scope.parentMapTable[key])[0]==categorySlected){
          var innerkey = Object.keys($scope.parentMapTable[key][categorySlected][typeSelect])[0]
          typeSelect = $scope.parentMapTable[key][categorySlected][typeSelect][innerkey].type
        }
      }
    }
      var flag=0;
      for(var key in $scope.typeSelected){
        if(key==categorySlected){
          if ($scope.typeSelected[categorySlected][index]==index) {
            $scope.typeSelected[categorySlected][index]=typeSelect;
            $scope.showType[categorySlected][index]=true;
          }
          else {
            $scope.typeSelected[categorySlected][index]=typeSelect;
            $scope.showType[categorySlected][index]=true;
          }
          flag=1;
        }
      }
      if(flag==0){
        $scope.typeSelected[categorySlected]=[];
        $scope.typeSelected[categorySlected][index]=typeSelect
        $scope.showType[categorySlected]=[];
        $scope.showType[categorySlected][index]=true;
        }
        for(var key in $scope.parentMapTable){
          if(Object.keys($scope.parentMapTable[key])[0]==categorySlected){
            var keyValue=key;
          }
        }
        if (keyValue) {
        for(var i=0;i<$scope.parentMapTable[keyValue][categorySlected].length;i++){
          var innerkey = Object.keys($scope.parentMapTable[keyValue][categorySlected][i])[0];

          for (var z = 0; z < $scope.parentMapTable[keyValue][categorySlected][i][innerkey].map.length; z++) {
            if ($scope.parentMapTable[keyValue][categorySlected][i][innerkey].map[z].outputField == mapSelected.outputField) {
              $scope.parentMapTable[keyValue][categorySlected][i][innerkey].map.splice(z,1);
            }
          }

          if($scope.parentMapTable[keyValue][categorySlected][i][innerkey].type == typeSelect){
            // $scope.asisSelected.inputField="";
            for (var z = 0; z < $scope.parentMapTable[keyValue][categorySlected][i][innerkey].map.length; z++) {
              if ($scope.parentMapTable[keyValue][categorySlected][i][innerkey].map[z].outputField == mapSelected.outputField) {
                $scope.parentMapTable[keyValue][categorySlected][i][innerkey].map.splice(z,1);
              }
            }
            mapSelected.type = "AsIs";
            $scope.parentMapTable[keyValue][categorySlected][i][innerkey].map.push(mapSelected);
            delete mapSelected['assocSet'];
          }
        }
      }
      // }
    }

    $scope.rejectAsisMap = function(selectedItem,selectedCategory,typeSelected){
      if (typeSelected==undefined) {
        $scope.multivalueMap[0].suggested.push({"inputField":selectedItem.inputField,"parentCategory":selectedCategory});
        for(var j=0;j<$scope.multivalueMap[0].mapped.length;j++){
          if($scope.multivalueMap[0].mapped[j].type=="AsIs" && $scope.multivalueMap[0].mapped[j].parentCategory==selectedCategory){
            for(var k=0;k<$scope.multivalueMap[0].mapped[j].map.length;k++){
              if($scope.multivalueMap[0].mapped[j].map[k].outputField==selectedItem.outputField){
                $scope.multivalueMap[0].mapped[j].map[k].inputField="";
              }
            }
          }
        }
      }
      if(typeSelected!=null && typeSelected!="" && typeSelected!=undefined){
        $scope.multivalueMap[0].suggested.push({"inputField":selectedItem.inputField,"parentCategory":selectedCategory});
        for(var j=0;j<$scope.multivalueMap[0].mapped.length;j++){
          if($scope.multivalueMap[0].mapped[j].type=="AsIs" && $scope.multivalueMap[0].mapped[j].parentCategory==selectedCategory){
            for(var k=0;k<$scope.multivalueMap[0].mapped[j].map.length;k++){
              if($scope.multivalueMap[0].mapped[j].map[k].outputField==selectedItem.outputField){
                $scope.multivalueMap[0].mapped[j].map[k].inputField="";
              }
            }
          }
        }
        for(var key in $scope.parentMapTable){
          if(Object.keys($scope.parentMapTable[key])[0]==selectedCategory){
            var keyValue=key;
          }
        }
        for(var i=0;i<$scope.parentMapTable[keyValue][selectedCategory].length;i++){
          var innerkey = Object.keys($scope.parentMapTable[keyValue][selectedCategory][i])[0];
          if($scope.parentMapTable[keyValue][selectedCategory][i][innerkey].type == typeSelected.typeSelected){
            for(var j=0;j<$scope.parentMapTable[keyValue][selectedCategory][i][innerkey].map.length;j++){
              if($scope.parentMapTable[keyValue][selectedCategory][i][innerkey].map[j].outputField == selectedItem.outputField){
                $scope.parentMapTable[keyValue][selectedCategory][i][innerkey].map.splice(j,1);
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

  $scope.getOutputObject = function(selectedOutObject,index,typeSelect,mapTable,idx,parentCategory){
    $scope.asisSelected = {};
    $scope.selectedOutObject=selectedOutObject;
    $scope.typeSelect=typeSelect;
    $scope.index=index;
    $scope.mapTable=mapTable;
    $scope.idx=idx;
    $scope.category=parentCategory;
}

  $scope.createMap = function(slectedField){
    $scope.parentCategory = $scope.category;
    if(slectedField==$scope.selectedInObject.inputField && $scope.parentCategory == $scope.selectedInObject.parentCategory)
    if(Object.keys($scope.asisSelected).length!=0 && $scope.typeSelected){
      if ($scope.asisSelected.inputField == null || $scope.asisSelected.inputField == "") {
        for(var j=0;j<$scope.multivalueMap[0].mapped.length;j++){
          if($scope.multivalueMap[0].mapped[j].type=="AsIs" && $scope.multivalueMap[0].mapped[j].parentCategory==$scope.parentCategory){
            for(var k=0;k<$scope.multivalueMap[0].mapped[j].map.length;k++){
              if($scope.multivalueMap[0].mapped[j].map[k].outputField==$scope.asisSelected.outputField){
                $scope.multivalueMap[0].mapped[j].map[k].inputField=$scope.selectedInObject.inputField;
              }
            }
          }
        }
        for(var key in $scope.parentMapTable){
          if(Object.keys($scope.parentMapTable[key])[0] == $scope.parentCategory){
            var keyValue = key;
          }
        }
        if ($scope.parentMapTable[keyValue] != undefined) {
          for(var i=0;i<$scope.parentMapTable[keyValue][$scope.selectedInObject.parentCategory].length;i++){
            var innerkey = Object.keys($scope.parentMapTable[keyValue][$scope.selectedInObject.parentCategory][i])[0];
            if($scope.parentMapTable[keyValue][$scope.selectedInObject.parentCategory][i][innerkey].type == $scope.typeSelected){
              for(var j=0;j<$scope.parentMapTable[keyValue][$scope.selectedInObject.parentCategory][i][innerkey].map.length;j++){
                if($scope.parentMapTable[keyValue][$scope.selectedInObject.parentCategory][i][innerkey].map[j].outputField == $scope.asisSelected.outputField){
                $scope.parentMapTable[keyValue][$scope.selectedInObject.parentCategory][i][innerkey].map[j].inputField=$scope.selectedInObject.inputField;
                }
              }
            }
          }
        }
        for(var i=0; i<$scope.multivalueMap[0].suggested.length;i++){
          if($scope.multivalueMap[0].suggested[i].inputField == $scope.selectedInObject.inputField){
            $scope.multivalueMap[0].suggested.splice(i,1);
          }
        }
        $scope.asisSelected = {};
        $scope.parentCategory = null;
        $scope.selectedInObject = {};
      }
    }
    else if(Object.keys($scope.selectedOutObject).length!=0){
      if ($scope.selectedOutObject.inputField == null || $scope.selectedOutObject.inputField == "") {
        $scope.idx=Object.keys($scope.mapTable)[0]
        $scope.mapTable[$scope.idx].map[$scope.index].inputField=$scope.selectedInObject.inputField;
        $scope.typeSelect[$scope.index].inputField=$scope.selectedInObject.inputField;
        for(var i=0; i<$scope.multivalueMap[0].suggested.length;i++){
          if($scope.multivalueMap[0].suggested[i].inputField == $scope.selectedInObject.inputField){
            $scope.multivalueMap[0].suggested.splice(i,1);
          }
        }
        $scope.selectedOutObject = {};
        $scope.parentCategory = null;
        $scope.selectedInObject = {};
      }
    }
    else{
      $scope.selectedInObject = {};
      // $scope.selectedOutObject = {};
    }
  }
})

.directive('addRowDirective',function($compile){
    return {
      scope: {
        multivalueMap:'=',
        fields :'=',
        parentCategory:'=',
        callEnable:'&',
        removeSelect:'&',
        obj:'=',
        sendOutputObject:'&',
        mapTable:'=',
        addRowIdx:'=',
        parentMapTable: '=',
        createMapTable:'&',
        createParentMapTable:'&',
        removeParentMapTable:'&',
        addGroupTable: '&',
        removeGroupTable: '&',
        defaultSelected:'=',
        parentMap:'=',
        mapToRemove:'=',
      },
      restrict: 'E',
      transclude: true,
      templateUrl:'/static/template/directiveHtml.html' ,
      link: function(scope, element, attrs) {
            scope.outputMap=false;
            scope.selectedOutObject = {};
            scope.newCopyArray=angular.copy(scope.multivalueMap);
            scope.typeSelected=[];
            scope.typeSelect=null;

            scope.collectType = function(item){
              scope.typeSelected=item;
              scope.createMapTable({item: item, obj:scope.obj});
              scope.typeSelect=null;
              for(var key in scope.parentMapTable){
                if(key==item && scope.parentMapTable[key] ==1){
                  for(var i=0;i<scope.multivalueMap[0].mapped.length;i++){
                    if(item == scope.multivalueMap[0].mapped[i].type && scope.parentCategory == scope.multivalueMap[0].mapped[i].parentCategory){
                      var newObjectToInsert=scope.newCopyArray[0].mapped[i];
                      scope.typeSelect=scope.multivalueMap[0].mapped[i].map;
                      scope.orderSelected = scope.multivalueMap[0].mapped[i].order;
                      scope.outputMap=true;
                      break;
                    }
                  }
                }
                else if(key==item && scope.parentMapTable[key]>1){
                  // var flag=false;
                  for(var j=0;j<scope.newCopyArray[0].mapped.length;j++){
                    var flag=false;
                    if(item == scope.newCopyArray[0].mapped[j].type && scope.parentCategory == scope.newCopyArray[0].mapped[j].parentCategory){
                      if(!scope.obj.type){
                        var flagArray = [];
                        for(var k=0;k<scope.newCopyArray[0].mapped[j].map.length;k++){
                          if (scope.newCopyArray[0].mapped[j].map[k].groupName) {
                            if (scope.newCopyArray[0].mapped[j].map[k].groupOrder == 0) {
                              console.log("in if:",JSON.stringify(scope.newCopyArray[0].mapped[j].map[k]));
                              scope.newCopyArray[0].mapped[j].map[k].inputField='';
                            }
                            else{
                              flagArray.push(k);
                            }
                          }
                          else{
                            scope.newCopyArray[0].mapped[j].map[k].inputField='';
                          }
                        }
                        for (var k = 0 ;k<flagArray.length; k++) {
                          var index = flagArray[k] - k;
                          scope.newCopyArray[0].mapped[j].map.splice(index,1);
                        }
                      }
                      else{
                        for(var sheep in scope.parentMap){
                          if(Object.keys(scope.parentMap[sheep])[0]==scope.parentCategory){
                          for(var i=0; i<scope.parentMap[sheep][scope.parentCategory].length;i++){
                            var innerkey=Object.keys(scope.parentMap[sheep][scope.parentCategory][i])[0];
                            var str = scope.parentMap[sheep][scope.parentCategory][i][innerkey].type;
                            if (!isNaN(scope.parentMap[sheep][scope.parentCategory][i][innerkey].type.charAt(scope.parentMap[sheep][scope.parentCategory][i][innerkey].type.length-1))) {
                              str = str.slice(0,-1);
                            }
                            if (str==item && scope.parentMap[sheep][scope.parentCategory][i][innerkey].order==scope.newCopyArray[0].mapped[j].order) {
                              flag=true;
                              break;
                            }
                          }
                        }
                      }
                        //
                        // for (var z = 0; z < scope.parentMap.length; z++) {
                        //   if (scope.parentMap[z].type==item && scope.parentMap[z].order==scope.newCopyArray[0].mapped[j].order) {
                        //     console.log(scope.parentMap[z].type,"",scope.parentMap[z].order,"",scope.newCopyArray[0].mapped[j].order);
                        //   }
                        // }
                      }
                      if (!flag) {
                        scope.newCopyArray[0].mapped[j].type=scope.newCopyArray[0].mapped[j].type+scope.parentMapTable[key];
                        var newObjectToInsert=scope.newCopyArray[0].mapped[j];
                        // scope.multivalueMap[0].mapped.push(newObjectToInsert);
                        scope.typeSelect=newObjectToInsert.map;
                        scope.orderSelected = scope.newCopyArray[0].mapped[j].order;
                        scope.outputMap=true;
                        break;
                      }
                    }
                    // if (flag) {
                    //   break;
                    // }
                  }
                }
              }
              scope.createParentMapTable({item: newObjectToInsert, obj:scope.obj});
            }

            if(scope.obj.type){
              scope.collectType(scope.obj.type);
            }

            scope.makeItEnable=function(index){
              scope.callEnable();
            }

            scope.remove=function(item,typeselect,selectedOrder){
              scope.removeSelect({item:item,obj:scope.obj});
              for(var key in scope.parentMapTable){
                if(key==item && scope.parentMapTable[key] == 0){
                  for(var k=0;k<typeselect.length;k++){
                    if(typeselect[k].inputField!="" && typeselect[k].inputField!=null){
                      scope.multivalueMap[0].suggested.push({inputField:typeselect[k].inputField,parentCategory:scope.parentCategory});
                      typeselect[k].inputField="";
                    }
                    else {
                    }
                  }
                  scope.outputMap=false;
                }
                else if(key==item && scope.parentMapTable[key]>=1){
                  // scope.result = angular.equals(typeselect, scope.multivalueMap[0].mapped[i].map);
                  // console.log(scope.result);
                      for(var k=0;k<typeselect.length;k++){
                        if(typeselect[k].inputField!="" && typeselect[k].inputField!=null){
                          scope.multivalueMap[0].suggested.push({inputField:typeselect[k].inputField,parentCategory:scope.parentCategory});
                          typeselect[k].inputField="";
                        }
                        else {
                        }
                      }
                      // scope.multivalueMap[0].mapped.slice(i,1);
                      // break;
                    // }
                  }
                }
                if(scope.obj.type){
                for (var i = 0; i < scope.multivalueMap[0].mapped.length; i++) {
                  if (scope.multivalueMap[0].mapped[i].parentCategory==scope.parentCategory && scope.multivalueMap[0].mapped[i].type==item && scope.multivalueMap[0].mapped[i].order == selectedOrder) {
                    for (var j = 0; j < scope.multivalueMap[0].mapped[i].map.length; j++) {
                      scope.multivalueMap[0].mapped[i].map[j].inputField ="";
                    }
                    break;
                  }
                }}
            }

            scope.selectOutItem = function (item) {
              for (var i = 0; i < scope.typeSelect.length; i++) {
                if(_.isEqual(scope.typeSelect[i],item))
                {
                  var index = i;
                  break;
                }
              }
              if (scope.selectedOutObject == item) //reference equality should be sufficient
                scope.selectedOutObject = {}; //de-select if the same object was re-clicked
              else{
                scope.selectedOutObject = item;
                scope.sendOutputObject({selectedOutObject:scope.selectedOutObject,index:index,typeSelect:scope.typeSelect,obj:scope.obj});
              }
            }

            scope.rejectMap = function(item){
              index = scope.typeSelect.indexOf(item)
              scope.multivalueMap[0].suggested.push({inputField:item.inputField,parentCategory:scope.parentCategory});
              scope.typeSelect[index].inputField = "";
              scope.removeParentMapTable({item: item, obj:scope.obj,index:index});
            }

            scope.addGroup = function(groupSelectd){
              var obj = angular.copy(groupSelectd);
              var d = obj[obj.length-1].orderWithinGroup;
              for(var i=0;i<obj.length;i++){
                obj[i].inputField = null;
                obj[i].groupOrder++;
                obj[i].orderWithinGroup = d+1;
                d++;
                scope.typeSelect.push(obj[i]);
              }
              scope.addGroupTable({item: obj, selectedObj: scope.obj});
            }

            scope.removeGroup = function(group){
              for (var i = 0; i < group.length; i++) {
                var index = scope.typeSelect.indexOf(group[i])
                if (group[i].inputField) {
                  scope.multivalueMap[0].suggested.push({inputField:group[i].inputField,parentCategory:scope.parentCategory});
                }
                scope.typeSelect.splice(index,1);
              }
              scope.removeGroupTable({item: group, selectedObj: scope.obj});
            }
      }
    }
})

.directive('newDirective',function($compile){
    return {
      scope: {
        fields :'=',
        parentCategory:'=',
        multivalueMap:'=',
        getOutputObject:'&',
        parentMapTable:'=',
        defaultMapTable:'='
      },
      restrict: 'E',
      transclude: true,
      template:'<div class="col-sm-6 pull-light">'+
      '<button class="btn glyphicon glyphicon-plus addBtnRow" name="button" add-html ng-click="addSelectButton(null);" ng-disabled="rowButton"></button>'+
      '</div>',
      link: function(scope, element, attrs) {
      scope.rowButton=true;
      scope.addSelect=[];
      scope.count=0;
      scope.obj={};
      scope.obj.count=scope.count;
      scope.addSelect.push(scope.obj);
      scope.mapTable={};
      scope.outputMap=false;
      scope.parentMap={};
      scope.childMapTable={};
      scope.mapToRemove={};

      // scope.parentMapTable=[];

      scope.addSelectButton=function(type){
        scope.obj={};
        scope.obj.count=scope.count+1;
        scope.obj.type=type;
        scope.addSelect.push(scope.obj);
        scope.rowButton=true;
      }

      scope.createParentMapTable = function(item, obj){
        var indexOfObj= scope.addSelect.indexOf(obj);
        var boolean =false;
        for(var key in scope.parentMapTable){
          if(Object.keys(scope.parentMapTable[key])[0] == scope.parentCategory){
            var category = scope.parentCategory;
            scope.parentMap = {};
            scope.parentMap[indexOfObj]= item;
            scope.parentMapTable[key][category].push(scope.parentMap);
            boolean=true;
          }
        }
        if(!boolean){
          var parent={};
          parent[scope.parentCategory]=[];
          scope.parentMap = {};
          scope.parentMap[indexOfObj]= item;
          parent[scope.parentCategory].push(scope.parentMap);
          scope.parentMapTable.push(parent);
        }
      }

      scope.createMapTable=function(item, obj){
        var childBoolean=false;
         if(Object.keys(scope.childMapTable).length==0){
           scope.childMapTable[item]=1;
         }else{
           for(var key in scope.childMapTable){
             if(key==item){
               childBoolean=true;
               scope.childMapTable[key]=scope.childMapTable[key]+1;
             }
           }
           if(childBoolean==false){
             scope.childMapTable[item]=1;
           }
         }
      };

        scope.removeSelectButton= function(item,obj){
          var idx=scope.addSelect.indexOf(obj);
          for(var key in scope.parentMapTable){
            if(Object.keys(scope.parentMapTable[key])[0]==scope.parentCategory){
              scope.mapToRemove = scope.parentMapTable[key][scope.parentCategory][idx];
              scope.parentMapTable[key][scope.parentCategory].splice(idx,1);
            }
          }
          // scope.parentMapTable.splice(idx,1);
          for(var key in scope.childMapTable){
            if(key==item){
              scope.childMapTable[key]=scope.childMapTable[key]-1;
            }
          }
          scope.addSelect.splice(idx,1);
        }

        scope.changeDisableToEnable=function(){
          scope.rowButton=false;
        }

        scope.sendOutputObject = function(selectedOutObject,index,typeSelect,obj){
          var idx=scope.addSelect.indexOf(obj);
          for(var key in scope.parentMapTable){
            if(Object.keys(scope.parentMapTable[key])[0]==scope.parentCategory){
              var keyValue=key;
            }
          }
            scope.getOutputObject({selectedOutObject:selectedOutObject,index:index,typeSelect:typeSelect,mapTable:scope.parentMapTable[keyValue][scope.parentCategory][idx],idx:idx,parentCategory:scope.parentCategory});
        }

        scope.removeParentMapTable = function(item,obj,index){
          var idx=scope.addSelect.indexOf(obj);
          for(var key in scope.parentMapTable){
            if(Object.keys(scope.parentMapTable[key])[0]==scope.parentCategory){
              var keyValue=key;
            }
          }
          scope.parentMapTable[keyValue][scope.parentCategory][idx][idx].map[index].inputField="";
        }

        scope.addGroupTable = function(item,selectedObj){
          // if (selectedObj.type != null) {
            var idx=scope.addSelect.indexOf(selectedObj);
            for(var key in scope.parentMapTable){
              if(Object.keys(scope.parentMapTable[key])[0]==scope.parentCategory){
                var keyValue=key;
              }
            }
            for (var i = 0; i < item.length; i++) {
              if (_.indexOf(scope.parentMapTable[keyValue][scope.parentCategory][idx][idx].map,item[i]) == -1) {
                scope.parentMapTable[keyValue][scope.parentCategory][idx][idx].map.push(item[i]);
              }
            }
          // }
        }

        scope.removeGroupTable = function(item,selectedObj){
          if (selectedObj.type != null) {
            var idx=scope.addSelect.indexOf(selectedObj);
            for(var key in scope.parentMapTable){
              if(Object.keys(scope.parentMapTable[key])[0]==scope.parentCategory){
                var keyValue=key;
              }
            }
            for (var i = 0; i < item.length; i++) {
              var indexItem = scope.parentMapTable[keyValue][scope.parentCategory][idx][idx].map.indexOf(item[i]);
              scope.parentMapTable[keyValue][scope.parentCategory][idx][idx].map.splice(indexItem,1);
            }
          }
        }

        scope.onPageLoad = function(){
          scope.defaultSelected=[];
          var counter=true;
          for(var i=0;i<scope.defaultMapTable.length;i++){
            if(scope.defaultMapTable[i].parentCategory==scope.parentCategory){
              if (counter) {
                scope.addSelect.splice(0,1);
                counter = false;
              }
              scope.addSelectButton(scope.defaultMapTable[i].type);
              // scope.createMapTable(scope.defaultMapTable[i].type,scope.obj);
              // scope.createParentMapTable(scope.defaultMapTable[i],scope.obj);
              scope.defaultSelected.push(scope.defaultMapTable[i]);
            }
          }
          if(scope.defaultSelected.length!=0)
            scope.addSelectButton(null);
        }
        scope.onPageLoad();

        $addRowDirective = angular.element("<add-row-directive obj='obj' parent-map='parentMapTable' default-selected='defaultSelected' parent-map-table='childMapTable' create-parent-map-table='createParentMapTable(item,obj)' remove-parent-map-table='removeParentMapTable(item,obj,index)' create-map-table='createMapTable(item,obj)' add-group-table=addGroupTable(item,selectedObj) remove-group-table=removeGroupTable(item,selectedObj) map-table='mapTable' send-output-object='sendOutputObject(selectedOutObject,index,typeSelect,obj)' ng-repeat='obj in addSelect ' call-enable='changeDisableToEnable();' remove-select='removeSelectButton(item,obj);' multivalue-map='multivalueMap' fields='fields' parent-category='parentCategory' map-to-remove='mapToRemove'></add-row-directive>");
        element.prepend($addRowDirective);
        $compile( $addRowDirective )(scope);
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

        // $scope.multivalueMap[0].suggested.push({"inputField":Object.keys(itemsSelected)[i],"parentCategory":parentCategory});
      }
    }
    $scope.parentCategory = null;
  }
  $scope.cancel = function () {
    $uibModalInstance.dismiss('cancel');
  };
});
