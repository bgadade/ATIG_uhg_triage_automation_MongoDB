angular.module("myapp")

.controller("MvAdminMapController", function($scope,mainService,$state) {
  $scope.multiValueMap=[];

  $scope.getMvAdminMap = function(){
    mainService.getMvAdminMap(function(response){
      if (response.status >= 200 && response.status <= 299)
      {
        var obj = {};
        for(var key in response.data){
          var flag =false;
          if(Object.keys(obj).length==0){
            obj[response.data[key].PARENT_CATEGORY] = {};
            obj[response.data[key].PARENT_CATEGORY][key]={};
            var obj2 = {};
            if (response.data[key].Column_Type) {
              for(var i=0;i<response.data[key].Column_Type.length;i++){
                var newObj = [];
                newObj = response.data[key].Column_Type[i].Input_Column;
                obj2[response.data[key].Column_Type[i].Tag] = [];

                for(var z=0;z<newObj.length;z++){
                var newObj1 = {};
                newObj1[response.data[key].Column_Type[i].Tag] = newObj[z];
                  obj2[response.data[key].Column_Type[i].Tag].push(newObj1);
                }
                // obj2[response.data[key].Column_Type[i].Tag]=response.data[key].Column_Type[i].Input_Column;
              }
            }
            else{
              var newObj = [];
              newObj = response.data[key].Input_Column;
              obj2[response.data[key].DATAFRAME_TYPE] = [];

              for(var z=0;z<newObj.length;z++){
              var newObj1 = {};
              newObj1[response.data[key].DATAFRAME_TYPE] = newObj[z];
                obj2[response.data[key].DATAFRAME_TYPE].push(newObj1);
              }
              // obj2[response.data[key].DATAFRAME_TYPE]=response.data[key].Input_Column;
            }

            obj[response.data[key].PARENT_CATEGORY][key]=obj2;
          }
          else{
            for(var abc in obj){
              if(abc == response.data[key].PARENT_CATEGORY){
                flag=true;
                var obj2= {};
                if (response.data[key].Column_Type) {
                  for(var i=0;i<response.data[key].Column_Type.length;i++){
                    var newObj = [];
                    newObj = response.data[key].Column_Type[i].Input_Column;
                    obj2[response.data[key].Column_Type[i].Tag] = [];

                    for(var z=0;z<newObj.length;z++){
                    var newObj1 = {};
                    newObj1[response.data[key].Column_Type[i].Tag] = newObj[z];
                      obj2[response.data[key].Column_Type[i].Tag].push(newObj1);
                    }
                    // obj2[response.data[key].Column_Type[i].Tag]=response.data[key].Column_Type[i].Input_Column;
                  }
                }
                else{
                  var newObj = [];
                  newObj = response.data[key].Input_Column;
                  obj2[response.data[key].DATAFRAME_TYPE] = [];

                  for(var z=0;z<newObj.length;z++){
                  var newObj1 = {};
                  newObj1[response.data[key].DATAFRAME_TYPE] = newObj[z];
                    obj2[response.data[key].DATAFRAME_TYPE].push(newObj1);
                  }
                  // obj2[response.data[key].DATAFRAME_TYPE]=response.data[key].Input_Column;
                }
                obj[abc][key]=obj2;
              }
            }
          }

          if(!flag){
            obj[response.data[key].PARENT_CATEGORY] = {};
            obj[response.data[key].PARENT_CATEGORY][key]={};
            var obj2= {};
            if (response.data[key].Column_Type) {
              for(var i=0;i<response.data[key].Column_Type.length;i++){
                var newObj = [];
                newObj = response.data[key].Column_Type[i].Input_Column;
                obj2[response.data[key].Column_Type[i].Tag] = [];

                for(var z=0;z<newObj.length;z++){
                var newObj1 = {};
                newObj1[response.data[key].Column_Type[i].Tag] = newObj[z];
                  obj2[response.data[key].Column_Type[i].Tag].push(newObj1);
                }
                // obj2[response.data[key].Column_Type[i].Tag]=response.data[key].Column_Type[i].Input_Column;
              }
            }
            else{
              var newObj = [];
              newObj = response.data[key].Input_Column;
              obj2[response.data[key].DATAFRAME_TYPE] = [];

              for(var z=0;z<newObj.length;z++){
              var newObj1 = {};
              newObj1[response.data[key].DATAFRAME_TYPE] = newObj[z];
                obj2[response.data[key].DATAFRAME_TYPE].push(newObj1);
              }
              // obj2[response.data[key].DATAFRAME_TYPE]=response.data[key].Input_Column;
            }
            obj[response.data[key].PARENT_CATEGORY][key]=obj2;
          }
        }
        $scope.multiValueMap.push(obj);
      }
      else {
        $state.go("error");
      }
    });
  };

  $scope.setMvAdminMap = function(){
    if($scope.multiValueMap.length >= 1 && $scope.multiValueMap[0] === undefined){
        $scope.multiValueMap[0] ={};
    }
    mainService.setMvAdminMap($scope.multiValueMap[0],function(response){
      if (response.status >= 200 && response.status <= 299) {
        // $('#setMvMapModal').on('hidden.bs.modal', function (e) {
        // })
        $scope.multiValueMap=[];
        $scope.getMvAdminMap();
      }
      else{
        $state.go("error");
      }
    });
  };

  $scope.editTag = function(inputFields){
    $scope.tagFields=[];
    for(var key in inputFields){
      $scope.tagFields.push(key);
    }
  }

  $scope.onEdit = function(newTypeSelected,mappings){
    var tagSelected = Object.keys(mappings)[0];
    var obj = {};
    obj[newTypeSelected] = mappings[tagSelected];
    $scope.multiValueMap[0][$scope.selectedParent][$scope.selectedChild][newTypeSelected].push(obj)
    var indexSlected = $scope.multiValueMap[0][$scope.selectedParent][$scope.selectedChild][tagSelected].indexOf(mappings);
    $scope.multiValueMap[0][$scope.selectedParent][$scope.selectedChild][tagSelected].splice(indexSlected,1);
  }

  $scope.rmMvOutField = function(mappings){
    var tagSelected = Object.keys(mappings)[0];
    var indexSlected = $scope.multiValueMap[0][$scope.selectedParent][$scope.selectedChild][tagSelected].indexOf(mappings);
    $scope.multiValueMap[0][$scope.selectedParent][$scope.selectedChild][tagSelected].splice(indexSlected,1);
  }

  $scope.selectedParent = null;
  $scope.setActiveTagData = function(parent,child,childTags){
    $scope.activeMapData = null;
    $scope.selectedParent = parent;
    $scope.selectedChild = child;
    $scope.activeTagData = childTags;
  }

  $scope.setActiveMapData = function(mappings){
    $scope.activeMapData = mappings;
  }

})
.directive('dropdownHover', function() {
    return {
      require: 'uibDropdown',
      link: function(scope, element, attrs, dropdownCtrl) {

        var menu = angular.element(element[0].querySelector('.dropdown-menu')),
          button = angular.element(element[0].querySelector('.dropdown-toggle'));

        menu.addClass('dropdown-hover-menu');

        element.bind('mouseenter', onMouseenter);
        element.bind('mouseleave', onMouseleave);
        button.bind('click', onClick);

        function openDropdown(open) {
          scope.$apply(function() {
            dropdownCtrl.toggle(open);
          });
        }

        function onMouseenter(event) {
          if (!element.hasClass('disabled') && !attrs.disabled) {
            openDropdown(true);
          }
        };

        function onMouseleave(event) {
          openDropdown(false);
        };

        function onClick(event) {
          event.stopPropagation();
        }

        scope.$on('$destroy', function() {
          element.unbind('mouseenter', onMouseenter);
          element.unbind('mouseleave', onMouseleave);
          button.unbind('click', onClick);
        });
      }
    };
  });
