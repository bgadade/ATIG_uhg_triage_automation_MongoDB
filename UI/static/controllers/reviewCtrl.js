angular.module("myapp")

.controller("ReviewController", function($scope,mainService,$uibModal,$state) {

  // CollapsibleLists.applyTo(document.getElementById('newList'));

  $scope.navbarCollapsed = true;
  $scope.selectedColName = null;
  $scope.selectedSheet = null;

  $scope.addressList = {};

  $scope.token = mainService.getHeaders().token;
  $scope.getOutputData = function(){
    mainService.getOutputData(function(response){
      $scope.rowsLimit = 20;
      $scope.altJson = response.data;
      console.log(JSON.stringify($scope.altJson));
      for (var key in $scope.altJson){
         if($scope.altJson[key]===null || $scope.altJson[key]===undefined){
            delete $scope.altJson[key]
         }
      }
      $scope.selectedSheet = Object.keys($scope.altJson)[0];
      $scope.selectedColName = Object.keys($scope.altJson[$scope.selectedSheet])[0];
      $scope.altJson = $scope.getNextRows($scope.altJson, {}, 0, $scope.rowsLimit)
      });
  }

  $scope.updateOutput = function(value,index,data,selectedColName){
    data[index]=value;
    $scope.errorRows.splice($scope.errorRows.indexOf(selectedColName),1);
  }


  $scope.myPagingFunction = function(activeTab){
    $scope.addressList = $scope.getNextRows($scope.altJson, $scope.addressList, $scope.addressList[activeTab]["dataSet"].length, $scope.rowsLimit, activeTab)
  }
  $scope.arrangeHeaders = function(){
     $(".tableDiv table").each(function(){
           var table = this;
           var counter = 0;
         $(this).find("thead tr th").each(function(idx){
             var width = $(this).width();
             var tdx = $(table).find('tr td:eq(' + idx + ')')

             var w = tdx.width();
             if (w > width) {
                  width = w;
             }
             $(this).width(width);
             tdx.width(width);
             counter++;
         });
         $( '.tableDiv table thead tr th:last-child').width('3px');
       });
  }

  $scope.getNextRows = function(dataSource, dataTarget, begin, count, tab){
    var applyForTabs = tab ? [tab] : Object.keys(dataSource)

    applyForTabs.forEach(function(tab, idx){
      // to make sure ColsSet avail in target Data
      if (!dataTarget[tab] && dataSource[tab]!=null) {
        dataTarget[tab] = {}
        dataTarget[tab]["data"] = []
        dataTarget[tab]["data"]["colSet"] = dataSource[tab]["data"]["colSet"]
        var filteredSourceData = dataSource[tab]["data"]["dataSet"].slice(begin, begin + count);
        dataTarget[tab]["data"]["dataSet"] = dataTarget[tab]["data"]["dataSet"].concat(filteredSourceData)
      }
      else{
        dataTarget[tab] = null;
      }
    })
    return dataTarget
  }

  $scope.gnrtOtpt=false;
  $scope.setOutputData = function(e){
    e.preventDefault();
    fnlAray = []
    var obj = {}
    obj[$scope.selectedSheet] = $scope.altJson[$scope.selectedSheet]
    fnlAray.push(obj)
    var btn_href = $('#dwnldFile').attr("href", "/getDwnldfile/").attr("href");
    $scope.countErr();
    mainService.setOutputData(fnlAray,function(response){
      if (response.status >= 200 && response.status <= 299){
        $("#generateModal").modal('show');
        $scope.gnrtOtpt=true;
        $scope.outputGnrtd = response.data;
        $scope.outputFile = null;
           for(var i=0;i<$scope.outputGnrtd.length;i++){
               $scope.outputFile = $scope.outputGnrtd[i][$scope.selectedSheet];
               if($scope.outputFile!=null)
                   break;
           }
           $('#dwnldFile').attr("href",btn_href+$scope.outputFile+"/");
           $("#dwnldFile")[0].click()
      }
      else {
        $state.go("error");
      }
    });
  }
$scope.downloadZipFile = function(e){
    e.preventDefault();
    var btn_href = $('#dwnldAllFile').attr("href", "/getDwnldAllfile/").attr("href");
    mainService.downloadAllOutputData($scope.altJson,function(response){
      if (response.status >= 200 && response.status <= 299){
        $scope.outputFile = response.data
        $('#dwnldAllFile').attr("href",btn_href+$scope.outputFile+"/");
        $("#dwnldAllFile")[0].click()
      }
      else {
        $state.go("error");
      }
    });
  }

  $scope.showSheet = function(item,subItem){
    $scope.selectedSheet = item;
    if (subItem)
      $scope.selectedColName = subItem;
      // $scope.selectedColName = item;
    else
      $scope.selectedColName = Object.keys($scope.altJson[item])[0];
  }
  $scope.errorRows = [];

  $scope.errorOutput = function(item,row){
    if (item!=undefined && item!=null && isNaN(item)) {
      if(item.includes("Clarify:")){
        if ($scope.errorRows.indexOf(row)==-1) {
          $scope.errorRows.push(row);
        }
        return true;
      }
      else if (item.includes("Reject:")) {
        if ($scope.errorRows.indexOf(row)==-1) {
          $scope.errorRows.push(row);
        }
        return true;
      }
      else if (item.includes("Error:")) {
        if ($scope.errorRows.indexOf(row)==-1) {
          $scope.errorRows.push(row);
        }
        return true;
      }
      else
        return false;
    }
  }

  $scope.setColor = function (row) {
    if ($scope.errorRows.indexOf(row)!=-1) {
      return {color: "#EE6C4D", height: "40px"}
    }
  }

  $scope.errorColumns = function(row){
    if ($scope.errorRows.indexOf(row)!=-1) {
      return true;
    }
  }

  $scope.countErr = function(){
    $scope.clarifyCount = 0;
    $scope.errorCount = 0;
    $scope.rejectCount = 0;

    for(var sheet in $scope.altJson){
      for(var key in $scope.altJson[sheet]){
        for (var i = 0; i < $scope.altJson[sheet][key].dataSet.length; i++) {
          for (var j = 0; j < $scope.altJson[sheet][key].dataSet[i].length; j++) {
            var item=$scope.altJson[sheet][key].dataSet[i][j];
            if (item!=undefined && item!=null && isNaN(item)) {
              if(item.includes("Clarify:"))
                $scope.clarifyCount++;
              else if (item.includes("Reject:")) {
                $scope.rejectCount++;
              }
              else if (item.includes("Error:")) {
                $scope.errorCount++;
              }
            }
          }
        }
      }
    }
  }
})

.directive('keyNavigation', function ($timeout) {
    return function (scope, element, attrs) {
        element.bind("keydown keypress", function (event) {
            if (event.which === 38) {
                var target = $(event.target).prev();
                $(target).trigger('focus');
            }
            if (event.which === 40) {
                var target = $(event.target).next();
                $(target).trigger('focus');
            }
        });
    };
})

.directive('onScrollToBottom', function ($document,$parse) {
    return {
        restrict: 'A',
        link: function (scope, element, attrs) {
          //  var doc = angular.element($document)[0].body;
            element.on("scroll", function () {
              var scrollBottom = element.scrollTop() + element[0]['clientHeight']
                if(scrollBottom >= (element[0].scrollHeight - 100))
                {
                  scope.$apply(attrs.onScrollToBottom);
                  // $parse(attrs.onScrollToBottom)(scope);
                }
            });
        }
    };
})


.directive('onFinishRender',['$timeout', '$parse', function ($timeout, $parse) {
    return {
        restrict: 'A',
        link: function (scope, element, attr) {
            if (scope.$last === true) {
                $timeout(function () {
                    scope.$emit('ngRepeatFinished');
                    if(!!attr.onFinishRender){
                      $parse(attr.onFinishRender)(scope);
                    }
                });
            }
        }
    }
}]);
// .directive('postRender', [ '$timeout', function($timeout) {
// var def = {
//     restrict : 'A',
//     terminal : true,
//     transclude : true,
//     link : function(scope, element, attrs) {
//       debugger;
//       $(document).ready(function(){
//         debugger;
//           $(".tableDiv table").each(function(){
//             debugger;
//             var that = this;
//             var counter = 0;
//           $(this).find("thead tr th").each(function(){
//             var width = $(this).width();
//               debugger;
//               $(that).find("tr").each(function(){
//                 var w = $(that).find('tr td:eq(' + counter + ')').width();
//                 if (w > width) {
//                   width = w;
//                 }
//               });
//               // $(".NewHeader thead tr").append(this);
//               debugger;
//               this.width = width;
//               // $(".MyTable tr th").width(width);
//               // $(".NewHeader tr th").width(width);
//               // $('.MyTable tr:last td:eq(' + counter + ')').width(width);
//               counter++;
//           });});
//     });
//   }
// };
// return def;
// }])
// .directive('fixedHeader', fixedHeader);
//
//     fixedHeader.$inject = ['$timeout'];
//
//     function fixedHeader($timeout) {
//         return {
//             restrict: 'A',
//             link: link
//         };
//
//         function link($scope, $elem, $attrs, $ctrl) {
//             var elem = $elem[0];
//
//             // wait for data to load and then transform the table
//             $scope.$watch(tableDataLoaded, function(isTableDataLoaded) {
//                 if (isTableDataLoaded) {
//                     transformTable();
//                 }
//             });
//
//             function tableDataLoaded() {
//                 // first cell in the tbody exists when data is loaded but doesn't have a width
//                 // until after the table is transformed
//                 var firstCell = elem.querySelector('tbody tr:first-child td:first-child');
//                 return firstCell && !firstCell.style.width;
//             }
//
//             function transformTable() {
//                 // reset display styles so column widths are correct when measured below
//                 angular.element(elem.querySelectorAll('thead, tbody, tfoot')).css('display', '');
//
//                 // wrap in $timeout to give table a chance to finish rendering
//                 $timeout(function () {
//                     // set widths of columns
//                     angular.forEach(elem.querySelectorAll('tr:first-child th'), function (thElem, i) {
//
//                         var tdElems = elem.querySelector('tbody tr:first-child td:nth-child(' + (i + 1) + ')');
//                         var tfElems = elem.querySelector('tfoot tr:first-child td:nth-child(' + (i + 1) + ')');
//
//                         var columnWidth = tdElems ? tdElems.offsetWidth : thElem.offsetWidth;
//                         if (tdElems) {
//                             tdElems.style.width = columnWidth + 'px';
//                         }
//                         if (thElem) {
//                             thElem.style.width = columnWidth + 'px';
//                         }
//                         if (tfElems) {
//                             tfElems.style.width = columnWidth + 'px';
//                         }
//                     });
//
//                     // set css styles on thead and tbody
//                     angular.element(elem.querySelectorAll('thead, tfoot')).css('display', 'block');
//
//                     angular.element(elem.querySelectorAll('tbody')).css({
//                         'display': 'block',
//                         'height': $attrs.tableHeight || 'inherit',
//                         'overflow': 'auto'
//                     });
//
//                     // reduce width of last column by width of scrollbar
//                     var tbody = elem.querySelector('tbody');
//                     var scrollBarWidth = tbody.offsetWidth - tbody.clientWidth;
//                     if (scrollBarWidth > 0) {
//                         // for some reason trimming the width by 2px lines everything up better
//                         scrollBarWidth -= 2;
//                         var lastColumn = elem.querySelector('tbody tr:first-child td:last-child');
//                         lastColumn.style.width = (lastColumn.offsetWidth - scrollBarWidth) + 'px';
//                     }
//                 });
//             }
//         }
//     }
