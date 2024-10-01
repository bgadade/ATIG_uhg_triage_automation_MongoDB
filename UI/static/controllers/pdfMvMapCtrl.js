angular.module("myapp")
    .controller("pdfMVMapController", function($scope,mainService,$state,authService) {

        //variable declaration
        $scope.mvUserData = {};
        $scope.pCatFlag = false;
        $scope.showPcatDetails = false;
        $scope.selectMvOutItem = null;
        $scope.textline = "False"
        $scope.mvUserData.grptextline = "False"
        $scope.userName=authService.getUser().name;
        $scope.checkedLoopID = 1
        $scope.minXInputFields = [{}];
        $scope.minXTextVal = [];
        $scope.maxXInputFields = [{}];
        $scope.maxXTextVal = [];
        $scope.minYInputFields = [{}];
        $scope.minYTextVal = [];
        $scope.maxYInputFields = [{}];
        $scope.delTextVal_1 = [];
        $scope.delInputfields_1 = [{}]
        $scope.maxYTextVal = [];
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

        $scope.allData= null;
        $scope.getPdfMvData = function() {
            $scope.allData = mainService.getPDFInfo();
            $scope.parentCategory = $scope.allData.parentCategories;
            $scope.mVData = $scope.allData.multivalue;
            $scope.tableData = $scope.allData.tableInfo;
            $scope.loopData = $scope.allData.loopInfo;
        }

        $scope.getParCatDetails = function(pCat) {
            $scope.showPcatDetails = true;
            $scope.detailTypeFlag=true;
            $scope.parentCatDetails = {};
            $scope.mappedTags = [];
            $scope.allOutFields = [];
            $scope.allTags = [];
            $scope.selectedTag = null;
            $scope.selectOutItem = null;
            $scope.selectMvOutItem = null;
            $scope.detailType = null;
            $scope.tableId = null;
            $scope.columnIndex = null;
            $scope.mvUnmappedColList = [];
            $scope.mappedMvOutCols = [];
            for (var i = 0; i < $scope.mVData.multivalue.length; i++) {
                if ($scope.mVData.multivalue[i].pCat == pCat) {
                    $scope.parentCatDetails = $scope.mVData.multivalue[i];
                    $scope.allOutFields = $scope.parentCatDetails.allOutFields;
                    $scope.allTags = $scope.parentCatDetails.allTags;
                    for (var j = 0; j < $scope.parentCatDetails.mapped.length; j++) {
                        $scope.mappedTags.push($scope.parentCatDetails.mapped[j].tag);
                    }
                    if($scope.mappedTags.length!=0){
                        $scope.selectedTag = $scope.mappedTags[0];
                        $scope.fillTagDetails(pCat, $scope.selectedTag,0)
                        $scope.detailTypeFlag=false;
                    }
                    console.log($scope.mvUnmappedColList);
                }
            }
            $scope.pCatFlag = true;
            $scope.addNewFlag = false;
            $scope.reset()
        }

        $scope.table_minXInputFields = [{}];
        $scope.table_minXTextVal = [];
        $scope.table_maxXInputFields = [{}];
        $scope.table_maxXTextVal = [];
        $scope.table_minYInputFields = [{}];
        $scope.table_minYTextVal = [];
        $scope.table_maxYInputFields = [{}];
        $scope.table_maxYTextVal = [];

        $scope.fillMvTableDetails = function(pCat, tid) {
            //        $scope.tableInfo =mainService.getPDFInfo().tableInfo;
            $scope.tableInfo = $scope.tableData.tableInfo;
            for (var i = 0; i < $scope.tableInfo.length; i++) {
                if ($scope.tableInfo[i].pCat == pCat) {
                    for (var j = 0; j < $scope.tableInfo[i].mapping.length; j++) {
                        if ($scope.tableInfo[i].mapping[j].tableId == tid) {
                            $scope.type = $scope.tableInfo[i].mapping[j].type;
                            $scope.tableId = $scope.tableInfo[i].mapping[j].tableId;
                            $scope.tableHeader = $scope.tableInfo[i].mapping[j].tableHeader;
                            $scope.tableXMargin = $scope.tableInfo[i].mapping[j].xMargin;

                            $scope.table_minXTextVal = $scope.tableInfo[i].mapping[j].parentBB.min_X.text;
                            if ($scope.table_minXTextVal == "None") {
                                $scope.table_minXTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.table_minXTextVal.length; k++) {
                                if ($scope.table_minXInputFields.length == $scope.table_minXTextVal.length) {
                                    break;
                                }
                                $scope.table_minXInputFields.push({});
                            }


                            $scope.tableminXPoint = $scope.tableInfo[i].mapping[j].parentBB.min_X.point
                            $scope.tableminXOccurence = $scope.tableInfo[i].mapping[j].parentBB.min_X.occurence;

                            $scope.table_maxXTextVal = $scope.tableInfo[i].mapping[j].parentBB.max_X.text;
                            if ($scope.table_maxXTextVal == "None") {
                                $scope.table_maxXTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.table_maxXTextVal.length; k++) {
                                if ($scope.table_maxXInputFields.length == $scope.table_maxXTextVal.length) {
                                    break;
                                }
                                $scope.table_maxXInputFields.push({});
                            }

                            $scope.tablemaxXPoint = $scope.tableInfo[i].mapping[j].parentBB.max_X.point;
                            $scope.tablemaxXOccurence = $scope.tableInfo[i].mapping[j].parentBB.max_X.occurence;

                            $scope.table_minYTextVal = $scope.tableInfo[i].mapping[j].parentBB.min_Y.text;
                            if ($scope.table_minYTextVal == "None") {
                                $scope.table_minYTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.table_minYTextVal.length; k++) {
                                if ($scope.table_minYInputFields.length == $scope.table_minYTextVal.length) {
                                    break;
                                }
                                $scope.table_minYInputFields.push({});
                            }

                            $scope.tableminYPoint = $scope.tableInfo[i].mapping[j].parentBB.min_Y.point;
                            $scope.tableminYOccurence = $scope.tableData.tableInfo[i].mapping[j].parentBB.min_Y.occurence;

                            $scope.table_maxYTextVal = $scope.tableInfo[i].mapping[j].parentBB.max_Y.text;
                            if ($scope.table_maxYTextVal == "None") {
                                $scope.table_maxYTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.table_maxYTextVal.length; k++) {
                                if ($scope.table_maxYInputFields.length == $scope.table_maxYTextVal.length) {
                                    break;
                                }
                                $scope.table_maxYInputFields.push({});
                            }

                            $scope.tablemaxYPoint = $scope.tableInfo[i].mapping[j].parentBB.max_Y.point;
                            $scope.tablemaxYOccurence = $scope.tableInfo[i].mapping[j].parentBB.max_Y.occurence;

                        }
                    }
                }
            }
        }

        $scope.loop_minXInputFields = [{}];
        $scope.loop_minXTextVal = [];
        $scope.loop_maxXInputFields = [{}];
        $scope.loop_maxXTextVal = [];
        $scope.loop_minYInputFields = [{}];
        $scope.loop_minYTextVal = [];
        $scope.loop_maxYInputFields = [{}];
        $scope.loop_maxYTextVal = [];


        $scope.fillMVLoopDetails = function(pCat, lId) {
            $scope.loopInfo = mainService.getPDFInfo().loopInfo;
            $scope.loopInfo = angular.copy($scope.loopInfo["loopInfo"])
            for (var i = 0; i < $scope.loopInfo.length; i++) {
                if ($scope.loopInfo[i].pCat == pCat) {
                    for (var j = 0; j < $scope.loopInfo[i].mapping.length; j++) {
                        if ($scope.loopInfo[i].mapping[j].loopId == lId) {
                            $scope.loopId = $scope.loopInfo[i].mapping[j].loopId;
                            $scope.loopDirection = $scope.loopInfo[i].mapping[j].direction;
                            $scope.loopType = $scope.loopInfo[i].mapping[j].type;
                            $scope.delTextVal_1 = $scope.loopInfo[i].mapping[j].delimiter[1]

                              for (var k = 0; k < $scope.delTextVal_1.length; k++) {
                                if ($scope.delInputfields_1.length == $scope.delTextVal_1.length) {
                                    break;
                                }
                                $scope.delInputfields_1.push({});
                            }
                            $scope.loop_minXTextVal = $scope.loopInfo[i].mapping[j].parentBB.min_X.text;
                            if ($scope.loop_minXTextVal == "None") {
                                $scope.loop_minXTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.loop_minXTextVal.length; k++) {
                                if ($scope.loop_minXInputFields.length == $scope.loop_minXTextVal.length) {
                                    break;
                                }
                                $scope.loop_minXInputFields.push({});
                            }


                            $scope.loopminXPoint = $scope.loopInfo[i].mapping[j].parentBB.min_X.point
                            $scope.loopminXOccurence = $scope.loopInfo[i].mapping[j].parentBB.min_X.occurence;

                            $scope.loop_maxXTextVal = $scope.loopInfo[i].mapping[j].parentBB.max_X.text;
                            if ($scope.loop_maxXTextVal == "None") {
                                $scope.loop_maxXTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.loop_maxXTextVal.length; k++) {
                                if ($scope.loop_maxXInputFields.length == $scope.loop_maxXTextVal.length) {
                                    break;
                                }
                                $scope.loop_maxXInputFields.push({});
                            }

                            $scope.loopmaxXPoint = $scope.loopInfo[i].mapping[j].parentBB.max_X.point;
                            $scope.loopmaxXOccurence = $scope.loopInfo[i].mapping[j].parentBB.max_X.occurence;

                            $scope.loop_minYTextVal = $scope.loopInfo[i].mapping[j].parentBB.min_Y.text;
                            if ($scope.loop_minYTextVal == "None") {
                                $scope.loop_minYTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.loop_minYTextVal.length; k++) {
                                if ($scope.loop_minYInputFields.length == $scope.loop_minYTextVal.length) {
                                    break;
                                }
                                $scope.loop_minYInputFields.push({});
                            }

                            $scope.loopminYPoint = $scope.loopInfo[i].mapping[j].parentBB.min_Y.point;
                            $scope.loopminYOccurence = $scope.loopInfo[i].mapping[j].parentBB.min_Y.occurence;

                            $scope.loop_maxYTextVal = $scope.loopInfo[i].mapping[j].parentBB.max_Y.text;
                            if ($scope.loop_maxYTextVal == "None") {
                                $scope.loop_maxYTextVal = ["None"]
                            }
                            for (var k = 0; k < $scope.loop_maxYTextVal.length; k++) {
                                if ($scope.loop_maxYInputFields.length == $scope.loop_maxYTextVal.length) {
                                    break;
                                }
                                $scope.loop_maxYInputFields.push({});
                            }

                            $scope.loopmaxYPoint = $scope.loopInfo[i].mapping[j].parentBB.max_Y.point;
                            $scope.loopmaxYOccurence = $scope.loopInfo[i].mapping[j].parentBB.max_Y.occurence;

                        }
                    }
                }
            }
        }

        $scope.addNewFlag = false;
        $scope.addNewTag = function() {
            $scope.addNewFlag = true;
        }

        $scope.detailTypeFlag = true;
        $scope.addTag = function(selectedTag, mvParCat) {
            if ($scope.parentCatDetails.pCat == mvParCat && selectedTag) {
                $scope.parentCatDetails.mapped.push({
                    "tagMapping": [],
                    detailType: "None",
                    tag: selectedTag
                });
                $scope.mappedTags.push(selectedTag);
                $scope.selectedMvTag = selectedTag;
                index = $scope.mappedTags.length-1
                $scope.fillTagDetails(mvParCat, $scope.selectedMvTag,index )
            }
            else{
                swal("Please select the Tag");
            }
         $scope.detailTypeFlag = false;
        }

        $scope.selectedIndex = 0;
        $scope.selectedMvTag = null;
        $scope.selectMvTag = function(item, index) {
            for (var i = 0; i < $scope.mappedTags.length; i++) {
                if ($scope.mappedTags[i] == item) {
                    $scope.selectedMvTag = $scope.mappedTags[index];
                    break;
                }
            }
            $scope.selectedIndex = index;
            $scope.reset();
        }

        $scope.mvTagFlag = false;
        $scope.mvSelectedTagIdx=null;
        $scope.fillTagDetails = function(mvParCat, mappedTag, index) {
            $scope.mvSelectedTagIdx = index;
            $scope.mvTagFlag = true;
            $scope.detailTypeFlag = false;
            console.log(mvParCat, mappedTag)
            $scope.mappedMvOutCols = [];
            $scope.mvUnmappedColList = [];
            if (mappedTag == $scope.parentCatDetails.mapped[index].tag) {
                $scope.selectedTag = $scope.parentCatDetails.mapped[index].tag;
                $scope.detailType = $scope.parentCatDetails.mapped[index].detailType;
                for (var j = 0; j < $scope.parentCatDetails.mapped[index].tagMapping.length; j++) {
                    $scope.mappedMvOutCols.push($scope.parentCatDetails.mapped[index].tagMapping[j].outField)
                }

                $scope.mvUnmappedColList = $scope.parentCatDetails.allOutFields.filter(i1 => !$scope.mappedMvOutCols.some(i2 => i1 === i2));
                $scope.tableIds = $scope.getTableIdDetails(mvParCat);
                $scope.loopIDs = $scope.getLoopIdDetails(mvParCat);
                $scope.addNewFlag = true;
                $scope.selectMvOutItem = null;
                $scope.reset();
            }
        }


        $scope.removeTab = function(index, mappedTag, mvParCat) {
            $scope.removeTagFlag = false;
            if ($scope.parentCatDetails.pCat == mvParCat) {
                if ($scope.parentCatDetails.mapped[index].tag == mappedTag) {
                    $scope.parentCatDetails.mapped.splice(index, 1)
                    $scope.mvUnmappedColList = [];
                    $scope.detailType = null;
                    $scope.removeTagFlag = true;
                    $scope.detailTypeFlag = true;
                }
            }
            console.log($scope.parentCatDetails);
            if ($scope.removeTagFlag) {
                $scope.mappedTags.splice(index, 1);
            }
            console.log($scope.mVData.mapped);
        }

        $scope.selectMvOutField = function(item) {
            if (item) {
                $scope.selectMvOutItem = item;
            } else {
                $scope.selectMvOutItem = null;
            }
        }

        $scope.fillOutFields = function(mvParCat) {
            $scope.reset();
            $scope.minXInputFields = [{}]
            $scope.maxXInputFields = [{}]
            $scope.minYInputFields = [{}]
            $scope.maxYInputFields = [{}]
            if (mvParCat && $scope.selectedTag && $scope.selectMvOutItem) {
                    if ($scope.selectedTag == $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tag) {
                        for (var j = 0; j < $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping.length; j++) {
                            if ($scope.selectMvOutItem == $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].outField) {
                                $scope.detailType = Object.keys($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping)[0]
                                if($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB){
                                    $scope.textline = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.textline;
                                    if($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.start){
                                        $scope.mvUserData.mvStartText = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.start.text[0];
                                    }

                                    $scope.minXTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_X.text;
                                    $scope.mvUserData.minXPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_X.point;
                                    $scope.mvUserData.minXOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_X.occurence;

                                    $scope.minXTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_X.text;
                                    if ($scope.minXTextVal == "None") {
                                        $scope.minXTextVal = ["None"]
                                    }
                                    for (var k = 0; k < $scope.minXTextVal.length; k++) {
                                            if ($scope.minXInputFields.length == $scope.minXTextVal.length) {
                                                break;
                                            }
                                            $scope.minXInputFields.push({});
                                    }
                                        $scope.maxXTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.max_X.text;
                                        if ($scope.maxXTextVal == "None") {
                                            $scope.maxXTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.maxXTextVal.length; k++) {
                                            if ($scope.maxXInputFields.length == $scope.maxXTextVal.length) {
                                                break;
                                            }
                                            $scope.maxXInputFields.push({});
                                        }

                                        $scope.mvUserData.maxXPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.max_X.point;
                                        $scope.mvUserData.maxXOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.max_X.occurence;

                                        $scope.minYTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_Y.text;
                                        if ($scope.minYTextVal == "None") {
                                            $scope.minYTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.minYTextVal.length; k++) {
                                            if ($scope.minYInputFields.length == $scope.minYTextVal.length) {
                                                break;
                                            }
                                            $scope.minYInputFields.push({});
                                        }

                                        $scope.mvUserData.minYPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_Y.point;
                                        $scope.mvUserData.minYOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.min_Y.occurence;

                                        $scope.maxYTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.max_Y.text;
                                        if ($scope.maxYTextVal == "None") {
                                            $scope.maxYTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.maxYTextVal.length; k++) {
                                            if ($scope.maxYInputFields.length == $scope.maxYTextVal.length) {
                                                break;
                                            }
                                            $scope.maxYInputFields.push({});
                                        }

                                        $scope.mvUserData.maxYPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.max_Y.point;
                                        $scope.mvUserData.maxYOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.parentBB.max_Y.occurence;

                                        $scope.setGrpDetails($scope.mvSelectedTagIdx,j);
                                    }
                                    else if($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails){
                                        $scope.checkedLoopID =$scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.loopId
                                        $scope.textline = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.textline;
                                        $scope.minXTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationX.min_X.text;
                                        $scope.mvUserData.minXPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationX.min_X.point;
                                        $scope.mvUserData.minXOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationX.min_X.occurence;

                                        if ($scope.minXTextVal == "None") {
                                            $scope.minXTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.minXTextVal.length; k++) {
                                            if ($scope.minXInputFields.length == $scope.minXTextVal.length) {
                                                break;
                                            }
                                            $scope.minXInputFields.push({});
                                        }
                                        $scope.maxXTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationX.max_X.text;
                                        if ($scope.maxXTextVal == "None") {
                                            $scope.maxXTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.maxXTextVal.length; k++) {
                                            if ($scope.maxXInputFields.length == $scope.maxXTextVal.length) {
                                                break;
                                            }
                                            $scope.maxXInputFields.push({});
                                        }

                                        $scope.mvUserData.maxXPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationX.max_X.point;
                                        $scope.mvUserData.maxXOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationX.max_X.occurence;

                                        $scope.minYTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationY.min_Y.text;
                                        if ($scope.minYTextVal == "None") {
                                            $scope.minYTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.minYTextVal.length; k++) {
                                            if ($scope.minYInputFields.length == $scope.minYTextVal.length) {
                                                break;
                                            }
                                            $scope.minYInputFields.push({});
                                        }

                                        $scope.mvUserData.minYPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationY.min_Y.point;
                                        $scope.mvUserData.minYOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationY.min_Y.occurence;

                                        $scope.maxYTextVal = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationY.max_Y.text;
                                        if ($scope.maxYTextVal == "None") {
                                            $scope.maxYTextVal = ["None"]
                                        }
                                        for (var k = 0; k < $scope.maxYTextVal.length; k++) {
                                            if ($scope.maxYInputFields.length == $scope.maxYTextVal.length) {
                                                break;
                                            }
                                            $scope.maxYInputFields.push({});
                                        }

                                        $scope.mvUserData.maxYPoint = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationY.max_Y.point;
                                        $scope.mvUserData.maxYOccurence = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.loopDetails.locationY.max_Y.occurence;
                                        $scope.setGrpDetails($scope.mvSelectedTagIdx,j);
                                    }
                                    else if($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.tableDetails){
                                        $scope.mvUserData.columnIndex = $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.tableDetails.columnIndex;
                                        $scope.mvUserData.tableId =$scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].mapping.tableDetails.tableId
                                        $scope.setGrpDetails($scope.mvSelectedTagIdx,j);
                                    }
                                }
                            }
                    }
            }
        }
        $scope.removeMinX = function(index) {
            if($scope.minXInputFields.length>1){
                //Remove the item from inputFields using Index.
                $scope.minXInputFields.splice(index, 1);
                $scope.minXTextVal.splice(index, 1)
            }else{
                swal("There should be minimum one text");
            }
        }

        $scope.removeMaxX = function(index) {
        if($scope.maxXInputFields.length>1){
            //Remove the item from inputFields using Index.
            $scope.maxXInputFields.splice(index, 1);
            $scope.maxXTextVal.splice(index, 1)
         }else{
                swal("There should be minimum one text");
         }
        }
        $scope.removeMinY = function(index) {
        if($scope.minYInputFields.length>1){
            //Remove the item from inputFields using Index.
            $scope.minYInputFields.splice(index, 1);
            $scope.minYTextVal.splice(index, 1)
           }
           else{
                swal("There should be minimum one text");
            }
        }
        $scope.removeMaxY = function(index) {
            //Remove the item from inputFields using Index.
               if($scope.maxYInputFields.length>1){
                    $scope.maxYInputFields.splice(index, 1);
                    $scope.maxYTextVal.splice(index, 1)
                }
                else{
                swal("There should be minimum one text");
            }

        }

        $scope.checkNoneCond = function() {

            if ($scope.minXTextVal[0].toLowerCase() == 'none') {
                $scope.minXTextVal = "None";
                $scope.mvUserData.minXOccurence = "None";
            }

            if ($scope.maxXTextVal[0].toLowerCase() == 'none') {
                $scope.maxXTextVal = "None";
                $scope.mvUserData.maxXOccurence = "None";
            }

            if ($scope.minYTextVal[0].toLowerCase() == 'none') {
                $scope.minYTextVal = "None";
                $scope.mvUserData.minYOccurence = "None";
            }

            if ($scope.maxYTextVal[0].toLowerCase() == 'none') {
                $scope.maxYTextVal = "None";
                $scope.mvUserData.maxYOccurence = "None";
            }
        }

        $scope.selectTabs = function(tag) {
            $scope.selectedTab = tag;
        }


        $scope.showMappings = function(selectedItem) {
            $scope.selectedCategory = selectedItem;
        }


        //groupDetails
        $scope.groupInputFields = [{}];
        $scope.grpTextVal = [];

        $scope.addGrpText = function() {
            $scope.groupInputFields.push([{}]);
        }
        $scope.removeGrpText = function(index) {
            if($scope.groupInputFields.length>1){
                //Remove the item from inputFields using Index.
                $scope.groupInputFields.splice(index, 1);
                $scope.grpTextVal.splice(index, 1)
            }else{
                swal("There should be minimum one text");
            }
        }
        $scope.grpMinXInputFields = [{}];
        $scope.minXGrpTextVal = [];
        $scope.grpMinXaddText = function() {
            $scope.grpMinXInputFields.push([{}]);
        }

        $scope.grpMaxXInputFields = [{}];
        $scope.grpMaxXTextVal = [];


        $scope.grpMaxXaddText = function() {
            $scope.grpMaxXInputFields.push([{}]);
        }

        $scope.grpMinYInputFields = [{}];
        $scope.grpMinYTextVal = [];

        $scope.grpMinYaddText = function() {
            $scope.grpMinYInputFields.push([{}]);
        }

        $scope.grpMaxYInputFields = [{}];
        $scope.grpMaxYTextVal = [];

        $scope.grpMaxYaddText = function() {
            $scope.grpMaxYInputFields.push([{}]);
        }

        $scope.groupFlag = false;

        $scope.showAddGroup = true;

        $scope.GrpTextLineFlag = false;

        $scope.addGroupDetailFlag = function() {
            $scope.groupFlag = true;
            $scope.GrpTextLineFlag = true;
            $scope.showAddGroup = false;
        }

        $scope.removeGroup = function() {
            $scope.showAddGroup = true;
            $scope.groupFlag = false;
            $scope.GrpTextLineFlag = false;
        }

        $scope.removeGrpMinX = function(index) {
            if($scope.grpMinXInputFields.length>1){
                //Remove the item from inputFields using Index.
                $scope.grpMinXInputFields.splice(index, 1);
                $scope.minXGrpTextVal.splice(index, 1)
            }else{
                swal("There should be minimum one text");
            }
        }

        $scope.removeGrpMaxX = function(index) {
            if($scope.grpMaxXInputFields.length>1){
                //Remove the item from inputFields using Index.
                $scope.grpMaxXInputFields.splice(index, 1);
                $scope.grpMaxXTextVal.splice(index, 1)
            }
            else{
                swal("There should be minimum one text");
            }
        }
        $scope.removeGrpMinY = function(index) {
            if($scope.grpMaxXInputFields.length>1){
                //Remove the item from inputFields using Index.
                $scope.grpMinYInputFields.splice(index, 1);
                $scope.grpMinYTextVal.splice(index, 1)
            }
            else{
                swal("There should be minimum one text");
            }
        }
        $scope.removeGrpMaxY = function(index) {
            if($scope.grpMaxYInputFields.length>1){
                //Remove the item from inputFields using Index.
                $scope.grpMaxYInputFields.splice(index, 1);
                $scope.grpMaxYTextVal.splice(index, 1)
            }
            else{
            swal("There should be minimum one text");
        }
        }

        $scope.checkOccurence = function() {

            if ($scope.mvUserData.minXOccurence != "None" && (/^\d+$/.test($scope.mvUserData.minXOccurence))) {
                $scope.mvUserData.minXOccurence = parseInt($scope.mvUserData.minXOccurence)
            }
            if ($scope.mvUserData.maxYOccurence != "None" && (/^\d+$/.test($scope.mvUserData.maxYOccurence))) {
                $scope.mvUserData.maxYOccurence = parseInt($scope.mvUserData.maxYOccurence)
            }
            if ($scope.mvUserData.minYOccurence != "None" && (/^\d+$/.test($scope.mvUserData.minYOccurence))) {
                $scope.mvUserData.minYOccurence = parseInt($scope.mvUserData.minYOccurence)
            }
            if ($scope.mvUserData.maxXOccurence != "None" && (/^\d+$/.test($scope.mvUserData.maxYOccurence))) {
                $scope.mvUserData.maxXOccurence = parseInt($scope.mvUserData.maxXOccurence)
            }
        }
        $scope.saveMultiValueMap = function(mvParCat,checkedLoopID) {
            if ($scope.detailType != "None" && $scope.mvParCat && $scope.selectedTag) {
                $scope.referenceJson = angular.copy($scope.mVData.referenceJson)
                $scope.tableDetailsFlag = false;

                if ($scope.groupFlag) {
                    $scope.referenceJson.groupDetails.groupText = $scope.grpTextVal;
                    $scope.referenceJson.groupDetails.textline = $scope.mvUserData.grptextline;

                    $scope.referenceJson.groupDetails.bbox.min_X.text = $scope.minXGrpTextVal;
                    $scope.referenceJson.groupDetails.bbox.min_X.point = $scope.mvUserData.grpMinXPoint;
                    $scope.referenceJson.groupDetails.bbox.min_X.ref = $scope.mvUserData.minxReference;


                    $scope.referenceJson.groupDetails.bbox.max_X.text = $scope.grpMaxXTextVal;
                    $scope.referenceJson.groupDetails.bbox.max_X.point = $scope.mvUserData.grpMaxXPoint;
                    $scope.referenceJson.groupDetails.bbox.max_X.ref = $scope.mvUserData.maxXReference;

                    $scope.referenceJson.groupDetails.bbox.min_Y.text = $scope.grpMinYTextVal;
                    $scope.referenceJson.groupDetails.bbox.min_Y.point = $scope.mvUserData.grpMinYPoint;
                    $scope.referenceJson.groupDetails.bbox.min_Y.ref = $scope.mvUserData.minYReference;

                    $scope.referenceJson.groupDetails.bbox.max_Y.text = $scope.grpMaxYTextVal;
                    $scope.referenceJson.groupDetails.bbox.max_Y.point = $scope.mvUserData.grpMaxYPoint;
                    $scope.referenceJson.groupDetails.bbox.max_Y.ref = $scope.mvUserData.maxYReference;

                    $scope.showAddGroup = true;
                }
                if ($scope.detailType == 'parentBB') {
                    $scope.parentBBFlag = true;
                    $scope.checkNoneCond($scope.minXTextVal[0], $scope.maxXTextVal[0], $scope.minYTextVal[0], $scope.maxYTextVal[0]);
                    $scope.checkOccurence();
                    if($scope.mvUserData.mvStartText){
                        $scope.referenceJson.parentBB.start.text[0] = $scope.mvUserData.mvStartText;
                    }
                    $scope.referenceJson.textline = $scope.textline
                    $scope.referenceJson.parentBB.min_X.text = $scope.minXTextVal;
                    $scope.referenceJson.parentBB.min_X.point = $scope.mvUserData.minXPoint;
                    $scope.referenceJson.parentBB.min_X.occurence = $scope.mvUserData.minXOccurence;

                    $scope.referenceJson.parentBB.max_X.text = $scope.maxXTextVal;
                    $scope.referenceJson.parentBB.max_X.point = $scope.mvUserData.maxXPoint;
                    $scope.referenceJson.parentBB.max_X.occurence = $scope.mvUserData.maxXOccurence;

                    $scope.referenceJson.parentBB.min_Y.text = $scope.minYTextVal;
                    $scope.referenceJson.parentBB.min_Y.point = $scope.mvUserData.minYPoint;
                    $scope.referenceJson.parentBB.min_Y.occurence = $scope.mvUserData.minYOccurence;

                    $scope.referenceJson.parentBB.max_Y.text = $scope.maxYTextVal;
                    $scope.referenceJson.parentBB.max_Y.point = $scope.mvUserData.maxYPoint;
                    $scope.referenceJson.parentBB.max_Y.occurence = $scope.mvUserData.maxYOccurence;
                }
                if ($scope.detailType == 'loopDetails') {
                    $scope.checkOccurence();
                    $scope.loopDetailFlag = true;
                    if(!$scope.loopId){
                        $scope.loopId = checkedLoopID;
                    }
                    $scope.referenceJson.loopDetails.loopId = $scope.loopId;
                    $scope.referenceJson.loopDetails.locationX.min_X.text = $scope.minXTextVal;
                    $scope.referenceJson.loopDetails.locationX.min_X.point = $scope.mvUserData.minXPoint;
                    $scope.referenceJson.loopDetails.locationX.min_X.occurence = $scope.mvUserData.minXOccurence;


                    $scope.referenceJson.loopDetails.locationX.max_X.text = $scope.maxXTextVal;
                    $scope.referenceJson.loopDetails.locationX.max_X.point = $scope.mvUserData.maxXPoint;
                    $scope.referenceJson.loopDetails.locationX.max_X.occurence = $scope.mvUserData.maxXOccurence;

                    $scope.referenceJson.loopDetails.locationY.min_Y.text = $scope.minYTextVal;
                    $scope.referenceJson.loopDetails.locationY.min_Y.point = $scope.mvUserData.minYPoint;
                    $scope.referenceJson.loopDetails.locationY.min_Y.occurence = $scope.mvUserData.minYOccurence;

                    $scope.referenceJson.loopDetails.locationY.max_Y.text = $scope.maxYTextVal;
                    $scope.referenceJson.loopDetails.locationY.max_Y.point = $scope.mvUserData.maxYPoint;
                    $scope.referenceJson.loopDetails.locationY.max_Y.occurence = $scope.mvUserData.maxYOccurence;

                }

                if ($scope.detailType == 'tableDetails') {
                    $scope.tableDetailsFlag = true;
                    $scope.referenceJson.tableDetails.tableId = parseInt($scope.tableId)
                    $scope.referenceJson.tableDetails.columnIndex = parseInt($scope.mvUserData.columnIndex);
                }

                if ($scope.selectMvOutItem && $scope.mvParCat && $scope.selectedTag && $scope.detailType) {
                    if ($scope.parentBBFlag) {
                        $scope.parentBBMap = $scope.referenceJson.parentBB;
                    }
                    if ($scope.groupFlag) {
                        $scope.groupDetailsMap = $scope.referenceJson.groupDetails;
                        $scope.groupFlag = false;
                        $scope.GrpTextLineFlag = false;
                    } else {
                        $scope.groupDetailsMap = "None";
                    }
                    if ($scope.loopDetailFlag) {
                        $scope.loopDetailsMap = $scope.referenceJson.loopDetails;
                    } else {
                        $scope.loopDetailsMap = "None";
                    }
                    if ($scope.tableDetailsFlag) {
                        $scope.tableDetailsMap = $scope.referenceJson.tableDetails;
                    } else {
                        $scope.tableDetailsMap = "None"
                    }
                    if ($scope.parentCatDetails.pCat == mvParCat) {
                        $scope.mvMapFlag = false;
                        if (($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tag == $scope.selectedTag) && ($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].detailType == "None")) {
                            $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].detailType = $scope.detailType;
                        }
                        if ($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tag == $scope.selectedTag) {
                            if ($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping.length == 0) {
                                $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping.push({
                                    "outField": $scope.selectMvOutItem,
                                    "mapping": {
                                    "parentBB": $scope.parentBBMap,
                                    "loopDetails": $scope.loopDetailsMap,
                                    "tableDetails": $scope.tableDetailsMap
                                    },
                                "groupDetails": $scope.groupDetailsMap
                                });
                                swal("Mv mapping saved successfully")
                                $scope.mvMapFlag = true;
                            }
                            else {
                                for (var j = 0; j < $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping.length; j++) {
                                    $scope.mvOutFlag = false;
                                    if ($scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j].outField == $scope.selectMvOutItem) {
                                        $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j]["mapping"] = {
                                            "parentBB": $scope.parentBBMap,
                                            "loopDetails":$scope.loopDetailsMap,
                                            "tableDetails": $scope.tableDetailsMap
                                        };
                                        $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping[j]["groupDetails"] = $scope.groupDetailsMap;
                                        $scope.mvMapFlag = true;
                                        $scope.mvOutFlag = true;
                                        swal("Mv mapping saved successfully")
                                        break;
                                    }
                                }
                                if (!$scope.mvMapFlag) {
                                    $scope.parentCatDetails.mapped[$scope.mvSelectedTagIdx].tagMapping.push({
                                        "outField": $scope.selectMvOutItem,
                                        "mapping": {
                                            "parentBB": $scope.parentBBMap,
                                            "loopDetails": $scope.loopDetailsMap,
                                            "tableDetails": $scope.tableDetailsMap
                                        },
                                        "groupDetails": $scope.groupDetailsMap
                                    });
                                    $scope.mvMapFlag = true;
                                    swal("Mv mapping saved successfully")
                                }
                            }
                            }
                    }
                    if (!$scope.mvOutFlag) {
                        for (var i = 0; i < $scope.mvUnmappedColList.length; i++) {
                            if ($scope.selectMvOutItem == $scope.mvUnmappedColList[i]) {
                                $scope.mvUnmappedColList.splice(i, 1);
                                break;
                            }
                        }
                        if($scope.mappedMvOutCols.indexOf($scope.selectMvOutItem) !== -1) {
                            $scope.message = 'already exists!';
                        }else{
                            $scope.mappedMvOutCols.push($scope.selectMvOutItem)
                        }

                        $scope.reset();
                        $scope.selectMvOutItem = null;
                    }
                }
            } else {
                alert("Please select parent category, detail type and tag")
            }

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
            if($scope.tableIDs){
                $scope.tableId = $scope.tableIDs[0]
            }
            return $scope.tableIDs
        }
        $scope.getTableData = function(){
            $scope.tableData = mainService.getPDFInfo().tableInfo;
            $scope.parentCategory= mainService.getPDFInfo().parentCategories;
        }

    $scope.getDetailType = function(detailType){
        if($scope.loopIDs.length==0 && ($scope.detailType=="loopDetails")){
            swal("Loop data does not exist");
             $scope.detailType="parentBB"
        }else if($scope.tableIDs.length==0 && ($scope.detailType=="tableDetails")){
            swal("Table data does not exist");
            $scope.detailType="parentBB"
        }
        else if($scope.detailType=="parentBB"){
            $scope.detailType=detailType
        }
    }

    $scope.setGrpDetails = function(i,j){
        if(($scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails !="None") && (Object.keys($scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails).length>=1)){
            $scope.groupFlag = true;
            $scope.grpTextVal = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.groupText
            if ($scope.grpTextVal == "None") {
                $scope.grpTextVal = ["None"]
            }
            for (var k = 0; k < $scope.grpTextVal.length; k++) {
                if ($scope.groupInputFields.length == $scope.grpTextVal.length) {
                    break;
                }
                $scope.groupInputFields.push({});
            }

            $scope.mvUserData.grptextline = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.textline

            $scope.minXGrpTextVal = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.min_X.text
            if ($scope.minXGrpTextVal == "None") {
                $scope.minXGrpTextVal = ["None"]
            }
            for (var k = 0; k < $scope.minXGrpTextVal.length; k++) {
                    if ($scope.grpMinXInputFields.length == $scope.minXGrpTextVal.length) {
                        break;
                    }
                    $scope.grpMinXInputFields.push({});
            }
            $scope.mvUserData.minxReference = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.min_X.ref
            $scope.mvUserData.grpMinXPoint = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.min_X.point

            $scope.grpMaxXTextVal = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.max_X.text
            if ($scope.grpMaxXTextVal == "None") {
                $scope.grpMaxXTextVal = ["None"]
            }
            for (var k = 0; k < $scope.grpMaxXTextVal.length; k++) {
                    if ($scope.grpMaxXInputFields.length == $scope.grpMaxXTextVal.length) {
                        break;
                    }
                    $scope.grpMaxXInputFields.push({});
            }

            $scope.mvUserData.maxXReference = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.max_X.ref
            $scope.mvUserData.grpMaxXPoint = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.max_X.point

            $scope.grpMinYTextVal = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.min_Y.text
            if ($scope.grpMinYTextVal == "None") {
                $scope.grpMinYTextVal = ["None"]
            }
            for (var k = 0; k < $scope.grpMinYTextVal.length; k++) {
                    if ($scope.grpMinYInputFields.length == $scope.grpMinYTextVal.length) {
                        break;
                    }
                    $scope.grpMinYInputFields.push({});
            }
            $scope.mvUserData.minYReference = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.min_Y.ref
            $scope.mvUserData.grpMinYPoint = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.min_Y.point

            $scope.grpMaxYTextVal = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.max_Y.text
            if ($scope.grpMaxYTextVal == "None") {
                $scope.grpMaxYTextVal = ["None"]
            }
            for (var k = 0; k < $scope.grpMaxYTextVal.length; k++) {
                    if ($scope.grpMaxYInputFields.length == $scope.grpMaxYTextVal.length) {
                        break;
                    }
                    $scope.grpMaxYInputFields.push({});
            }
            $scope.mvUserData.maxYReference = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.max_Y.ref
            $scope.mvUserData.grpMaxYPoint = $scope.parentCatDetails.mapped[i].tagMapping[j].groupDetails.bbox.max_Y.point
        }
    }
    $scope.getLoopInfo = function(){
         $scope.loopData = mainService.getPDFInfo().loopInfo;
         $scope.loopRefenceJson = $scope.loopData.referenceJson ;
         $scope.parentCategory= mainService.getPDFInfo().parentCategories;
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
             if($scope.loopIDs){
                $scope.loopId = $scope.loopIDs[0]
            }
            return $scope.loopIDs;
        }

        //save mv Json
        $scope.saveMultiValueJson = function() {
            mainService.pdfUserCommittedMVMappings($scope.mVData);
            $scope.responseFlag = false;
            mainService.sendAllJson(function(response) {
                if (response.data=="success") {
                    $scope.responseFlag = true;
                    $state.go('processInput');

                }
              else{
                 $scope.responseFlag = false;
                }
            })
        }


        $scope.reset = function(){
          if($scope.detailType=="tableDetails"){
            $scope.mvUserData.tableId = "";
            $scope.mvUserData.columnIndex = "";
          }
          else{
          $scope.mvUserData.mvStartText = "";
          $scope.minXTextVal = [];
          $scope.mvUserData.minXOccurence = ""
          $scope.mvUserData.minXPoint = []
          $scope.maxXTextVal=[]
          $scope.mvUserData.maxXOccurence= ""
          $scope.mvUserData.maxXPoint = []
          $scope.minYTextVal = []
          $scope.mvUserData.minYOccurence = ""
          $scope.mvUserData.minYPoint = []
          $scope.maxYTextVal = []
          $scope.mvUserData.maxYOccurence =""
          $scope.mvUserData.maxYPoint = []
          $scope.minXInputFields = [{}];
          $scope.maxXInputFields = [{}];
          $scope.minYInputFields = [{}];
          $scope.maxYInputFields = [{}];
          $scope.showAddGroup = true;
          $scope.groupFlag = false;
          $scope.GrpTextLineFlag = false;
        }
          if($scope.groupFlag){
            $scope.groupFlag = false;
          }
        }

        $scope.deleteMvMap = function(mvParCat){
         if (mvParCat && $scope.selectedTag && $scope.selectMvOutItem) {
                for (var i = 0; i < $scope.parentCatDetails.mapped.length; i++) {
                    if ($scope.selectedTag == $scope.parentCatDetails.mapped[i].tag) {
                        for (var j = 0; j < $scope.parentCatDetails.mapped[i].tagMapping.length; j++) {
                            if($scope.selectMvOutItem==$scope.parentCatDetails.mapped[i].tagMapping[j].outField){
                                debugger;
                                $scope.mvUnmappedColList.push($scope.selectMvOutItem)
                                $scope.parentCatDetails.mapped[i].tagMapping.splice(j,1)
                                $scope.mappedMvOutCols.splice(j,1)
                                $scope.selectMvOutItem = "";
                                $scope.reset()
                                break;
                        }
                        }
                    }
            }
        }
        }
    });