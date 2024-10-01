angular.module("myapp")

  .controller("DashboardController", function ($scope, mainService, $state) {
    $scope.resdashboardData;
    $scope.username = null;
    $scope.start_date = null;
    $scope.end_date = null;
    $scope.sdate = null;
    $scope.edate = null;
    $scope.getData = function () {
      mainService.getlastBusinessdayData( function (response) {
        console.log(response,"response");
        $scope.resdashboardData = response.data.output;
        let start_date = response.data.startdate;
        let end_date = response.data.enddate;
        formatst_date = start_date.split('-');
        start_date =  formatst_date[1] + '-' + formatst_date[0] + '-' + formatst_date[2];
        formatend_date = end_date.split('-');
        end_date =  formatend_date[1] + '-' + formatend_date[0] + '-' + formatend_date[2]
        $scope.sdate = start_date;
        $scope.edate = end_date;
        $scope.start_date = new Date(start_date);
        $scope.end_date = new Date(end_date);
        console.log($scope.start_date,$scope.end_date,"dates");

      })
    } 
    $scope.dashboardData = function (start_date, end_date) {
      console.log(start_date, end_date, typeof (start_date), "dates selected");
      if (start_date) {
        start_date = start_date.toLocaleDateString()
        start_date = start_date.replace(/\//g, '-')
        format_date = start_date.split('-');
        if (format_date[0].length < 2) format_date[0] = '0' + format_date[0];
        if (format_date[1].length < 2) format_date[1] = '0' + format_date[1];
        start_date =  format_date[0] + '-' + format_date[1] + '-' + format_date[2]
        $scope.sdate = start_date;
      }
      if (end_date) {
        end_date = end_date.toLocaleDateString()
        end_date = end_date.replace(/\//g, '-')
        format_date = end_date.split('-');
        if (format_date[0].length < 2) format_date[0] = '0' + format_date[0];
        if (format_date[1].length < 2) format_date[1] = '0' + format_date[1];
        end_date = format_date[0] + '-' + format_date[1] + '-' + format_date[2]
        $scope.edate = end_date;
      }
      mainService.getDashboardData(start_date, end_date, function (response) {
        console.log(response, "response");
        $scope.resdashboardData = response.data.output;
        console.log($scope.resdashboardData, "$scope.dashboardData");
      })
    }
    $scope.toggleRow = function (rowData) {
      rowData.showDetail = !rowData.showDetail;
    }
    $scope.exportData = function (){
      console.log($scope.sdate, $scope.edate);
      let datatoExport = [];
      for(let i=0;i < $scope.resdashboardData.length; i++){
        let obj = {};
        for (let [key, value] of Object.entries($scope.resdashboardData[i])) {
          if(key != 'Token' && key != '$$hashKey' && key != 'FileNpi' && key != 'showDetail'){
            obj[key] = value; 
          }
        }
        datatoExport.push(obj);
      }
      let reportname = "Report_"+$scope.sdate+"_"+$scope.edate+".xlsx";
      // arrayToExport
      alasql("SELECT * INTO XLSX( ? ,{headers:true}) FROM ? ", [reportname, datatoExport]);
      // alasql('SELECT * INTO XLSX('report.xlsx',{headers:true}) FROM ?', [datatoExport]);
    }
  });
