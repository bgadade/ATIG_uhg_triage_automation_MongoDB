angular.module("myapp")
.factory('mainService',function($http,encryptDecryptService){
  var headers = {};
  var usrCmtdMap = {};
  var mappings = {};
  var usrCmtdSvMap = null;
  var usrCmtdMvMap = null;
  var parentMapTable = null;
  var typeSelected = null;
  var showTypes = null;
  var finalOutput = null;
  var transTypedata = null;
  var nonDelData = {};
  var mappingHeaders = {};
  var pdfInfoData = null;
  var pdfInfoJson = null;
  var svRunMapData = null;
  var savePageDi = {};
  var usrCmtdPDFConfMap = {};
  var usrCmtdPDFConfSvMap =null;
  var usrCmtdPDFConfTableMap = null;
  var usrCmtdPDFConfLoopMap = null;
  var usrCmtdPDFConfMvMap =null;
  var usrCmtdPDFMap =null;
  var logDataObj = {}
  var saveUserMapIds = null;
  var saveTotalCount = null;
  var sotData = null;
  var declonedData = null;

  var svFields = null;
  function initialize(){
      headers = {};
      savePageDi = {};
      usrCmtdMap = {};
      mappings = {};
      usrCmtdPDFConfMap = {};
      usrCmtdPDFConfSvMap =null;
      usrCmtdPDFConfTableMap = null;
      usrCmtdPDFConfLoopMap = null;
      usrCmtdPDFConfMvMap =null;
      usrCmtdPDFMap =null;

      usrCmtdSvMap = null;
      usrCmtdMvMap = null;
      parentMapTable = null;
      typeSelected = null;
      showTypes = null;
      finalOutput = null;
      finalData = null;
      transTypedata = null;
      sotData = null;
      declonedData=null;
      mappingHeaders = {};
      nonDelData = {}
      nonDelPdfDataObj = [];
      pdfConfJSonData = {};
  }
  function setHeaderValues(data,sheet,callback) {
    headers['headervalues'] = data.headervalues;
    headers['delegateCode'] = data.delegateCode;
    headers['delegateStatus'] = data.delegateStatus;
    headers['filename'] = data.filename;
    headers['fileFormat'] = data.fileFormat;
  }
  function setToken(token,fNmUniq){
    headers['token'] = token
    headers['fNmUniq'] = fNmUniq
  }
  function savePageDiOut(data,callback){
    savePageDi['pageDi'] = data.pageDi
  }
  function setIntermMapJson(sheet,data){
    if (usrCmtdMap[sheet] == undefined) {
      usrCmtdMap[sheet] = {};
    }
    mappingHeaders = data;
  }
  function transformMappings(data,sheet) {
    if (mappings[sheet] == undefined) {
      mappings[sheet] = {};
    }
    mappings[sheet]["sv"] = data;
    // svUserMapping = data;
  }
  function transformMvMappings(data,sheet){
    mappings[sheet]["mv"] = data;
    // if (headers['mappingData'][sheet]['matchPer'] >= 80) {
    // }
  }
  function userCommittedSvMappings(data,sheet){
    usrCmtdSvMap = data;
    // usrCmtdMap[sheet] = {};
    usrCmtdMap[sheet]["sv"] = data;
  }

  function pdfUserCommittedSvMappings(data){
    usrCmtdPDFConfSvMap = data;
    pdfConfJSonData['singleValue']["mapped"]  = data["mapped"];
  }
   function pdfUserCommittedPdfMappings(data){
      pdfConfJSonData["pdfInfo"]["pdfInfoData"]=[data]
  }
  function pdfUserCommittedTableMappings(data){
    usrCmtdPDFConfTableMap = data;
     pdfConfJSonData['tableInfo']["tableInfo"] = data["tableInfo"];
  }
   function pdfUserCommittedLoopMappings(data){
    usrCmtdPDFConfLoopMap = data;
    pdfConfJSonData["loopInfo"]["loopInfo"] = data["loopInfo"];
  }

  function pdfUserCommittedMVMappings(data){
    usrCmtdPDFConfMvMap = data;
    pdfConfJSonData["multivalue"]["multivalue"] = data["multivalue"];
  }

  function userCommittedMvMappings(data,parentMap,types,showType,sheet){
    usrCmtdMvMap = data;
    parentMapTable = parentMap;
    typeSelected = types;
    showTypes = showType;
    usrCmtdMap[sheet]["mv"] = data;
    usrCmtdMap[sheet]["parentMapTable"] = parentMap;
    usrCmtdMap[sheet]["typeSelected"] = types;
    usrCmtdMap[sheet]["showTypes"] = showType;
    headers['mappingData'][sheet]['matchPer'] = "Customised";
  }
  function clearMappings(sheet,flag){
    delete usrCmtdMap[sheet];
    if (mappings[sheet] != undefined && flag) {
      delete mappings[sheet];
    }
    else if (mappings[sheet] != undefined && Object.keys(mappings[sheet]).length != 2 ) {
      delete mappings[sheet];
    }
  }
  return{
    initialize :initialize,
    extractEmail :function(filename,callback){
      return $http.post('/extractEmail/'+filename).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getInputFile :function(username,filename,callback){
      var usrCmtdSvMap = null;
      var usrCmtdMvMap = null;
      var obj={
        "userName":username,
        "headers" : headers
      }
      return $http.post('/getInputFile/'+filename,obj).then(function(response){
//        headers['token'] = response.data.unique_id;
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },

    saveFileType:function(fileFormat){
      headers['fileFormat'] = fileFormat;
    },

    saveNextFlag: function(nextFlag){
      headers['nextFlag'] = nextFlag;
    },
    saveSelFileName:function(filename){
         headers['selFileName'] = filename;
    },

    saveFileFlags:function(fileObj){
      headers['filesFlag'] = fileObj;
    },

    saveNonDelData :function(filename,pdfData){
        nonDelPdfDataObj[filename] = pdfData
    },

    saveUserCmtdNonDelData:function(records,sampleData){
        nonDelData['records'] = records;
        nonDelData["sampleData"] = sampleData;
        headers['backFlag'] = true;
    },

    getUsrCmtdNonDelData : function(){
        var data = {};
        data = {
          records : nonDelData["records"],
          mvMapJson : nonDelData["mv"],
          sampleData:nonDelData["sampleData"]
        }
        return data
    },
     getSavedLogData : function(){
          return logDataObj
     },
    getSavedUserMapIds : function(){
          return saveUserMapIds
     },
     getTotalCount : function(){
          return saveTotalCount
     },
    getsavedNonDelData :function(){
        return nonDelPdfDataObj
    },

    setHeaderValues :setHeaderValues,
    setToken:setToken,
    setHeaderRows :function(callback){
      return $http.post('/setHeaderRows',headers).then(function(response){
        console.log(response,"set header rows response")
        headers['mappingData'] = response.data;
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getMappingHeaders :function(){
      return mappingHeaders;
    },
    setIntermMapJson :setIntermMapJson,
    setDelegate :function(delegate,filename,fileType,delType){
      headers['delegateCode']=delegate.code;
      headers['delegateStatus']=delegate.status;
      headers['filename']=filename;
      headers['fileType'] = fileType;
      headers['delType'] = delType;
    },
    getDelegates :function(callback){
      return $http.post('/getDelegates').then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getHeaders : function () {
      return headers;
    },
    getPDFInfo : function () {
      return pdfConfJSonData;
    },

    getProviderMappings :function(callback){
      return $http.post('/getProviderMappings/',headers).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getMappingFields :function(sheetname,callback){
      if (usrCmtdMap[sheetname]["sv"] != null) {
        for (var j = 0; j < usrCmtdMap[sheetname]["sv"][0].inFields.length; j++) {
          var flag = 0;
          for (var i = 0; i < usrCmtdMap[sheetname]["mv"][0].unmappedInpCols.length; i++) {
            if (usrCmtdMap[sheetname]["mv"][0].unmappedInpCols[i] == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField) {
              if (usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped == 'nlp') {
                flag = 1;
              }
              else{
                usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped = false;
                flag = 1;
              }
            }
          }
          if (flag==0) {
            for (var i = 0; i < usrCmtdMap[sheetname]["mv"][0].suggested.length; i++) {
              if ( usrCmtdMap[sheetname]["mv"][0].suggested[i].inputField == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField) {
                for (var k = 0; k < usrCmtdMap[sheetname]["sv"][0].unmapOtFields.length; k++) {
                  if (usrCmtdMap[sheetname]["sv"][0].unmapOtFields[k].bestMatch == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField && usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped == true) {
                    usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped = "nlp";
                    usrCmtdMap[sheetname]["sv"][0].unmapOtFields[k].original_map = "nlp";
                    flag = 1;
                  }
                }
                if(flag == 0)
                {
                  if (usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped != 'nlp') {
                    usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped = false;
                    flag = 1;
                  }
                  else{
                    flag = 1;
                  }
                }
              }
            }
            if (flag==0) {
              for (var k = 0; k < usrCmtdMap[sheetname]["sv"][0].unmapOtFields.length; k++) {
                if (usrCmtdMap[sheetname]["sv"][0].unmapOtFields[k].bestMatch == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField && usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped == 'nlp') {
                  usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped = true;
                  usrCmtdMap[sheetname]["sv"][0].unmapOtFields[k].original_map = true;
                  flag = 1;
                }
              }
              if (flag == 0) {
                usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped = true;
              }
            }
          }
        }
        callback({svMap:usrCmtdMap[sheetname]["sv"][0],status:"200"});
      }
      else{
        if (mappings[sheetname] == undefined || mappings[sheetname] == 'to be ignored') {
          var obj = {
            selectedSheet :sheetname,
            headers  :headers,
            delegate : "new"
          }
        }
        else{
          var obj = {
            selectedSheet :sheetname,
            headers  :headers,
            delegate :mappings[sheetname].sv
          }
        }
        $http.post('/getSvMappingFields/',obj).then(function (response) {
          // headerValues = response.data.headerValues;
          for (var i = 0; i < response.data.unmapOtFields.length; i++) {
            if (response.data.unmapOtFields[i].bestMatch != null && response.data.unmapOtFields[i].bestMatch != undefined) {
              response.data.unmapOtFields[i].original_map = 'nlp';
            }
          }
          callback({svMap:response.data,status:"200"});
        }).catch(function(response) {
          console.log('Error:', response.status, response.data);
          callback(response);
        });
      }
    },
//    extract pdf
    userCommittedSvMappings :userCommittedSvMappings,
    pdfUserCommittedSvMappings:pdfUserCommittedSvMappings,
    pdfUserCommittedPdfMappings:pdfUserCommittedPdfMappings,
    pdfUserCommittedLoopMappings:pdfUserCommittedLoopMappings,
    pdfUserCommittedMVMappings:pdfUserCommittedMVMappings,
    pdfUserCommittedTableMappings:pdfUserCommittedTableMappings,

    transformMappings :transformMappings,
    userCommittedMvMappings :userCommittedMvMappings,
    getMvMappingFields :function(sheetname,callback){
      if (mappings[sheetname].mv == undefined || mappings[sheetname] == 'to be ignored') {
        svMappingsHeader =
         {
          sv : mappings[sheetname]["sv"],
          token : headers['token'],
          sheetname : sheetname,
          delegate : "new",
          headers:headers
        }
      }
      else{
        svMappingsHeader = {
          sv : mappings[sheetname]["sv"],
          token : headers['token'],
          sheetname : sheetname,
          delegate : mappings[sheetname].mv,
          headers: headers
        }
      }

      if (usrCmtdMap[sheetname]["mv"] != null) {
        for (var j = 0; j < usrCmtdMap[sheetname]["sv"][0].inFields.length; j++) {
          var flag = 0;
          for (var i = 0; i < usrCmtdMap[sheetname]["mv"][0].unmappedInpCols.length; i++) {
            if (usrCmtdMap[sheetname]["mv"][0].unmappedInpCols[i] == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField) {
              if (usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped == true) {
                usrCmtdMap[sheetname]["mv"][0].unmappedInpCols.splice(i,1);
              }
              flag = 1;
            }
          }
          if (flag == 0 && usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped == false) {
            for (var k = 0; k < usrCmtdMap[sheetname]["mv"][0].suggested.length; k++) {
              if ( usrCmtdMap[sheetname]["mv"][0].suggested[k].inputField == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField) {
                flag = 1;
              }
            }
            if (flag == 0) {
              usrCmtdMap[sheetname]["mv"][0].unmappedInpCols.push(usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField);
            }
          }
          if (flag == 0 && usrCmtdMap[sheetname]["sv"][0].inFields[j].mapped == true) {
            for (var k = 0; k < usrCmtdMap[sheetname]["mv"][0].suggested.length; k++) {
              if ( usrCmtdMap[sheetname]["mv"][0].suggested[k].inputField == usrCmtdMap[sheetname]["sv"][0].inFields[j].inputField) {
                usrCmtdMap[sheetname]["mv"][0].suggested.splice(k,1);
              }
            }
          }
        }
        var data = {};
        data = {
          multiValueMap : usrCmtdMap[sheetname]["mv"],
          sampleData : sampleMvData,
          parentMapTable :usrCmtdMap[sheetname]["parentMapTable"],
          typeSelected :usrCmtdMap[sheetname]["typeSelected"],
          showType :usrCmtdMap[sheetname]["showTypes"]
        }
        // var response= {};
        // response.data=data;
        // callback(response);
        callback({data:data,status:"200"});
      }
      else{
        $http.post('/getMvMappingFields/',svMappingsHeader).then(function (response) {
          sampleMvData = response.data.sampleData;
          callback({data:response.data,status:"200"});

          // callback(response);
        }).catch(function(response) {
          console.log('Error:', response.status, response.data);
          callback(response);
        });
      }
    },
    transformMvMappings : transformMvMappings,
    clearMappings : clearMappings,
    setMvMappingFields :function(username,transformMap,callback){
      var usrCmtdSvMap = null;
      var usrCmtdMvMap = null;
      var userMappings = {
        mappings: transformMap,
        username: username,
        headers: headers
      }
      return $http.post('/setMvMappingFields/',userMappings).then(function(response){
        transTypedata = response;
        // finalOutput = response;
        callback("success");
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getTransactionData :function(callback){
      if(transTypedata!=null)
        callback(transTypedata);
    },
    getTransformMapping :function(){
      return mappings;
    },
    cloneSelectedRows :function(finalCloningData,callback){
      return $http.post('/multiTransDataClone/',finalCloningData).then(function(response){
        sotData = response;
        console.log(response);
        callback("success");
      }).catch(function(response) {
        console.log('Error:',response, response.status, response.data);
        callback(response);
      });
    },
    decloneData :function(callback){
      if(sotData!=null)
        callback(sotData);
    },
    fetchSotData :function(callback){
      if(sotData!=null)
        callback(sotData);
    },
    generateOutput :function(transData,previousScrData,callback){
      console.log(transData,previousScrData);
      var finalMappings = {
        validateActions : transData,
        previousScrData : previousScrData
      }
      return $http.post('/generateOutput/',finalMappings).then(function(response){
        finalOutput = response;
        callback("success");
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getOutputData :function(callback){
      if(finalOutput!=null)
        callback(finalOutput);
    },
    setException :function(exceptionData,username,callback){
      var specExceptions = {
        data : exceptionData,
        username: username
      }
      return $http.post('/setExceptionData/',specExceptions).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getProviderOutput :function(username,providerName,callback){
      var providerMappings = {
        username: username,
        providerName: providerName,
        headers: headers
      }
      return $http.post('/providerOutput/',providerMappings).then(function(response){
        if (response.data == "false" || response.data == null) {
          callback(response.data)
        }
        else{
          // finalOutput = response;
          transTypedata = response;
          callback("success");
        }
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    viewMapping :function(sheetMap,callback){
      return $http.post('/viewMapping/',sheetMap).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setOutputData :function(outputData,callback){
      var output = {
        outputData: outputData,
        token: headers['token'],
        dwndFlag:false
      };
      return $http.post('/setOutputData/',output).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
     downloadAllOutputData :function(outputData,callback){
      var output = {
        outputData: outputData,
        token: headers['token'],
        dwndFlag:true
      };
      return $http.post('/setOutputData/',output).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getAdminMap :function(callback){
      return $http.post('/adminMap/').then(function(response){
          saveUserMapIds = response.data.userMapIds
          saveTotalCount = response.data.totalCount
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setAdminMap :function(adminMap,callback){
      data = {
      "adminMap":adminMap,
      "userMapIds":saveUserMapIds
      }
      return $http.post('/setAdminMap/',data).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getExceptions :function(callback){
      return $http.post('/exceptions/').then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setAdminExceptions :function(exceptionData,callback){
      return $http.post('/setAdminExceptionData/',exceptionData).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getSvAdminMap :function(callback){
      return $http.post('/svAdminMap/').then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setSvAdminMap :function(svAdminMap,callback){
      return $http.post('/setSvAdminMap/',svAdminMap).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getMvAdminMap :function(callback){
      return $http.post('/mvAdminMap/').then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setMvAdminMap :function(mvAdminMap,callback){
      return $http.post('/setMvAdminMap/',mvAdminMap).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getAdminProvMap :function(callback){
      return $http.post('/getAdminProvMap/').then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setProvUserMap :function(userProv,callback){
      return $http.post('/setProvUserMap/',userProv).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getJSON :function(filename,callback){
      var obj = {
        filename: filename
      }
      encryptData = encryptDecryptService.encrypt(JSON.stringify(obj),encryptDecryptService.key);
      return $http.post('/getJSON/',encryptData).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    addNewUser :function(userList,callback){
      return $http.post('/addNewUser/',userList).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setDataList :function(dataList,filename,callback){
      var obj = {
        data : dataList,
        filename : filename,
      }
      encryptData = encryptDecryptService.encrypt(JSON.stringify(obj),encryptDecryptService.key);
      return $http.post('/setJSON/',encryptData).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setProviderList :function(dataList,callback){
      return $http.post('/setProvider/',dataList).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getCsvToJson :function(filename,callback){
      var obj = {
        filename: filename
      }
      return $http.post('/getCsvToJson/',obj).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getJsonToCsv :function(degreeData,filename,cols,callback){
      var obj = {
        data : degreeData,
        filename : filename,
        cols : cols
      }
      return $http.post('/getJsonToCsv/',obj).then(function(response){
        callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
      extractPDF :function(username,callback){
      var obj = {
        filename: headers['filename'],
        delegateCode :headers['delegateCode'],
        delegateStatus : headers['delegateStatus'],
        username:username
      }
      return $http.post('/extractPDF/',obj).then(function(response){
          headers['token'] = response.data.unique_id;
           savePageDi['pageDi'] = response.data.pageDi;
          pdfConfJSonData['singleValue'] = response.data.uiAllJsons.singleValue;
          pdfConfJSonData['pdfInfo'] = response.data.uiAllJsons.pdfInfo
          pdfConfJSonData['tableInfo'] = response.data.uiAllJsons.tableInfo;
          pdfConfJSonData['loopInfo'] = response.data.uiAllJsons.loopInfo;
          pdfConfJSonData['parentCategories'] = response.data.uiAllJsons.parentCategory;
          pdfConfJSonData['multivalue'] = response.data.uiAllJsons.multivalue;
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
     openPDFFile :function(callback){
      fileName = headers['filename'];
//      filename= JSON.stringify(filename);
      return $http.get('/openPDFFile/'+fileName, {responseType: 'arraybuffer'}).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },

     sendPDFInfo :function(pdfInfo,callback){
       pdfInfoJson = pdfInfo
       var obj = {
        filename: headers['filename'],
        pdfInfo :pdfInfo,
        pageDi: JSON.stringify(savePageDi['pageDi'])
      }
      return $http.post('/sendPDFInfo/',obj).then(function(response){
//          pdfInfoData = response.data;
//          recordCount = Object.keys($scope.outData["recordDi"]).length;
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
     svSampleRun :function(sampleSvRunMap,callback){
     svRunMapData = sampleSvRunMap;
     var obj = {
        "pdfInfoData":JSON.stringify(pdfInfoData),
        "singleValue" : sampleSvRunMap
     }
      return $http.post('/svSampleRun/',obj).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getSVFields :function(callback){
      return $http.get('/getSVFields/').then(function(response){
          svFields = response.data;
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    setMargin :function(editedData,callback){
     var obj = {
           "pdfInfoJson" : pdfInfoJson,
           "pdfInfoData": JSON.stringify(pdfInfoData),
           "singleValue" : svRunMapData,
           "modifiedData": editedData
        }

      return $http.post('/setMargin/',obj).then(function(response){
          pdfInfoJson = response.data;
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
     sendAllJson :function(callback){
     var obj = {
     "pdfMapping": pdfConfJSonData,
     "headers":headers
     }

      return $http.post('/sendAllJson/',obj).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },

    getPdfData :function(callback){
      filename = headers['filename'][0];
      return $http.get('/getPdfData/'+filename,{responseType: 'arraybuffer'}).then(function(response){
            headers['pdfData'] = []
          headers['pdfData'].push({pdfFNm:response.statusText,pdfData:response.data})
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getSelectedPdfFile :function(filename,callback){
      return $http.get('/getPdfData/'+filename,{responseType: 'arraybuffer'}).then(function(response){
          headers['pdfData'].push({pdfFNm:response.statusText,pdfData:response.data})
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getHtmlData :function(username,callback){
    filename = headers['filename'];
    return $http.post('/getHtmlData/'+filename,username).then(function(response){
          headers['token'] = response.data.unique_id;
          headers['htmlData'] =  response.data.htmlData
//          headers['pdfData'] =  response.data.pdfData
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    getNonDelData :function(callback){
      return $http.get('/getNonDelData/').then(function(response){
          nonDelData["mv"] = response.data.records['mv']
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
     getLogData :function(callback){
      return $http.get('/getLogData/').then(function(response){
          logDataObj["logData"] = JSON.parse(response.data['logData'])
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
     getNextMappings :function(callback){
      return $http.get('/getNextMappings/'+saveUserMapIds).then(function(response){
          callback(response);
      }).catch(function(response) {
        console.log('Error:', response.status, response.data);
        callback(response);
      });
    },
    // getDashboardData :function(start_date,end_date,callback){
    //   let inputData = {
    //     "startdate":start_date,
    //     "enddate":end_date
    //   }
    //   console.log(inputData,"inputdata");
    //   return $http.post('/retrieveFilesProv/',inputData).then(function(response){
    //     console.log(response, "dashboard output");
    //       callback(response);
    //   }).catch(function(response) {
    //     console.log('Error:', response.status, response.data);
    //     callback(response);
    //   });
    // },
    // getlastBusinessdayData :function(callback){     
    //   const url = '/retrieveFilesProv/';
    //   return $http.get(url).then(function(response){
    //     console.log(response, "dashboard output");
    //       // logDataObj["logData"] = JSON.parse(response.data['logData'])
    //       callback(response);
    //   }).catch(function(response) {
    //     console.log('Error:', response.status, response.data);
    //     callback(response);
    //   });
    // }

  }
});