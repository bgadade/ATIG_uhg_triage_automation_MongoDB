var app = angular.module("myapp", ['ui.router', 'ngFileUpload', 'ui.bootstrap', 'xeditable', 'ui.select', 'anguFixedHeaderTable', 'angular.filter', 'ngIdle', 'ngMessages']);


app.config(function ($stateProvider, $urlRouterProvider, KeepaliveProvider, IdleProvider) {
  IdleProvider.idle(300);
  IdleProvider.timeout(10);
  KeepaliveProvider.interval(10);

  $urlRouterProvider.otherwise('/login');

  $stateProvider.state('login', {
    url: '/login',
    views: {
      "content": {
        templateUrl: "static/template/login_alt.html",
        controller: "LoginController"
      }
    }
  })
    .state('adminMap', {
      url: '/adminMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/adminmap.html",
          controller: "AdminMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('home', {
      url: '/home',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/input.html",
          controller: "InputController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('email', {
      url: '/extractedEmail/:emailFile/:attachmentFile',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/extractEmail.html",
          controller: "ExtractEmailController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('processInput', {
      url: '/processInput/:attachmentFile/:delegate',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/processInput.html",
          controller: "ProcessInputController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mapping', {
      url: '/mapping/:map',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/mappingControl.html",
          controller: "MappingHeaderController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('svMap', {
      url: '/svMapping/:sheet',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/mapping.html",
          controller: "MappingController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mvMap', {
      url: '/mvMapping/:sheet',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/altermultivalueMapping.html",
          controller: "MultivalueControllerAlter"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('transType', {
      url: '/transactionType',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/transactionType.html",
          controller: "TransTypeController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    // .state('dashboard', {
    //   url: '/dashboard',
    //   views: {
    //     "header": {
    //       templateUrl: "static/template/mainHeader.html",
    //       controller: "HeaderController"
    //     },
    //     "content": {
    //       templateUrl: "static/template/dashboard.html",
    //       controller: "DashboardController"
    //     },
    //     "footer": {
    //       templateUrl: "static/template/mainFooter.html"
    //     }
    //   }
    // })
    .state('review', {
      url: '/review',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/review.html",
          controller: "ReviewController"
          // templateUrl: "static/template/output.html",
          // controller: "OutputController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('exception', {
      url: '/reportExceptions',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/reportExceptions.html",
          controller: "ExceptionController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('providerMap', {
      url: '/providerMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/providerMapping.html",
          controller: "AdminProviderController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('svAdminMap', {
      url: '/svAdminMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/adminSvMap.html",
          controller: "SvAdminMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mvAdminMap', {
      url: '/mvAdminMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/adminMvMap.html",
          controller: "MvAdminMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('adminInput', {
      url: '/uploadFile',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/adminInput.html",
          controller: "AdminInputController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('adminException', {
      url: '/viewException',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/adminException.html",
          controller: "AdminExceptionController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('degException', {
      url: '/DegreeException',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/degException.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mstrDeg', {
      url: '/MasterDegree',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/masterDegree.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mdLvlSpc', {
      url: '/midLevelSpeciality',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/midLvlSpc.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('langCode', {
      url: '/languageCode',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/langCode.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('usStates', {
      url: '/usStates',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/usStates.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('hspAffStatus', {
      url: '/hspAffilationStatus',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/hspAffStatus.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('pcpVsSpec', {
      url: '/PcpVsSpeciality',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/pcpVsSpeciality.html",
          controller: "RefDataController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mngUsr', {
      url: '/manageUser',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/manageUsrs.html",
          controller: "MngUsrController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('newUser', {
      url: '/addNewUser',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/addNewUser.html",
          controller: "MngUsrController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('mngProvider', {
      url: '/manageProvider',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/manageProviders.html",
          controller: "ProviderController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('dummy', {
      url: '/executeAction',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/dummyTemplate.html"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('error', {
      url: '/error',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/error.html"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('nonDelMapData', {
      url: '/nonDelMapData',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/nonDelMapData.html",
          controller: "nonDelMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('nonDelMap', {
      url: '/nonDelMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/nonDelMap.html",
          controller: "nonDelMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('pdfInfoJson', {
      url: '/pdfInfoJson',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/pdfInfoJson.html",
          controller: "pdfMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('pdfMap', {
      url: '/pdfMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/pdfMapping.html",
          controller: "pdfMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('tableInfo', {
      url: '/tableInfo',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/tableInfo.html",
          controller: "pdfMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })

    .state('pdfSVMap', {
      url: '/pdfSVMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/confSVMap.html",
          controller: "pdfMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('pdfMvMap', {
      url: '/pdfMvMap',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/pdfMvMap.html",
          controller: "pdfMVMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('loopInfo', {
      url: '/loopInfo',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/loopInfo.html",
          controller: "pdfMapController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
    .state('logReport', {
      url: '/logReport',
      views: {
        "header": {
          templateUrl: "static/template/mainHeader.html",
          controller: "HeaderController"
        },
        "content": {
          templateUrl: "static/template/logReport.html",
          controller: "LogReportController"
        },
        "footer": {
          templateUrl: "static/template/mainFooter.html"
        }
      }
    })
});
app.run(function (editableOptions, $rootScope, $location, Idle, authService) {
  editableOptions.theme = 'bs3';
  $rootScope.$on("$stateChangeStart", function (event, toState, toParams, fromState, fromParams) {
    var role = sessionStorage.getItem("role")
    // var userAccess = ["/home", "/reportExceptions", "/dashboard", "/processInput/:attachmentFile/:delegate", "/mapping/:map", "/svMapping/:sheet", "/mvMapping/:sheet", "/transactionType", "/review", "/error", "/nonDelMap", "/nonDelMapData", "/loopInfo", "/pdfMvMap", "/pdfSVMap", "/tableInfo", "/pdfMap", "/pdfInfoJson"];
    var userAccess = ["/home", "/reportExceptions", "/processInput/:attachmentFile/:delegate", "/mapping/:map", "/svMapping/:sheet", "/mvMapping/:sheet", "/transactionType", "/review", "/error", "/nonDelMap", "/nonDelMapData", "/loopInfo", "/pdfMvMap", "/pdfSVMap", "/tableInfo", "/pdfMap", "/pdfInfoJson"];
    var adminAccess = ["/adminMap", "/providerMap", "/svAdminMap", "/mvAdminMap", "/viewException", "/manageUser", "/manageProvider", "/DegreeException", "/MasterDegree", "/midLevelSpeciality", "/languageCode", "/usStates", "/addNewUser", "/error", "/logReport"];
    if (toState.url == "/login") {
    }
    else if (adminAccess.includes(toState.url) && role == "admin") {
      Idle.watch();
    } else if (userAccess.includes(toState.url) && role == "user") {
      Idle.unwatch();
    }
    else {
      authService.signout();
    }
  });
});