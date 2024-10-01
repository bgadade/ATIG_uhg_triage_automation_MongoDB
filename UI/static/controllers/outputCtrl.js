angular.module("myapp")

.controller("OutputController", function($scope,mainService,$uibModal,$state) {

  // CollapsibleLists.applyTo(document.getElementById('newList'));
  $scope.activeUserMap = null;
  $scope.navbarCollapsed = true;
  $scope.selectedColName = null;
  $scope.token = mainService.getHeaders().token;
  $scope.getOutputData = function(){
    // $scope.altJson = {
    // 	"Psi": [{
    // 		"NPI": "1255683850",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Carmen", "D", "Anderson", "NP", "Primary", "", "Bellin Memorial Hospital", "", "1255683850", "", "390884478", "NURSE PRACTITIONER", "SPEC", "54313", "Input: Hospitalist/Output: Hospitalist", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "390884478",
    // 		"TRANSACTION TYPE": [""]
    // 	}, {
    // 		"NPI": "1295814176",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Robert", "A", "Cavanaugh", "MD", "Primary", "", "Bellin Memorial Hospital", "", "1295814176", "", "390884478", "OBSTETRICS AND GYNECOLOGY", "Clarify: Mandatory field Missing Value|Input: ", "54305", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "390884478",
    // 		"TRANSACTION TYPE": [""]
    // 	}, {
    // 		"NPI": "1518020429",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Kirk", "D", "Dimitris", "MD", "Primary", "", "Bellin Memorial Hospital", "", "1518020429", "", "390884478", "ORTHOPEDIC SURGERY", "SPEC", "54313", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "390884478",
    // 		"TRANSACTION TYPE": [""]
    // 	}, {
    // 		"NPI": "1629277652",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Costica", "", "Aloman", "MD", "Primary", "", "University Transplant Program", "", "1629277652", "", "911897100", "Clarify: could not match speciality|Input: American Board of Internal Medicine", "SPEC", "60612", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "911897100",
    // 		"TRANSACTION TYPE": [""]
    // 	}, {
    // 		"NPI": "1154350460",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Rosemarie", "E", "Andres", "MD", "Primary", "", "University Anesthesiologists, S.C.", "", "1154350460", "", "363117700", "Clarify: could not match speciality|Input: American Board of Anesthesiology", "SPEC", "60612", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "363117700",
    // 		"TRANSACTION TYPE": [""]
    // 	}, {
    // 		"NPI": "1407048440",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Sapanjot", "K", "Bajwa", "DO", "Primary", "", "Riverside Health System", "", "1407048440", "", "363167726", "Clarify: could not match speciality|Input: American Board of Obstetrics & Gynecology", "SPEC", "60914", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "363167726",
    // 		"TRANSACTION TYPE": [""]
    // 	}, {
    // 		"NPI": "1841221710",
    // 		"PSI ROW": {
    // 			"colSet": ["Practitioner Type", "Practitioner First Name", "Practitioner Middle Name", "Practitioner Last Name", "Degree", "Primary Indicator", "Suffix", "Group Name", "MPIN", "NPI", "Provider/Group E-mail", "TINNums", "Specialty", "Is the provider a PCP?", "Zip Code", "Supervising Specialty", "TRC IND", "Par Status E & I", "Par Status C & S ", "Par Status M & R", "Par Status M &  V West", "Par Status M &  V East", "TPSM Code", "TPSM Description", "TPSM Effective Date", "TPSM Inactivate Date", "NUCC Taxonomy Code", "Supervising Physician FirstName", "Supervising Physician LastName", "Supervising Physician Middle Initial", "Supervising Physician Suffix", "SupervisingPhysicianMPIN", "TransactionType"],
    // 			"dataSet": ["P", "Dale", "F", "Andres", "", "", "", "Clarify: Group name is not given kindly check|Input: ", "", "1841221710", "", "421463823", "UNKNOWN SPECIAL PHYSICIAN", "SPEC", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 		},
    // 		"TAX ID": "421463823",
    // 		"TRANSACTION TYPE": [""]
    // 	}],
    // 	"Sot": {
    // 		"ADDRESS DETAILS SCREEN(A)": {
    // 			"colSet": ["ADDRESS STATUS", "ADDRESS TYPE", "Address Line 1", "Address Line 2", "Address  City", "Address State", "Address Zip(00000)", "P/S", "Correspondence", "Address Phone Number-1", "Extn", "P/S", "Phone Type", "Address Phone Number-2", "Extn", "P/S", "Phone Type", "Address Phone Number-3", "Extn", "P/S", "Phone Type", "Address Phone Number-4", "Extn", "P/S", "Phone Type", "Address TypeP = PracticeC = Billing and PracticeM = Mail OnlyD = Credentialing Only", "HANDICAP ACCESSIBILITY", "MAID"],
    // 			"dataSet": [
    // 				["", "Reject: no combo, plsv and bill address are mandatory.|Input: ", "Clarify: Couldn't parse Address City, Check Manually|Input: addressLine1 -->", "", "Clarify: Mandatory field Missing Value|Input: ", "Clarify: Mandatory field Missing Value|Input: ", "Clarify: Mandatory field Missing Value|Input: ", "S", "", "Clarify: Mandatory field Missing Value|Input: ", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "Reject: no combo, plsv and bill address are mandatory.|Input: ", "", ""],
    // 				["", "", "Clarify: Mandatory field Missing Value|Input: ", "", "Clarify: Mandatory field Missing Value|Input: ", "Clarify: Mandatory field Missing Value|Input: ", "Clarify: Mandatory field Missing Value|Input: ", "", "", "Clarify: Mandatory field Missing Value|Input: ", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 4
    // 		},
    // 		"AFFILIATIONS-CATEGORY: HOSPITAL (H)": {
    // 			"colSet": ["Hospital Affiliation or Covering Group/Provider name", "Hospital MPIN", "P/S", "Hospital Privilege / Affiliation Status: Default \"AC\" if not given.Roll over the header and see the Affiliation status", "DIR: Default Value \"Y\" unless specificied (Note for Hospitalist value is \"N\")"],
    // 			"dataSet": [
    // 				["", "", "", "", ""],
    // 				["", "", "", "", ""]
    // 			],
    // 			"order": 10
    // 		},
    // 		"C & S (I-7-Z and I-4-Z Screen)": {
    // 			"colSet": ["Instance", "IPA", "LOB", "Pricing Package(Aggreement ID)", "Load", "Network Affliation", "CAP Code", "Enrollment Limit"],
    // 			"dataSet": [
    // 				["", "", "", "", "", "", "", ""],
    // 				["", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 16
    // 		},
    // 		"CREDENTIAL CATEGORY :DEA/CDS/LICENSE(F)": {
    // 			"colSet": ["License Number", "State in which License is Held", "State License Number Effective Date-Default Date: 01/01/0001", "State License Number Expiration Date", "DEA Number", "DEA Number Expiration Date: Default Value 12/31/9999 unless specified", "State in which CDS is Held", "CDS/CSR Number", "CDS Expiration Date: Default Value 12/31/9999 unless specified"],
    // 			"dataSet": [
    // 				["", "Clarify: Mandatory field Missing Value|Input: ", "", "", "", "", "", "", ""],
    // 				["", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 7
    // 		},
    // 		"CREDENTIALS-CATEGORY:DELEGATED ENTITIES NAME (T)": {
    // 			"colSet": ["Delegate Code", "Original Del Date:Initial/Credential/Appointment", "CURRENT DEL DT: Recred or Reappoint or Last Cred Dt"],
    // 			"dataSet": [
    // 				["", "10/31/2017", "10/31/2017"],
    // 				["", "", ""]
    // 			],
    // 			"order": 13
    // 		},
    // 		"Contract Detail Information": {
    // 			"colSet": ["Accepting patient value", "ENW Indicator", "STATUS", "OFFICE HOURS", "NPI  STATUS", "IFC", "ITC", "IDC", "OFFICE EMAIL", "OFFICE LANGUAGE", "DEA COMMENT", "CDS COMMENT", "PCP_REASSIGN1", "PCP_REASSIGN2", "RSN_CODE", "RSN_DESCRIPTION", "Action"],
    // 			"dataSet": [
    // 				["O", "", "", "", "", "", "", "", "", "", "", "", "", "", "45", "", ""],
    // 				["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 17
    // 		},
    // 		"FOR TRICARE ONLY: PROVIDER EDUCATION INFORMATION \"IDZ\"": {
    // 			"colSet": ["Completion Date", "School Name", "School Code", "Completion Date", "School Name", "School Code"],
    // 			"dataSet": [
    // 				["", "", "", "", "", ""],
    // 				["", "", "", "", "", ""]
    // 			],
    // 			"order": 12
    // 		},
    // 		"MPIN": {
    // 			"colSet": ["NEW MPIN"],
    // 			"dataSet": [
    // 				[""],
    // 				[""]
    // 			],
    // 			"order": 5
    // 		},
    // 		"NPI INFORMATION \"I5Z\"": {
    // 			"colSet": ["TAXONOMY", "Enumeration Date"],
    // 			"dataSet": [
    // 				["Clarify: Mandatory field Missing Value|Input: ", "Clarify: Empty enumeration date.|Input: "],
    // 				["", ""]
    // 			],
    // 			"order": 11
    // 		},
    // 		"OFFICE LIMITATION/COMMUNICATION (L)": {
    // 			"colSet": ["AGE LIMIT(FORMAT SHOULD BE \"00-000\")", "EXT HR IND", "MEDICAID", "LOC", "Office limitation Gender", "MON(START TIME)", "MON(END TIME)", "TUE(START TIME)", "TUE(END TIME)", "WED(START TIME)", "WED(END TIME)", "THU(START TIME)", "THU(END TIME)", "FRI(START TIME)", "FRI(END TIME)", "SAT(START TIME)", "SAT(END TIME)", "SUN(START TIME)", "SUN(END TIME)", "Cred Contact type1", "Cred Name", "Cred Comm Type1", "Cred Phone1", "Cred EXT1", "Cred Contact type2", "Cred Name2", "Cred Comm Type2", "Cred Phone2", "Cred EXT2", "Cred Contact type3", "Cred Name3", "Cred Comm Type3", "Cred Phone3", "Cred EXT3", "Languages Spoken(FIRST LANGAUGE)", "Language Spoken By P = Provider S = Staff B = Both", "Written By P = Provider S = Staff B = Both", "Languages Spoken(SECOND LANGAUGE)", "Language Spoken By P = Provider S = Staff B = Both", "Written By P = Provider S = Staff B = Both", "Languages Spoken at this Location(THIRD LANGUAGE)", "Language Spoken By P = Provider S = Staff B = Both", "Written By P = Provider S = Staff B = Both", "Languages Spoken at this Location(FOURTH LANGUAGE)", "Language Spoken By P = Provider S = Staff B = Both", "Written By P = Provider S = Staff B = Both", "Languages Spoken at this Location(FIFTH LANUAGE)", "Language Spoken By P = Provider S = Staff B = Both", "Written By P = Provider S = Staff B = Both", "Languages Spoken at this Location(SIXTH LANUAGE)", "Language Spoken By P = Provider S = Staff B = Both", "Written By P = Provider S = Staff B = Both", "Electronic Communication-1", "Type: E/W", "Prov/Addr:P/A", "Electronic Communication-2", "Type: E/W", "Prov/Addr"],
    // 			"dataSet": [
    // 				["", "", "Clarify: State not matched|Input: plsv State: ->", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
    // 				["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 6
    // 		},
    // 		"PROVIDER DIRECTORY MAINTENANCE SCREEN(@)- I@Z": {
    // 			"colSet": ["BUS SEG", "PRD CD", "DIV CD", "DIR IND", "SPL CD", "ADR TYP(MAID)"],
    // 			"dataSet": [
    // 				["", "", "", "", "Y", ""],
    // 				["", "", "", "", "", ""]
    // 			],
    // 			"order": 14
    // 		},
    // 		"Physician-Main Screen(M)": {
    // 			"colSet": ["Effective Date", "Last Name", "First Name", "Middle Name", "Degree/Credentials/Title", "Name Suffix", "Primary Ind", "Alt L Name (If Given)", "Alt F Name", "Alt M Name", "Gender", "DOB", "SSN", "Medicare"],
    // 			"dataSet": [
    // 				["10/31/2017", "Andres", "Dale", "F", "", "", "", "", "", "", "Clarify: whether the gender is correct or not|Input: ", "", "", ""],
    // 				["", "", "", "", "", "", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 1
    // 		},
    // 		"Provider Type": {
    // 			"colSet": ["Is this provider a Hospital-based provider operating SOLELY in a Hospital?(Y/N)(Required for TRICARE)", "Is this Provider a PCP, Specialist, Hospitalist or Hospital Based Provider(PCP, Spec, Hosp or HBP.)"],
    // 			"dataSet": [
    // 				["", "SPEC"],
    // 				["", ""]
    // 			],
    // 			"order": 8
    // 		},
    // 		"SPECIFIC SPECIALTY DETAILS": {
    // 			"colSet": ["Supervising Physician Name", "Mid-Level Supervising Specialty Name", "Mid-level Supervising Specialty (provide the specialty, not provider name) (Required For all Mid-Level Provider Only)", "Supervising specailty M/s \" Name of Speciality or Physician Name\".OPID.Date."],
    // 			"dataSet": [
    // 				["", "", "", ""],
    // 				["", "", "", ""]
    // 			],
    // 			"order": 9
    // 		},
    // 		"Search Field": {
    // 			"colSet": ["TAX ID", "NPI", "C&S Check DLI-BSAR)"],
    // 			"dataSet": [
    // 				["421463823", "1841221710", ""],
    // 				["", "", ""]
    // 			],
    // 			"order": 0
    // 		},
    // 		"Specialty Screen(S)": {
    // 			"colSet": ["Specialty Name ", "Primary Specialty/Specialty Code", "Primary/Secondary Indicator", "Prac-Default Value Y", "Ver Source(Board Name if Provided or Default Value\"PRV\"", "Board Cerified(Mid-level value is x)", "Cert Date", "Expiration Date", "TRC Indicator-Refer Tricare Grid"],
    // 			"dataSet": [
    // 				["", "Reject: spec and board-spec are empty|Input: ", "Primary", "", "PRV", "X", "", "", ""],
    // 				["", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 2
    // 		},
    // 		"TAX ID Information(T)": {
    // 			"colSet": ["Type", "TIER Indicator", "Bulk Recovery", "Name of Legal Owner of Tax id Number", "PTI Name or Group Name / PTI MPIN", "PTI MPIN", "TRICARE ONLY  Does this Provider accept VA? Default Value is Y(Y or N)", "TRICARE ONLY Does this Provider accept CHAMPVA? Default Value is Y(Y or N?)", "PROVIDER STATUS (PROVIDER STATUS SHOULD BE NEW LOAD OR PROVIDER NOT LOADED IN TIN OR PROVIDER LOADED IN TIN)"],
    // 			"dataSet": [
    // 				["T", "", "", "Clarify: Mandatory field Missing Value|Input: ", "Clarify: Group name is not given kindly check|Input: ", "", "", "", ""],
    // 				["", "", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 3
    // 		},
    // 		"UHC ID Screen": {
    // 			"colSet": ["COS Prov Num", "CLN Name(PCP Ony)", "PRA", "Cred Desig", "Mail", "All Payor", "UR Indicator", "Rx Priv"],
    // 			"dataSet": [
    // 				["0000", "", "MI", "Clarify: Mandatory field Missing Value|Input: ", "", "", "", ""],
    // 				["", "", "", "", "", "", "", ""]
    // 			],
    // 			"order": 15
    // 		}
    // 	}
    // };
    // $scope.psiJson = $scope.altJson.Psi;
    // $scope.sotJson = $scope.altJson.Sot;
    // $scope.selectedSheet = Object.keys($scope.altJson)[0];
    // $scope.activePsiMap = $scope.psiJson[0];
    // $scope.selectedSotSheet = Object.keys($scope.sotJson)[0]
    mainService.getOutputData(function(response){
      $scope.altJson = response.data;
      console.log(JSON.stringify($scope.altJson));
      $scope.psiJson = $scope.altJson.Psi;
      $scope.sotJson = $scope.altJson.Sot;
      $scope.selectedSheet = Object.keys($scope.altJson)[0];
      $scope.activePsiMap = $scope.psiJson[0];
      $scope.selectedSotSheet = Object.keys($scope.sotJson)[0]
    });
  }

  $scope.showSheet = function(item){
    $scope.selectedSheet = item;
    $scope.activePsiMap = $scope.psiJson[0];
    $scope.activeSotMap = $scope.sotJson[$scope.selectedSotSheet];
  }

  $scope.setActivePsiData = function(key,data){
    $scope.selectedPsiSheet = key;
    $scope.activePsiMap = data;
  }

  $scope.setActiveSotData = function(key,data){
    $scope.selectedSotSheet = key;
    $scope.activeSotMap = data;
  }
  $scope.errorOutput = function(item){
    if (item!=undefined && item!=null) {
      if(item.includes("Clarify:")){
        return true;
      }
      else if (item.includes("Reject:")) {
        return true;
      }
      else if (item.includes("Error:")) {
        return true;
      }
      else
        return false;
    }
  }

});
