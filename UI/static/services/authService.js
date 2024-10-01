angular.module("myapp")
.factory('authService', function($http,$q,$window,encryptDecryptService) {

  var auth = { user: undefined };

  auth.saveUser = function(user) {
    if(user !== undefined) {
      if(user.name) {
        //@TODO set a expiry time stamp user.sessionTime = Date.now() + 2 minutes??
        $window.sessionStorage['member-user'] = JSON.stringify(user);
      } else {
        console.log("Invalid user data for auth: ", user);
        auth.removeUser();
      }
    } else {
      console.log("Undefined user data for auth: ", user);
      auth.removeUser();
    }
  };

  auth.removeUser = function() {
    $window.sessionStorage.removeItem('member-user');
    //Commenting this line, as this may refresh the view/page, which is against SPA style
    //$window.location.reload();
  };

  auth.getUser = function() {
    var u = $window.sessionStorage['member-user'];
    if (u !== undefined)
    u = JSON.parse(u);
    return u;
  };

  auth.getGuest = function() {
    var guest = {};
    return guest;
  };

  auth.isMember = function() {
    var user = auth.getUser();
    if (user === undefined) {
      return false;
    } else {
      //@TODO check expiry timestamp on user session object
      return true;
    }
  };

  auth.getCurrentUser = function() {
    if (auth.isMember()) {
      return auth.getUser();
    } else {
      return auth.getGuest();
    }
  };

auth.signIn = function(userData) {
  var signinFormData=userData;
  return $q(function(resolve, reject) {
    encryptFormData = encryptDecryptService.encrypt(JSON.stringify(signinFormData),encryptDecryptService.key);
    $http.post('/signin/',encryptFormData)
    .then(function(res) {
      //success
      if (res.status >= 400) {
        //can be unauthorized and hence error
        auth.removeUser(); //ensuring user is not saved locally
        reject(res.data);
      } else if (res.status >= 200 && res.status <= 299) {
        if(res.data ) {
          signinFormData.role = res.data;
          //Successfully authenticated
          auth.saveUser(signinFormData);
          //console.log("response",res.data.token);
          resolve(auth.getCurrentUser());
        } else {
          //Login request passed but required data was not returned
          auth.removeUser();
          reject(res.data);
        }
      }
    },
    function(res) {
      //error
      //console.log("Sign-in returned with error status: ", res.status, ", Error: ", res.data);
      reject(res.data);
    }
  );
});
};

auth.signout = function() {
  //As a first step invalidate or destroy the local user object
  getCurrentUser = encryptDecryptService.encrypt(JSON.stringify(auth.getCurrentUser()),encryptDecryptService.key);
  auth.removeUser();
   return $.ajax({
   traditional: true,
    type: 'POST',
    async: false,
    url: '/signout/',
    dataType: 'json',
    contentType : 'application/json',
    data: getCurrentUser,
   })

  //Returning promise object
//  return $q(function(resolve, reject) {
//    $http.post('/signout/',getCurrentUser).then(function(res) {
//      //success
//      resolve("Signed-out successfully..!");
//      // resolve(res.data)
//    }, function(res) {
//      //error
//      reject(res.data);
//    }
//  );
//});
};
//console.log(auth);
return auth;
});
