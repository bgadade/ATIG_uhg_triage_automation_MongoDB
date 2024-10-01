angular.module("myapp")
.factory('encryptDecryptService', function() {

// encryption/decryption
  var encryptDecrypt = {};
  encryptDecrypt.key = CryptoJS.enc.Utf8.parse('1234567890123456');
  encryptDecrypt.encrypt = function(msgString, key) {
        // msgString is expected to be Utf8 encoded
        var iv = CryptoJS.lib.WordArray.random(16);
        var encrypted = CryptoJS.AES.encrypt(msgString, key, {
            iv: iv
        });
        return iv.concat(encrypted.ciphertext).toString(CryptoJS.enc.Base64);
    }

  encryptDecrypt.decrypt = function(ciphertextStr, key) {
        var ciphertext = CryptoJS.enc.Base64.parse(ciphertextStr);

        // split IV and ciphertext
        var iv = ciphertext.clone();
        iv.sigBytes = 16;
        iv.clamp();
        ciphertext.words.splice(0, 4); // delete 4 words = 16 bytes
        ciphertext.sigBytes -= 16;

        // decryption
        var decrypted = CryptoJS.AES.decrypt({ciphertext: ciphertext}, key, {
            iv: iv
        });
        return decrypted.toString(CryptoJS.enc.Utf8);
    }
    return encryptDecrypt;
});
