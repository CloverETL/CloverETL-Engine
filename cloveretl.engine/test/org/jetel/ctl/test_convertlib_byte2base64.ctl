string inputBase64;
string inputBase64wrapped;
string inputBase64nowrap;
string baseText;
string longText;

function integer transform() {
	inputBase64 = byte2base64($firstInput.ByteArray);
	
	baseText = "abcdefghijklmnopqrstuvwxyz ";
	longText = ""; 
	for (integer i = 0; i < 10; i++) {
		longText = longText + baseText;
	}
	byte longTextBytes = longText.str2byte("UTF-8");
	inputBase64wrapped = byte2base64(longTextBytes, true);
	inputBase64nowrap = byte2base64(longTextBytes, false);
	
	return 0;
}