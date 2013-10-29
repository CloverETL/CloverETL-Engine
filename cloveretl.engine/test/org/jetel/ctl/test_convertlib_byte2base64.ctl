string inputBase64;
string inputBase64wrapped;
string inputBase64nowrap;
string baseText;
string longText;
string nullLiteralOutput;
string nullVariableOutput;
string nullLiteralOutputWrapped;
string nullVariableOutputWrapped;
string nullLiteralOutputNowrap;
string nullVariableOutputNowrap;

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
	
	byte nullVariable = null;
	nullLiteralOutput = byte2base64(null);
	nullVariableOutput = byte2base64(nullVariable);
	nullLiteralOutputWrapped = byte2base64(null, true);
	nullVariableOutputWrapped = byte2base64(nullVariable, true);
	nullLiteralOutputNowrap = byte2base64(null, false);
	nullVariableOutputNowrap = byte2base64(nullVariable, false);
	
	return 0;
}