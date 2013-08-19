string hexResult;
string test_null1;
string test_null2;
function integer transform() {
	hexResult = byte2hex($firstInput.ByteArray);
	test_null1 = byte2hex(null);
	byte b = null;
	test_null2 = byte2hex(b);
	return 0;
}