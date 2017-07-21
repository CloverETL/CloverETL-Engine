integer resultInt;
long resultLong;
integer test_zero_int;
long test_zero_long;

byte resultByte;
byte testZeroByte;

function integer transform() {

	resultInt = bitNegate(59081716);
	resultLong = bitNegate(3321654987654105968L);
	test_zero_int = bitNegate(0);
	test_zero_long = bitNegate(0L);
	
	// CA93 = 1100101010010011
	// 356C = 0011010101101100
	resultByte = bitNegate(hex2byte("CA93")); // 1100101010010011
	testZeroByte = bitNegate(hex2byte("00000000"));
	
	return 0;
}