string result1;
string result2;
string result3;
string result4;
string result5;
string result6;
string result7;
string result8;

string zeroLength1;
string zeroLength2;

function integer transform() {
	result1 = "abc".rpad(6, "-");
	result2 = "abc".rpad(2, "-");
	result3 = "abc".rpad(6);
	result4 = "abc".rpad(2);

	result5 = rpad(null, 5);
	result6 = rpad(null, 5, "-");
	
	result7 = " ".rpad(4, "x");
	result8 = " ".rpad(4);
	
	zeroLength1 = "abc".rpad(0, "-");
	zeroLength2 = "abc".rpad(0);
	
	return 0;
}