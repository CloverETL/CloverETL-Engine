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
	result1 = "abc".lpad(6, "-");
	result2 = "abc".lpad(2, "-");
	result3 = "abc".lpad(6);
	result4 = "abc".lpad(2);

	result5 = lpad(null, 5);
	result6 = lpad(null, 5, "-");
	
	result7 = " ".lpad(4, "x");
	result8 = " ".lpad(4);
	
	zeroLength1 = "abc".lpad(0, "-");
	zeroLength2 = "abc".lpad(0);
	
	return 0;
}