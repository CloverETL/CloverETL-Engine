long toLong;
long toLong2;
long toLong3;
long nullRet1;
long nullRet2;

function integer transform() {
	double double1 = 0.007;
	double double2 = -500.123;
	double double3 = 10000000000.0;
	toLong = double2long(double1);
	toLong2 = double2long(double2);
	toLong3 = double2long(double3);
	double dou = null;
	nullRet1 = double2long(dou);
	nullRet2 = double2long(null);
	return 0;
}