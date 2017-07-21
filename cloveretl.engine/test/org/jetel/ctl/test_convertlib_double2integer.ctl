integer toInteger;
integer toInteger2;
integer toInteger3;
integer nullRet1;
integer nullRet2;

function integer transform() {
	double double1 = 0.007;
	double double2 = -500.123;
	double double3 = 1000000.0;
	toInteger = double2integer(double1);
	toInteger2 = double2integer(double2);
	toInteger3 = double2integer(double3);
	double dou = null;
	nullRet1 = double2integer(dou);
	nullRet2 = double2integer(null);
	return 0;
}