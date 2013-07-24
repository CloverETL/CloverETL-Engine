integer resultInt1;
integer resultInt2;
integer resultInt3;
integer resultInt4;
integer resultInt5;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;
long resultLong5;
long test_mixed1;
long test_mixed2;

function integer transform() {

	resultInt1 = bitLShift(1,1);
	resultInt2 = bitLShift(1,2);
	resultInt3 = bitLShift(5,1);
	resultInt4 = bitLShift(5,2);
	resultInt5 = bitLShift(1,-1);
	resultLong1 = bitLShift(1l,1l);
	resultLong2 = bitLShift(1l,2l);
	resultLong3 = bitLShift(5l,1l);
	resultLong4 = bitLShift(5l,2l);
	resultLong5 = bitLShift(1l,-1l);
//	test_mixed1 = bitLShift(22,3l);
//	test_mixed2 = bitLShift(77l,3);
//	printErr(test_mixed1);
//	printErr(test_mixed2);
	
	return 0;
}