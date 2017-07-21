integer resultInt1;
integer resultInt2;
integer resultInt3;
integer resultInt4;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;
integer test_neg1;
integer test_neg2;
long test_neg3;
long test_neg4;

long test_mix1;
long test_mix2;
function integer transform() {

	resultInt1 = bitRShift(5,1);
	resultInt2 = bitRShift(5,3);
	resultInt3 = bitRShift(8,1);
	resultInt4 = bitRShift(8,2);
	resultLong1 = bitRShift(5l,1l);
	resultLong2 = bitRShift(5l,3l);
	resultLong3 = bitRShift(8l,1l);
	resultLong4 = bitRShift(8l,2l);
		
	test_neg1 = bitRShift(12, -1);
	test_neg2 = bitRShift(24,-5);
	test_neg3 = bitRShift(12L,-5L);
	test_neg4 = bitRShift(55L,-1L);
//CLO - 1399	
	test_mix1 = bitRShift(122L,2);
	test_mix2 = bitRShift(158,2L);
	printErr(test_mix1);
	printErr(test_mix2);
	
	return 0;
}