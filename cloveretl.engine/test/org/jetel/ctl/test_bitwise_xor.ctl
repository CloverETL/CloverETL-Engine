integer resultInt1;
integer resultInt2;
integer resultInt3;
integer resultInt4;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;

long test_mixed1;
long test_mixed2;

function integer transform() {

	resultInt1 = bitXor(0,1);
	resultInt2 = bitXor(1,1);
	resultInt3 = bitXor(2,1);
	resultInt4 = bitXor(3,1);
	resultLong1 = bitXor(0l,1l);
	resultLong2 = bitXor(1l,1l);
	resultLong3 = bitXor(2l,1l);
	resultLong4 = bitXor(3l,1l);
//	CLO-1415
//	test_mixed1 = bitXor(31L,16);
//	printErr(test_mixed1);
//	test_mixed2 = bitXor(48,12L);
//	printErr(test_mixed2);
	return 0;
}