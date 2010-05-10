integer resultInt1;
integer resultInt2;
integer resultInt3;
integer resultInt4;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;

function integer transform() {

	resultInt1 = bit_or(0,1);
	resultInt2 = bit_or(1,1);
	resultInt3 = bit_or(2,1);
	resultInt4 = bit_or(3,1);
	resultLong1 = bit_or(0l,1l);
	resultLong2 = bit_or(1l,1l);
	resultLong3 = bit_or(2l,1l);
	resultLong4 = bit_or(3l,1l);
	
	return 0;
}