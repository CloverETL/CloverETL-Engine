int resultInt1;
int resultInt2;
int resultInt3;
int resultInt4;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;

function int transform() {

	resultInt1 = bit_rshift(5,1);
	resultInt2 = bit_rshift(5,3);
	resultInt3 = bit_rshift(8,1);
	resultInt4 = bit_rshift(8,2);
	resultLong1 = bit_rshift(5l,1l);
	resultLong2 = bit_rshift(5l,3l);
	resultLong3 = bit_rshift(8l,1l);
	resultLong4 = bit_rshift(8l,2l);
		
	return 0;
}