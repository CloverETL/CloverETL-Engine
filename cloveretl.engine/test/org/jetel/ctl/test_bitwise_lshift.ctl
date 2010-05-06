int resultInt1;
int resultInt2;
int resultInt3;
int resultInt4;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;

function int transform() {

	resultInt1 = bit_lshift(1,1);
	resultInt2 = bit_lshift(1,2);
	resultInt3 = bit_lshift(5,1);
	resultInt4 = bit_lshift(5,2);
	resultLong1 = bit_lshift(1l,1l);
	resultLong2 = bit_lshift(1l,2l);
	resultLong3 = bit_lshift(5l,1l);
	resultLong4 = bit_lshift(5l,2l);
		
	return 0;
}