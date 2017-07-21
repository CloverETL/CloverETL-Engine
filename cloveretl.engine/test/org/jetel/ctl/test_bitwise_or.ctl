integer resultInt1;
integer resultInt2;
integer resultInt3;
integer resultInt4;
long resultLong1;
long resultLong2;
long resultLong3;
long resultLong4;
long resultMix1;
long resultMix2;

function integer transform() {

	resultInt1 = bitOr(0,1);
	resultInt2 = bitOr(1,1);
	resultInt3 = bitOr(2,1);
	resultInt4 = bitOr(3,1);
	resultLong1 = bitOr(0l,1l);
	resultLong2 = bitOr(1l,1l);
	resultLong3 = bitOr(2l,1l);
	resultLong4 = bitOr(3l,1l);
	resultMix1 = bitOr(13,2L);
	resultMix2 = bitOr(13L,2);
	
	return 0;
}