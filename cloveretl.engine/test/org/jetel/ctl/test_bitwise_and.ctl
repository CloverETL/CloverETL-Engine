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

byte resultByte1;
byte resultByte2;
byte resultByte3;
byte resultByte4;

byte resultByteDifferentLength;

function integer transform() {

	resultInt1 = bitAnd(0,1);
	resultInt2 = bitAnd(1,1);
	resultInt3 = bitAnd(2,1);
	resultInt4 = bitAnd(3,1);
	resultLong1 = bitAnd(0l,1l);
	resultLong2 = bitAnd(1l,1l);
	resultLong3 = bitAnd(2l,1l);
	resultLong4 = bitAnd(3l,1l);
	test_mixed1 = bitAnd(6,12l);
	test_mixed2 = bitAnd(6l,12);
	
	resultByte1 = bitAnd(hex2byte("00F0FF"), hex2byte("010101"));
	resultByte2 = bitAnd(hex2byte("0001FF"), hex2byte("F1F1F1"));
	resultByte3 = bitAnd(hex2byte("0072FF"), hex2byte("717171"));
	resultByte4 = bitAnd(hex2byte("00F3FF"), hex2byte("F1F1F1"));
	
	resultByteDifferentLength = bitAnd(hex2byte("F0F0F0F0"), hex2byte("FF0077"));
	
	return 0;
}