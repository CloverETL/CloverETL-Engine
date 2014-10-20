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

	resultInt1 = bitXor(0,1);
	resultInt2 = bitXor(1,1);
	resultInt3 = bitXor(2,1);
	resultInt4 = bitXor(3,1);
	resultLong1 = bitXor(0l,1l);
	resultLong2 = bitXor(1l,1l);
	resultLong3 = bitXor(2l,1l);
	resultLong4 = bitXor(3l,1l);

	test_mixed1 = bitXor(31L,16);
	test_mixed2 = bitXor(48,12L);

	resultByte1 = bitXor(hex2byte("00F0FF"), hex2byte("010101"));
	resultByte2 = bitXor(hex2byte("0001FF"), hex2byte("F1F1F1"));
	resultByte3 = bitXor(hex2byte("0072FF"), hex2byte("717171"));
	resultByte4 = bitXor(hex2byte("00F3FF"), hex2byte("F1F1F1"));
	
	resultByteDifferentLength = bitXor(hex2byte("F0F0F0F0"), hex2byte("FF0077"));

	return 0;
}