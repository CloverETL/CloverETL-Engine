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

byte resultByte1;
byte resultByte2;
byte resultByte3;
byte resultByte4;

byte resultByteDifferentLength;

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
	
	resultByte1 = bitOr(hex2byte("00F0FF"), hex2byte("010101"));
	resultByte2 = bitOr(hex2byte("0001FF"), hex2byte("F1F1F1"));
	resultByte3 = bitOr(hex2byte("0072FF"), hex2byte("717171"));
	resultByte4 = bitOr(hex2byte("00F3FF"), hex2byte("F1F1F1"));
	
	resultByteDifferentLength = bitOr(hex2byte("F0F0F0F0"), hex2byte("FF0077"));

	return 0;
}