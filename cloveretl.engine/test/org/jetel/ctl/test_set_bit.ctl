integer resultInt1;
integer resultInt2;
long resultLong1;
long resultLong2;
boolean resultBool1;
boolean resultBool2;
boolean resultBool3;
boolean resultBool4;


function integer transform() {

	
	resultInt1 = bitSet(0xFF,9,true);
	resultInt2 = bitSet(0xFF,2,false);
	resultLong1 = bitSet(0l, 50, true);
	resultLong2 = bitSet(0xFFFFFFFFFFFFFFFl,49,false);
	resultBool1 = bitIsSet(resultInt1, 9);
	resultBool2 = bitIsSet(resultInt2, 2);
	resultBool3 = bitIsSet(resultLong1, 50);
	resultBool4 = bitIsSet(resultLong2, 49);
	
	return 0;
}
