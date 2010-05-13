integer resultInt1;
integer resultInt2;
long resultLong1;
long resultLong2;
boolean resultBool1;
boolean resultBool2;
boolean resultBool3;
boolean resultBool4;


function integer transform() {

	
	resultInt1 = bit_set(0xFF,9,true);
	resultInt2 = bit_set(0xFF,2,false);
	resultLong1 = bit_set(0l, 50, true);
	resultLong2 = bit_set(0xFFFFFFFFFFFFFFFl,49,false);
	resultBool1 = bit_is_set(resultInt1, 9);
	resultBool2 = bit_is_set(resultInt2, 2);
	resultBool3 = bit_is_set(resultLong1, 50);
	resultBool4 = bit_is_set(resultLong2, 49);
	
	return 0;
}
