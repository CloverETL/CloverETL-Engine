integer test1;
integer test2;
integer test3;
long test4;
long test5;
long test6;
function integer transform(){
	test1 = bitSet(11,3,false);
	test2 = bitSet(11,2,true);
	test3 = bitSet(2,5,true);
	test4 = bitSet(11L,3,false);
	test5 = bitSet(11l,2,true);
	test6 = bitSet(2l,5,true);
	return 0;
}