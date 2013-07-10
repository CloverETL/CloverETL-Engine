string test1;
string test2;
string test3;
string test4;
string test5;
string test6;
string test7;
string test8;
string test9;
string test10;
string test11;
string test12;
function integer transform() {
	test1 = left('aaa',2);
	test2 = left('aaa',7);
	test3 = left('',3);
	test4 = left(null,4);
	
	test5 = left('abcdefgh',3,true);
	test6 = left('ab',4,true);
	test7 = left('',3,true);
	test8 = left(null,2,true);
	
	test9 = left('abcdefgh',3,false);
	test10 = left('abc',6,false);
	test11 = left('',5,false);
	test12 = left(null,4,false);
	return 0;
}