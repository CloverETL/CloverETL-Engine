string[] values = ["John", "Johnny", "Little John", "Doe", "Defoe", "Dee", "Jersey", "New York"];
boolean[] results;

boolean test1;
boolean test2;
boolean test3;
boolean test4;
boolean test5;
boolean test6;
boolean test7;
boolean test8;
boolean test9;
boolean test10;
boolean test11;
boolean test12;
boolean test13;
boolean test14;
boolean test15;
boolean test16;
boolean test17;
boolean test18;
boolean test19;
boolean test20;
boolean test21;
boolean test22;
boolean test23;
boolean test24;
boolean test25;
boolean test26;

boolean[] listResults;
boolean listEmptyTest1;
boolean listEmptyTest2;
boolean listEmptyTest3;
boolean listEmptyTest4;
boolean integerToLongTest;

boolean listTest1;
boolean listTest2;
boolean listTest3;
boolean listTest4;
boolean listTest5;
boolean listTest6;
boolean listTest7;
boolean listTest8;
boolean listTest9;
boolean listTest10;
boolean listTest11;
boolean listTest12;
boolean listTest13;
boolean listTest14;
boolean listTest15;
boolean listTest16;
boolean listTest17;
boolean listTest18;
boolean listTest19;
boolean listTest20;
boolean listTest21;
boolean listTest22;
boolean listTest23;
boolean listTest24;
boolean listTest25;

function integer transform() {
	for (integer i = 0; i < values.length(); i++) {
		results[i] = $in.multivalueInput.stringMapField.containsValue(values[i]);
	}
	
	map[integer, boolean] boolMap;
	boolMap[12] = true;
	
	boolean bool = null;
	boolMap[15] = bool;
	test1 = boolMap.containsValue(bool);	
	test2 = boolMap.containsValue(true);
	test3 = boolMap.containsValue(false);
	
	map[integer, byte] byteMap;
	byte myByte = str2byte('booohooo','utf-8');
	byteMap[1] = myByte;
	byte myByte2 = null;
	byteMap[2] = myByte2;
	test4 = byteMap.containsValue(myByte2);
	test5 = byteMap.containsValue(myByte);
	test6 = byteMap.containsValue(str2byte('smile','utf-16'));
	test7 = byteMap.containsValue(str2byte('booohooo','utf-8'));
	
	map[integer, date] dateMap;
	dateMap[1] = str2date('2001-11-18','yyyy-MM-dd');
	date myDate = null;
	dateMap[3] = myDate;
	test8 = dateMap.containsValue(myDate);
	test9 = dateMap.containsValue(str2date('2001-11-18','yyyy-MM-dd'));
	test10 = dateMap.containsValue(str2date('10-11-2003','dd-MM-yyyy'));
	
	map[integer, integer] intMap;
	intMap[1] = 45;
	integer myInt = null;
	intMap[2] = myInt;
	test11 = intMap.containsValue(myInt);
	test12 = intMap.containsValue(45);
	test13 = intMap.containsValue(365436);
	
	map[integer, long] longMap;
	longMap[1] = 54l;
	long myLong = null;
	longMap[4] = myLong;
	test14 = longMap.containsValue(54L);
	test15 = longMap.containsValue(myLong);
	test16 = longMap.containsValue(5456L);
	
	map[integer, number] numMap;
	numMap[1] = 15.9;
	number myNum = null;
	numMap[3] = myNum;
	test17 = numMap.containsValue(myNum);
	test18 = numMap.containsValue(15.9);
	test19 = numMap.containsValue(45.9);
	
	map[integer, string] strMap;
	strMap[1] = 'c';
	string myStr = null;
	strMap[5] = myStr;
	test20 = strMap.containsValue('c');
	test21 = strMap.containsValue(myStr);
	test22 = strMap.containsValue('popocatepetl');
	
	map[integer, decimal] decMap;	
	decMap[1] = 23.34d;
	decimal myDec = null;
	decMap[6] = myDec;
	test23 = decMap.containsValue(myDec);
	test24 = decMap.containsValue(23.34d);
	test25 = decMap.containsValue(89.354d);
	
	map[integer, integer] emptyMap;
	test26 = emptyMap.containsValue(15);
	
	for (integer i = 0; i < values.length(); i++) {
		listResults[i] = $in.multivalueInput.stringListField.containsValue(values[i]);
	}
	
	boolean[] boolList;
	listEmptyTest1 = boolList.containsValue(null);	
	listEmptyTest2 = boolList.containsValue(true);
	listEmptyTest3 = boolList.containsValue(false);
	boolList[5] = true;
	listTest1 = boolList.containsValue(null);	
	listTest2 = boolList.containsValue(true);
	listTest3 = boolList.containsValue(false);
	
	byte[] byteList = [myByte, myByte2];
	listTest4 = byteList.containsValue(myByte2);
	listTest5 = byteList.containsValue(myByte);
	listTest6 = byteList.containsValue(str2byte('smile','utf-16'));
	listTest7 = byteList.containsValue(str2byte('booohooo','utf-8'));

	date[] dateList = [str2date('2001-11-18','yyyy-MM-dd'), myDate];
	listTest8 = dateList.containsValue(myDate);
	listTest9 = dateList.containsValue(str2date('2001-11-18','yyyy-MM-dd'));
	listTest10 = dateList.containsValue(str2date('10-11-2003','dd-MM-yyyy'));

	integer[] intList = [45, myInt];
	listTest11 = intList.containsValue(myInt);
	listTest12 = intList.containsValue(45);
	listTest13 = intList.containsValue(365436);

	long[] longList = [54l, myLong, 87];
	listTest14 = longList.containsValue(54L);
	listTest15 = longList.containsValue(myLong);
	listTest16 = longList.containsValue(5456L);
	integerToLongTest = longList.containsValue(87l);

	number[] numList = [15.9, myNum];
	listTest17 = numList.containsValue(myNum);
	listTest18 = numList.containsValue(15.9);
	listTest19 = numList.containsValue(45.9);

	string[] strList = ['c', myStr];
	listTest20 = strList.containsValue('c');
	listTest21 = strList.containsValue(myStr);
	listTest22 = strList.containsValue('popocatepetl');

	decimal[] decList = [23.34d, myDec];	
	listTest23 = decList.containsValue(myDec);
	listTest24 = decList.containsValue(23.34d);
	listTest25 = decList.containsValue(89.354d);

	integer[] emptyList;
	listEmptyTest4 = emptyList.containsValue(15);

	return 0;
}