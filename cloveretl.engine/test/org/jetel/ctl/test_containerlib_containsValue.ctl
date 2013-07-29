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

	return 0;
}