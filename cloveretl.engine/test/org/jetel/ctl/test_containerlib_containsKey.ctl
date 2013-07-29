string[] keys = ["name", "firstName", "given name", "lastName", "fullName", "address"];
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
function integer transform() {
	for (integer i = 0; i < keys.length(); i++) {
		results[i] = $in.multivalueInput.stringMapField.containsKey(keys[i]);
	}
	
	map[integer, string] intMap;
	intMap[1] = 'List';
	intMap[2] = 'Bach';
	intMap[3] = 'Ravel';
	integer intTest = null;
	
	test1 = intMap.containsKey(1);
	test2 = intMap.containsKey(intTest);
	
	map[string, integer] strMap;
	strMap['a'] = 15;
	strMap['f'] = 54;
	strMap[null] = 567;
	string strKey = null;
	test3 = strMap.containsKey(strKey);
	test4 = strMap.containsKey('');
	
	map[boolean, integer] boolMap;
	boolMap[true] = 15;
	boolMap[null] = 125;
	boolean bool = null;
	test5 = boolMap.containsKey(true);
	test6 = boolMap.containsKey(false);
	test7 = boolMap.containsKey(bool);
	
	map[date, long] dateMap;
	dateMap[str2date('11-11-2003','MM-dd-yyyy')] = 90L;
	dateMap[null] = 70L;
	date d = null;
	
	test8 = dateMap.containsKey(d);
	test9= dateMap.containsKey(str2date('11-11-2003','MM-dd-yyyy'));
	test10 = dateMap.containsKey(str2date('2006-10-23','yyyy-MM-dd'));
	 
	map[decimal,string] decMap;
	decMap[12.6d] = 'a';
	decMap[null] = 'c';
	decimal dec1 = null;
	
	test11 = decMap.containsKey(dec1);
	test12 = decMap.containsKey(12.6d);
	test13 = decMap.containsKey(63.989d);
	
	map[number, integer] numMap;
	numMap[12.6] = 468453;
	numMap[null] = 5643;
	number num1 = null;
	
	test14 = numMap.containsKey(num1);
	test15 = numMap.containsKey(12.6);
	test16 = numMap.containsKey(87.6);
	
	map[long, long] longMap;
	longMap[12L] = 135L;
	longMap[null] = 785l;
	long lo1 = null;
	
	test17 = longMap.containsKey(lo1);
	test18 = longMap.containsKey(12L);
	test19 = longMap.containsKey(15l);
	
	map[string,integer] emptyMap;
	test20 = emptyMap.containsKey('a');
	return 0;
}