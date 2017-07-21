string[] stringList;
string[] stringList2;
integer[] integerList;
integer[] integerList2;
date[] dateList;
date[] dateList2;
long[] longList;
long[] longList2;
number[] numList;
number[] numList2;
decimal[] decList;
decimal[] decList2;

integer[] emptyList;
integer[] emptyList2;

function integer transform() {
	map[string, integer] stringMap;
	stringMap['a'] = 1;
	stringMap['b'] = 2;
	stringList = stringMap.getKeys();
	stringList2 = getKeys(stringMap);
	
	map[integer, string] integerStringMap;
	integerStringMap[5] = "five";
	integerStringMap[7] = "seven";
	integerStringMap[2] = "two";
	integerList = integerStringMap.getKeys();
	integerList2 = getKeys(integerStringMap);
	
	map[date, integer] dateMap;
	dateMap[str2date('12-11-2008', 'dd-MM-yyyy')] = 23;
	dateMap[str2date('28-06-2001', 'dd-MM-yyyy')] = 89;
	dateList = dateMap.getKeys();
	dateList2 = getKeys(dateMap);
	
	map[long,integer] longMap;
	longMap[14L] = 145;
	longMap[45L] = 231;
	longList = longMap.getKeys();
	longList2 = getKeys(longMap);
	
	map[number, integer] numMap;
	numMap[12.3] = 11;
	numMap[13.4] = 22;
	numList = numMap.getKeys();
	numList2 = getKeys(numMap);
	
	map[decimal, integer] decMap;
	decMap[34.5d] = 87;
	decMap[45.6d] = 894;
	decList = decMap.getKeys();
	decList2 = getKeys(decMap);
	
	map[integer, long] emptyMap;
	emptyList = emptyMap.getKeys();
	emptyList2 = getKeys(emptyMap);
	return 0;
}