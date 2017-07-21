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
	map[integer, string] stringMap;
	stringMap[1] = 'a';
	stringMap[2] = 'b';
	stringList = stringMap.getValues();
	stringList2 = getValues(stringMap);
	
	map[string, integer] integerStringMap;
	integerStringMap["five"] = 5;
	integerStringMap["seven"] = 7;
	integerStringMap["two"] = 2;
	integerList = integerStringMap.getValues();
	integerList2 = getValues(integerStringMap);
	
	map[integer, date] dateMap;
	dateMap[23] = str2date('12-11-2008', 'dd-MM-yyyy');
	dateMap[89] = str2date('28-06-2001', 'dd-MM-yyyy');
	dateList = dateMap.getValues();
	dateList2 = getValues(dateMap);
	
	map[integer, long] longMap;
	longMap[145] = 14L;
	longMap[231] = 45L;
	longList = longMap.getValues();
	longList2 = getValues(longMap);
	
	map[integer, number] numMap;
	numMap[11] = 12.3;
	numMap[22] = 13.4;
	numList = numMap.getValues();
	numList2 = getValues(numMap);
	
	map[integer, decimal] decMap;
	decMap[87] = 34.5d;
	decMap[894] = 45.6d;
	decList = decMap.getValues();
	decList2 = getValues(decMap);
	
	map[long, integer] emptyMap;
	emptyList = emptyMap.getValues();
	emptyList2 = getValues(emptyMap);
	return 0;
}