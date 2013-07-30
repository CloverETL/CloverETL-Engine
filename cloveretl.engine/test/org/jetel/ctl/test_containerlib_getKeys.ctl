string[] stringList;
integer[] integerList;
date[] dateList;
long[] longList;
number[] numList;
decimal[] decList;
map[integer, string] integerStringMap;
integer[] emptyList;

function integer transform() {
	stringList = $in.multivalueInput.integerMapField.getKeys();
	integerStringMap[5] = "five";
	integerStringMap[7] = "seven";
	integerStringMap[2] = "two";
	integerList = integerStringMap.getKeys();
	
	map[date, integer] dateMap;
	dateMap[str2date('12-11-2008', 'dd-MM-yyyy')] = 23;
	dateMap[str2date('28-06-2001', 'dd-MM-yyyy')] = 89;
	dateList = dateMap.getKeys();
	
	map[long,integer] longMap;
	longMap[14L] = 145;
	longMap[45L] = 231;
	longList = longMap.getKeys();
	
	map[number, integer] numMap;
	numMap[12.3] = 11;
	numMap[13.4] = 22;
	numList = numMap.getKeys();
	
	map[decimal, integer] decMap;
	decMap[34.5d] = 87;
	decMap[45.6d] = 894;
	decList = decMap.getKeys();
	
	map[integer, long] emptyMap;
	emptyList = emptyMap.getKeys();
	return 0;
}