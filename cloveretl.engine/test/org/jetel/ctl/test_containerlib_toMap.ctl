map[integer, boolean] integerBooleanMap;
map[integer, boolean] integerBooleanMapNull;
map[integer, boolean] integerBooleanMapTrue;

map[decimal, long] emptyMap;
map[number, byte] nullMap;
map[date, string] duplicateMap;

string[] keys1;
string[] keys2;
string[] values;

function integer transform() {

	integerBooleanMap = toMap([5, 8], [true, false]);
	boolean nullBoolean = null;
	integerBooleanMapNull = toMap([5, 8], nullBoolean);
	integerBooleanMapTrue = toMap([5, 8], true);
	
	decimal[] decimalList;
	long[] longList;
	emptyMap = toMap(decimalList, longList);
	
	// list containing null
	number[] numberList;
	numberList.push(null);
	byte[] byteList;
	byteList.push(null);
	nullMap = toMap(numberList, byteList);
	
	// duplicate keys
	date[] dateList = [long2date(7), null, long2date(7), null, long2date(46)];
	string[] stringList = ["A", "B", "C", "D", "E"];
	duplicateMap = toMap(dateList, stringList);
	
	// insertion order
	string[] upperCase = ["A", "B", "C", "D", "E", "F", "G"];
	string[] lowerCase = ["a", "b", "c", "d", "e", "f", "g"];
	map[string, string] map1 = toMap(upperCase, lowerCase);
	map[string, string] map2 = toMap(upperCase, "x");
	keys1 = map1.getKeys();
	keys2 = map2.getKeys();
	foreach (string v: map1) {
		values.push(v);
	}
	
	return 0;
}