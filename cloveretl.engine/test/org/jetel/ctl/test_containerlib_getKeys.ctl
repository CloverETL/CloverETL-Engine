string[] stringList;
integer[] integerList;
map[integer, string] integerStringMap;

function integer transform() {
	stringList = $in.multivalueInput.integerMapField.getKeys();
	integerStringMap[5] = "five";
	integerStringMap[7] = "seven";
	integerStringMap[2] = "two";
	integerList = integerStringMap.getKeys();
	return 0;
}