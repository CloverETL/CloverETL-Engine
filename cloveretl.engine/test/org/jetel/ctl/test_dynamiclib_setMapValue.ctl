map[string, string] strMap;
map[string, integer] intMap;
map[string, date] dateMap;
map[string, byte] byteMap;
map[string, decimal] decimalMap;

map[string, string] ret1;
map[string, string] ret2;
map[string, string] ret3;
map[string, string] ret4;
map[string, string] ret5;

function integer transform(){
	
	strMap["firstName"] = "John";
	strMap["lastName"] = "Doe";
	strMap["address"] = "Jersey";

	intMap["count"] = 123;
	intMap["max"] = 456;
	intMap["sum"] = 789;

	dateMap["before"] = str2date("1970-01-01 01:00:12", "yyyy-MM-dd HH:mm:ss");
	dateMap["after"] = str2date("1970-01-01 01:00:34", "yyyy-MM-dd HH:mm:ss");
	
	byteMap["hash"] = str2byte('aa', "UTF-8");
	byteMap["checksum"] = str2byte('bb', "UTF-8");
	
	decimalMap["asset"] = 12.34D;
	decimalMap["liability"] = 56.78D;
	
	setMapValue($out.4, "stringMapField", strMap);
	setMapValue($out.4, "integerMapField", intMap);
	setMapValue($out.4, "dateMapField", dateMap);
	setMapValue($out.4, "byteMapField", byteMap);
	setMapValue($out.4, "decimalMapField", decimalMap);
	ret1 = getStringMapValue($out.4, "stringMapField");
	ret2 = getStringMapValue($out.4, "integerMapField");
	ret3 = getStringMapValue($out.4, "dateMapField");
	ret4 = getStringMapValue($out.4, "byteMapField");
	ret5 = getStringMapValue($out.4, "decimalMapField");

	return 0;
}