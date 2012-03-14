string integerString;
string longString;
string doubleString;
string decimalString;
string listString;
string mapString;
string byteMapString;
string fieldByteMapString;
string byteListString;
string fieldByteListString;

function integer transform() {
	integer integerToString = 10; 
	long longToString = 110654321874L; 
	double doubleToString = 0.00000000000001547874; 
	decimal decimalToString = -6847521431.1545874d;
	string[] listToString = ["not ALI A", "not ALI B", "not ALI D...", "but", "ALI H!"];
	map[integer, string] mapToString;
	mapToString[1] =  "Testing";
	mapToString[2] = "makes";
	mapToString[3] = "me";
	mapToString[4] = "crazy :-)";
	
	map[byte, byte] byteByteMap;
	byteByteMap[str2byte("key1", "utf-8")] = str2byte("value1", "utf-8");
	byteByteMap[str2byte("key2", "utf-8")] = str2byte("value2", "utf-8");
	
	map[string, byte] stringByteMap;
	stringByteMap["key1"] = str2byte("value1", "utf-8");
	stringByteMap["key2"] = str2byte("value2", "utf-8");
	$out.firstMultivalueOutput.byteMapField = stringByteMap;
	
	byte[] byteList = [str2byte("firstElement", "utf-8"), str2byte("secondElement", "utf-8")];
	$out.firstMultivalueOutput.byteListField = byteList;
	
	integerString = toString(integerToString);
	longString = toString(longToString);
	doubleString = toString(doubleToString);
	decimalString = toString(decimalToString);
	listString = toString(listToString);
	mapString = toString(mapToString);
	
	byteMapString = byteByteMap.toString();
	fieldByteMapString = $out.firstMultivalueOutput.byteMapField.toString();
	byteListString = byteList.toString();
	fieldByteListString = $out.firstMultivalueOutput.byteListField.toString();
	
	return 0;
}