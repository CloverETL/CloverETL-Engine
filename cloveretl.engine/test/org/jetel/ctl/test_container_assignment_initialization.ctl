string[] arrayString;
map[string,string] mapString;
boolean listNull;
boolean mapNull;

function integer transform() {
	//LIST
	$out.5.stringListField[1] = "value";
	
	string[] a;
	$out.4.stringListField = a;
	$out.4.stringListField[0] = "value";
	
	arrayString = null;
	arrayString[0] = "value";
	
	dictionary.stringList = null;
	listNull = dictionary.stringList == null;
	dictionary.stringList[1] = "value";
	
	
	
	
	
	
	//MAP
	$out.5.stringMapField["key"] = "value";
	
	map[string,string] b;
	$out.4.stringMapField = b;
	$out.4.stringMapField["key"] = "value";
	
	mapString = null;
	mapString["key"] = "value";
	
	dictionary.stringMap = null;
	mapNull = dictionary.stringMap == null;
	dictionary.stringMap["key"] = "value";
	
	return 0;
}
