boolean[] results;


function integer transform() {

	string expected = "hello";
	decimal d = 123D;
	
	string s;
	s = dictionary.stringEntry;
	results.append(s == expected);
	
	results.append(dictionary.stringListEntry.containsValue(expected));
	
	results.append(dictionary.stringEntry == expected);
	results.append(dictionary.stringListEntry[0] == expected);
	results.append(dictionary.stringMapEntry["key"] == expected);

	results.append(dictionary.decimalEntry == d);
	results.append(dictionary.decimalListEntry[0] == d);
	results.append(dictionary.decimalMapEntry["key"] == d);
	
	results.append((dictionary.stringListEntry = dictionary.stringListEntry + ["aa"])[0] == expected);
	map[string, string] tmpMap;
	tmpMap["a"] = "b";
	results.append((dictionary.stringMapEntry = dictionary.stringMapEntry + tmpMap)["key"] == expected);
	
	// TODO compound assignment operators
	
	
	return 0;
}

