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
	
	results.append((dictionary.stringListEntry2 += ["bb"])[0] == expected);
	dictionary.stringListEntry3.append("cc");
	results.append(dictionary.stringListEntry3[0] == expected);
	
	results.append((dictionary.stringMapEntry2 += tmpMap)["key"] == expected);
	dictionary.stringMapEntry3["key2"] = "value2";
	results.append(dictionary.stringMapEntry3["key"] == expected);
	
	
	return 0;
}

