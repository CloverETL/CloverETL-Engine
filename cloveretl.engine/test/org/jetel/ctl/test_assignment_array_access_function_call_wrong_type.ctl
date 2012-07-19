function integer transform() {
	map[string, string] originalMap;
	map[string, string] copiedMap;

	upperCase("dummy text")["c"] = "d";
	copy(copiedMap, originalMap)["c"] = 9;
	copy(copiedMap, originalMap)[4] = "d";
	
	return 0;
}