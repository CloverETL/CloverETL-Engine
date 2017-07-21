map[string, string] originalMap;
map[string, string] copiedMap;
	
function integer transform() {
	
	originalMap["a"] = "b";
	
	copy(copiedMap, originalMap)["c"] = "d";
	
	return 0;
}