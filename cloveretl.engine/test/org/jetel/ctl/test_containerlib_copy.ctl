integer[] origList;
integer[] copyList;
integer[] returnedList;

map[string, string] origMap;
map[string, string] copyMap;
map[string, string] returnedMap;

function integer transform() {
	origList = [1,2,3,4,5];
	//copy
	returnedList = copy(copyList,origList);
	
	origMap["a"] = "a"; 
	origMap["b"] = "b"; 
	origMap["c"] = "c"; 
	origMap["d"] = "d";
	
	returnedMap = copy(copyMap, origMap);
	 
	return 0;
}
