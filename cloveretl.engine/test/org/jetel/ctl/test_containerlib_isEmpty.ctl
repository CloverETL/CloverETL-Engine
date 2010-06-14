boolean emptyMap;
boolean fullMap;
boolean emptyList;
boolean fullList;

function integer transform() {
	map[integer, integer] myMap;
	emptyMap = isEmpty(myMap);
	myMap[1] = 3;
	fullMap = isEmpty(myMap);
	
	integer[] myList;
	emptyList = isEmpty(myList);
	myList.append(10);
	fullList = isEmpty(myList);
	return 0;
}