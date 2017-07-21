boolean emptyMap;
boolean emptyMap1;
boolean fullMap;
boolean fullMap1;
boolean emptyList;
boolean emptyList1;
boolean fullList;
boolean fullList1;
boolean emptyString;
boolean fullString;

function integer transform() {
	map[integer, integer] myMap;
	emptyMap = isEmpty(myMap);
	emptyMap1 = myMap.isEmpty();
	myMap[1] = 3;
	fullMap = isEmpty(myMap);
	fullMap1 = myMap.isEmpty();
	integer[] myList;
	emptyList = isEmpty(myList);
	emptyList1 = myList.isEmpty();
	myList.append(10);
	fullList = isEmpty(myList);
	fullList1 = myList.isEmpty();
	string myString1;
	string myString2;
	myString1=null;
	myString2=" ";
	emptyString=isEmpty(myString1);
	fullString=isEmpty(myString2);
	return 0;
}