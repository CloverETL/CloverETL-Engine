//string joinedString;
string joinedString1;
string joinedString2;
//string joinedString3;

string test_empty1;
string test_empty2;
string test_empty3;
string test_empty4;
string test_empty5;
string test_empty6;
string test_empty7;
string test_empty8;
string test_empty9;
string test_empty10;

string test_null1;
string test_null2;
string test_null3;
string test_null4;
string test_null5;
string test_null6;
function integer transform() {
	//joinedString = join(',', "Bagr", 3, 3.5641, -87L, "CTL2");
	
	number[] joinNumbers = [5,54.65,67,231]; 
	map[integer, number] joinMap; 
	joinMap[80] = 5455.987; 
	joinMap[-5] = 5455.987; 
	joinMap[3] = 0.1; 
	
	joinedString1 = join('"', joinMap);
	joinedString2 = join('♫', joinNumbers);
	//joinedString3 = join('☺', joinNumbers, joinMap, "CTL2", 42);
	string[] arr = ['a','b','c'];
	test_empty1 = join('',arr);
	test_null1 = join(null,arr);
	arr= ['','',''];
	test_empty2 = join('',arr);
	test_empty3 = join(' ',arr);
	test_null2 = join(null,arr);
	arr=['a',null,'b'];
	test_empty4 = join('',arr);
	test_null3 = join(null,arr);
	
	test_empty5 = join('',joinMap);
	test_empty6 = join(' ',joinMap);
	test_null4 = join(null, joinMap);
	//CLO-1210
	map[string,string] strMap;
	strMap["a"] = "x";
	strMap["b"] = null;
	strMap["c"] = "z";
	
	test_empty7 = join('',strMap);	
	test_empty8 = join(' ',strMap);
	test_null5 = join(null,strMap);
	
	map[string, string] strMap2;
	strMap2[null] = "x";
	strMap2["eco"] = "storm";
	
	
	test_empty9 = join('',strMap2);
	printErr(test_empty9);
	test_empty10 = join(' ', strMap2);
	test_null6 = join(null,strMap2);
	return 0;
}