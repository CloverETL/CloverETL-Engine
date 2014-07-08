map[string, string] ret1;
map[string, string] ret2;
map[string, string] ret3;
map[string, string] ret4;
map[string, string] ret5;

function integer transform(){
	ret1 = getFieldProperties($in.0, 1);
	ret2 = getFieldProperties($in.0, "Age");
	ret3 = getFieldProperties($in.0, "Name"); // other field
	ret4 = getFieldProperties($in.1, "Age"); // other record
	
	firstInput myRecord;
	ret5 = getFieldProperties(myRecord, "Age");

	return 0;
}