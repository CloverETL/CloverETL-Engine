map[string, string] ret1;
map[string, string] ret2;
map[string, string] ret3;
map[string, string] ret4;

function integer transform() {
	ret1 = getRecordProperties($in.0);
	ret2 = getRecordProperties($out.0);
	ret3 = getRecordProperties($in.3);
	
	// variable
	firstInput myRecord;
	ret4 = getRecordProperties(myRecord);
	
	return 0;
}