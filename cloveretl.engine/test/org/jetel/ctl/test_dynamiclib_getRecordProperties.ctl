map[string, string] ret1;
map[string, string] ret2;

function integer transform(){
	ret1 = getRecordProperties($in.0);
	ret2 = $in.1.getRecordProperties();
	
	return 0;
}