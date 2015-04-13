map[string, string] ret1;
map[string, string] ret2;
map[string, string] lastField;

function integer transform(){
	ret1 = getFieldProperties($in.0, 0);
	ret2 = $in.0.getFieldProperties(1);
	lastField = $in.0.getFieldProperties(length($in.0) - 1);
	
	return 0;
}