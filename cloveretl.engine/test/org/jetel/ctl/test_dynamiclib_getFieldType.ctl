string ret1;
string ret2;

function integer transform(){
	ret1 = getFieldType($in.0, 0);
	ret2 = $in.0.getFieldType(1);
	return 0;
}