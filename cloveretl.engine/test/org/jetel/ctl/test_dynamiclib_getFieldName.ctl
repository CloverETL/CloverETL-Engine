string ret1;
string ret2;

function integer transform(){
	ret1 = getFieldName($in.0, 1);
	ret2 = $in.0.getFieldName(0);
	
	return 0;
}