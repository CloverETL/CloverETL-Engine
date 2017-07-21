string ret1;
string ret2;
string ret3;

function integer transform(){
	ret1 = getFieldType($in.0, 0);
	ret2 = $in.0.getFieldType(1);
	ret3= $in.0.getFieldType("Age");
	return 0;
}