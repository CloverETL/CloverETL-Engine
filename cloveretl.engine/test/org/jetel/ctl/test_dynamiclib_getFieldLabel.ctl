string ret1;
string ret2;

function integer transform(){
	ret1 = getFieldLabel($in.0, 1);
	ret2 = $in.0.getFieldLabel(0);
	return 0;
}