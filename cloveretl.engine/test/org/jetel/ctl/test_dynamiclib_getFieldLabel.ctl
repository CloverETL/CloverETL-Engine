string ret1;
string ret2;
string ret3;
string ret4;

function integer transform(){
	ret1 = getFieldLabel($in.0, 1);
	ret2 = $in.0.getFieldLabel(0);
	ret3 = getFieldLabel($in.0, 'Age');
	ret4 = $in.0.getFieldLabel('Value');
	return 0;
}