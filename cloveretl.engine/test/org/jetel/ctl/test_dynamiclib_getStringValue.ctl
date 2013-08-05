string ret1;
string ret2;
string ret3;

function integer transform(){
	ret1 = getStringValue($in.0, 0);
	ret2 = $in.0.getStringValue('Name');
	$out.0.Name = null;
	ret3 = getStringValue($out.0, 'Name');
	return 0;
}