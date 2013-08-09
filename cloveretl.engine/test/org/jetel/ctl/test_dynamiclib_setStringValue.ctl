string ret1;
string ret2;

function integer transform(){
	$out.0.setStringValue('Name', 'Zac');
	ret1 = $out.0.getStringValue('Name');
	setStringValue($out.0, 0, null);
	ret2 = getStringValue($out.0, 0);
	return 0;
}