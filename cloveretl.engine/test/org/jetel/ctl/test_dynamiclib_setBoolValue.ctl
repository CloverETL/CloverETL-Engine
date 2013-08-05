boolean ret1;
boolean ret2;
boolean ret3;
function integer transform(){
	$out.0.setBoolValue('Flag',null);
	ret1 = getBoolValue($out.0, 'Flag');
	setBoolValue($out.0, 'Flag', true);
	ret2 = getBoolValue($out.0, 'Flag');
	setBoolValue($out.0, 6, false);
	ret3 = getBoolValue($out.0, 'Flag');
	return 0;
}