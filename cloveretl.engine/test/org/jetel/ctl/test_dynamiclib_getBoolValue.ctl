boolean ret1;
boolean ret2;
boolean ret3;
boolean ret4;
boolean ret5;
boolean ret6;

function integer transform(){
	$out.0.6 = true;
	ret1 = getBoolValue($out.0, 6);
	ret2 = getBoolValue($out.0, 'Flag');
	$out.0.6 = false;
	ret3 = $out.0.getBoolValue(6);
	ret4 = $out.0.getBoolValue('Flag');
	$out.0.6 = null;
	ret5 = getBoolValue($out.0,6);
	ret6 = getBoolValue($out.0, 'Flag');
	return 0;
}