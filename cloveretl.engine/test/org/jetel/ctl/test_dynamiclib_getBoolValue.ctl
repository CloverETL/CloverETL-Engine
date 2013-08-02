boolean ret1;
boolean ret2;
boolean ret3;

function integer transform(){
	$out.0.6 = true;
	ret1 = getBoolValue($out.0, 6);
	$out.0.6 = false;
	ret2 = $out.0.getBoolValue(6);
	$out.0.6 = null;
	ret3 = getBoolValue($out.0,6);
	return 0;
}