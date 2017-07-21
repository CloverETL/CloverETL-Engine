decimal ret1;
decimal ret2;

function integer transform(){
	$out.0.setDecimalValue('Currency', 12.3d);
	ret1 = getDecimalValue($out.0, 8);
	setDecimalValue($out.0, 8, null);
	ret2 = $out.0.getDecimalValue('Currency');
	return 0;
}