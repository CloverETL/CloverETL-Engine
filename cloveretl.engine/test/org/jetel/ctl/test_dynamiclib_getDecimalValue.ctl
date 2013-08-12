decimal ret1;
decimal ret2;
decimal ret3;
decimal ret4;

function integer transform(){
	ret1 = getDecimalValue($in.0, 8);
	ret2 = getDecimalValue($in.0, 'Currency');
	$out.0.8 = null;
	ret3 = $out.0.getDecimalValue(8);
	ret4 = $out.0.getDecimalValue('Currency');
	return 0;
}