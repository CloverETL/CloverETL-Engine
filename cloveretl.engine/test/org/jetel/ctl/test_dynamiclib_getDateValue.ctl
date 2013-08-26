date ret1;
date ret2;
date ret3;
date ret4;

function integer transform(){
	ret1 = getDateValue($in.0, 3);
	ret2 = getDateValue($in.0, 'Born');
	$out.0.Born = null;
	ret3 = $out.0.getDateValue(3);
	ret4 = $out.0.getDateValue('Born');
	return 0;
}