long ret1;
long ret2;

function integer transform(){
	setLongValue($out.0, 4, 1565486L);
	ret1 = getLongValue($out.0, 'BornMillisec');
	$out.0.setLongValue('BornMillisec', null);
	ret2 = $out.0.getLongValue(4);
	return 0;
}