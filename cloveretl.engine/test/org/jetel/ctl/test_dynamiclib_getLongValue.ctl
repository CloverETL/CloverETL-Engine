long ret1;
long ret2;
long ret3;

function integer transform(){
	ret1 = $in.0.getLongValue(4);
	ret2 = getLongValue($in.0, 'BornMillisec');
	$out.0.BornMillisec = null;
	ret3 = getLongValue($out.0, 4);
	return 0;
}