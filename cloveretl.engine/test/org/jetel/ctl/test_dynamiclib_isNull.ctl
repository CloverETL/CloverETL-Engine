boolean ret1;
boolean ret2;
boolean ret3;

function integer transform(){
	ret1 = isNull($in.0, 0);
	$out.0.0 = 'null';
	ret2 = $out.0.isNull(0);
	$out.0.0 = null;
	ret3 = isNull($out.0, 'Name');

	return 0;
}