number ret1;
number ret2;

function integer transform(){
	$out.0.setNumValue(1, 12.5);
	ret1 = $out.0.getNumValue(1);
	setNumValue($out.0, 'Age', null);
	ret2 = getNumValue($out.0, 'Age');
	return 0;
}