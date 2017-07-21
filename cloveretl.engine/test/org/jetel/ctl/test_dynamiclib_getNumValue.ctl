number ret1;
number ret2;
number ret3;

function integer transform(){
	ret1 = getNumValue($in.0, 1);
	ret2 = $in.0.getNumValue('Age');
	number tmp = null;
	$out.0.1 = tmp;
	ret3 = getNumValue($out.0, 1);

	return 0;
}