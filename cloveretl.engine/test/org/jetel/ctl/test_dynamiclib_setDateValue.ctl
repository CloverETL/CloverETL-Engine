date ret1;
date ret2;

function integer transform(){
	$out.0.setDateValue(3, str2date('2006-11-12','yyyy-MM-dd'));
	ret1 = getDateValue($out.0, 'Born');
	setDateValue($out.0, 'Born', null);
	ret2 = $out.0.getDateValue(3);
	return 0;
}