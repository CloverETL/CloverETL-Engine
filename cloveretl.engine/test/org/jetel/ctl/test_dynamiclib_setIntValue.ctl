integer ret1;
integer ret2;

function integer transform(){
	$out.0.setIntValue(5, 90);
	ret1 = getIntValue($out.0, 'Value');
	setIntValue($out.0,'Value', null);
	ret2 = $out.0.getIntValue(5);
	return 0;
}