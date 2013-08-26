integer ret1;
integer ret2;
integer ret3;
integer ret4;
integer ret5;

function integer transform(){

	ret1 = $in.0.getIntValue(5);
	ret2 = getIntValue($in.0, 5);
	ret3 = $in.0.getIntValue('Value');
	ret4 = getIntValue($in.0, 'Value');
	integer i;
	i = null;
	$out.0.setIntValue(5,i);
	ret5 = $out.0.getIntValue(5);
	
	return 0;
}