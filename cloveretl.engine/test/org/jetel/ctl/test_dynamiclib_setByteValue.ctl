byte ret1;
byte ret2;

function integer transform(){
	$out.0.setByteValue('ByteArray', str2byte('Urgot', 'utf-8'));
	ret1 = $out.0.getByteValue('ByteArray');
	setByteValue($out.0, 7 ,null);
	ret2 = getByteValue($out.0, 7);
	return 0;
}