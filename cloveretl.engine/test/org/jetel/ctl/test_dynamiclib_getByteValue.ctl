byte ret1;
byte ret2;
byte ret3;
byte ret4;
function integer transform(){
	ret1 = getByteValue($in.0, 7);
	ret2 = getByteValue($in.0, 'ByteArray');
	$out.0.ByteArray = null;
	ret3 = $out.0.getByteValue(7);
	ret4 = $out.0.getByteValue('ByteArray');
	return 0;
}