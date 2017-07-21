integer ret0;
integer ret1;
integer ret2;
integer ret3;
integer ret4;
integer ret5;

function integer transform() {
	byte input;
	
	input = hex2byte("00FF");
	ret0 = byteAt(input, 0);
	ret1 = byteAt(input, 1);
	
	input = hex2byte("F00F");
	ret2 = byteAt(input, 0);
	ret3 = byteAt(input, 1);
	
	input = hex2byte("0100");
	ret4 = byteAt(input, 0);
	ret5 = byteAt(input, 1);

	return 0;
}