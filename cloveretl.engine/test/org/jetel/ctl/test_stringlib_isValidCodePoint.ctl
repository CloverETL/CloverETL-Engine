boolean negative;
boolean minCodePoint; // zero
boolean maxCodePoint;
boolean tooHigh;

boolean a;
boolean c_caron;
boolean Ryo;
boolean S;

function integer transform() {

	negative = isValidCodePoint(-1);
	minCodePoint = isValidCodePoint(0);
	maxCodePoint = isValidCodePoint(0x10FFFF);
	tooHigh = isValidCodePoint(0x10FFFF + 1);
	
	a = isValidCodePoint(0x61);
	c_caron = isValidCodePoint(0x010D);
	Ryo = isValidCodePoint(0x826F);
	S = isValidCodePoint(0x01D54A);
	
	return 0;
}