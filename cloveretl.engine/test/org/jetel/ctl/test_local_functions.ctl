function void intFunction(integer i) {}

function void longFunction(long l) {}

function void numberFunction(number n) {}

function void booleanFunction(boolean b) {}

function integer transform() {
	// CLO-1246
	intFunction(null);
	longFunction(null);
	numberFunction(null);
	booleanFunction(null);
	
	return 0;
}