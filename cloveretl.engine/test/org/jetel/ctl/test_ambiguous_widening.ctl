function void widening(integer i, number n) {}

function void widening(long l1, long l2) {}

function integer transform() {
	widening(1, 1); // integer-long distance is 1, integer-number distance is 2
	return 0;
}