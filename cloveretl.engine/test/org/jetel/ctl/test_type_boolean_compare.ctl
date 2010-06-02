boolean b1;
boolean b2;

function integer transform() {
	b1 = true;
	b2 = false;
	if (b1 > b2) {
		printErr("FAIL: b1 > b2");
	}
	if (b1 >= b2) {
		printErr("FAIL: b1 >= b2");
	}
	if (b1 < b2) {
		printErr("FAIL: b1 < b2");
	}
	if (b1 <= b2) {
		printErr("FAIL: b1 <= b2");
	}
	if (b1.lt.b2) {
		printErr("FAIL: b1.lt.b2");
	}
	if (b1.gt.b2) {
		printErr("FAIL: b1.gt.b2");
	}
	if (b1.ge.b2) {
		printErr("FAIL: b1.ge.b2");
	}
	if (b1.le.b2) {
		printErr("FAIL: b1.le.b2");
	}
	return 0;
}