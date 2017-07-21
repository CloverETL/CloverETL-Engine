function string f2() {
	integer i = null;
	if (i > 1) { // raise NPE
	}
	return "f2";
}

function integer transform() {

	string n = null;
	string s = f2() : "foo";
	
	return 0;
}