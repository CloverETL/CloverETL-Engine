function void mixed(integer i, long l) {
}

function void mixed(long l, integer i) {
}

function void mixed(long l1, long l2) {
}

function boolean isAscii(string s) {
	return true;
}

function string charAt(string s, integer i) {
	return "";
}

function integer transform() {
	mixed(1, 1L); // not ambiguous
	mixed(1L, 1); // not ambiguous
	mixed(1L, 1L); // not ambiguous
	isAscii("");
	charAt(null, null); // the library function would throw an exception
	charAt("", 1); // the library function would throw an exception
	
	return 0;
}