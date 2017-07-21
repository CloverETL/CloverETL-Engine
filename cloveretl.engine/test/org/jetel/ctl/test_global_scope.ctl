// test case for issue 5006 - a function call in global scope in COMPILE mode was not possible
 
string s = "Kokon";
integer len = s.length();

function integer transform() {
	return OK;
}
