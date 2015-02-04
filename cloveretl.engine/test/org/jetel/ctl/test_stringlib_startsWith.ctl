boolean b1;
boolean b2;
boolean b3;
boolean b4;
boolean b5;
boolean b6;
boolean b7;

function integer transform() {
	b1 = "abc".startsWith("a");
	b2 = "abc".startsWith("b");
	b3 = "abc".startsWith("");

	b4 = startsWith(null, "x");
	b5 = startsWith(null, "");
	
	b6 = "".startsWith("x");
	b7 = "".startsWith("");
	
	return 0;
}