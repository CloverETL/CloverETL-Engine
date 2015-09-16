boolean b1;
boolean b2;
boolean b3;

boolean b4;
boolean b5;

boolean b6;
boolean b7;

function integer transform() {
	b1 = "abc".endsWith("c");
	b2 = "abc".endsWith("b");
	b3 = "abc".endsWith("");

	b4 = endsWith(null, "x");
	b5 = endsWith(null, "");
	
	b6 = "".endsWith("x");
	b7 = "".endsWith("");
	
	return 0;
}