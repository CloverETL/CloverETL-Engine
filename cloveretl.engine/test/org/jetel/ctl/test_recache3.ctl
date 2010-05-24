integer a = 3;

function integer transform() {
	
	string[] a;
	a = find("toto je testovaci string pre vyhladavanie to regexpov", "t?o");
	print_err(a);
	
	string p1 = "toto je testovaci string pre vyhladavanie to regexpov"; 
	string[] b;
	b = find(p1, "t?o");
	print_err(b);

	string p2 = "toto je testovaci string pre vyhladavanie to regexpov";
	string p3 = "t?o"; 
	string[] c;
	c = find(p2, p3);
	print_err(c);
	
	return 0;
}
