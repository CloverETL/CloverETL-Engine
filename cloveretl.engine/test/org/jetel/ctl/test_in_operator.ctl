integer a;
integer[] haystack;
integer needle;
boolean b1;
boolean b2;
double[] h2;
boolean b3;
string[] h3;
string n3; 
boolean b4;

function integer transform() {
	a = 1;
	haystack = [a,a+1,a+1+1];
	needle = 2;
	b1 = needle.in(haystack);
	printErr(needle + ' in ' + haystack + ': ' + b1);
	haystack.clear();
	b2 = in(needle,haystack);
	printErr(needle + ' in ' + haystack + ': ' + b2);
	h2 = [ 2.1, 2.0, 2.2];
	b3 = needle.in(h2);
	printErr(needle + ' in ' + h2 + ': ' + b3);
	h3 = [ 'memento', 'mori', 'memento ' + 'mori'];
	n3 = 'memento ' + 'mori'; 
	b4 = n3.in(h3);
	printErr(n3 + ' in ' + h3 + ': ' + b4);
	
	return 0;
}