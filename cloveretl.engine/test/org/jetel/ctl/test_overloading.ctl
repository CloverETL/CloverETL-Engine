integer res1;
string res2;

function integer transform() {
	res1 = sum(1,2);
	printErr(res1);
	res2 = sum('Memento ', 'mori');
	printErr(res2);
	return 0;
}

function integer sum(integer a, integer b) {
	return a+b;
}

function string sum(string a, string b) {
	return a+b;
}
