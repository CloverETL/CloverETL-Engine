string s;
boolean eq0;
boolean eq1;
string haystack;
boolean eq2;
boolean eq3;
boolean eq4;
boolean eq5;

function integer transform() {
	s='Hej';
	printErr(s);
	
	eq0=(s~="[a-z]{3}");
	printErr('eq0=' + eq0);
	
	eq1=(s~="[A-Za-z]{3}");
	printErr('eq1=' + eq1);
	
	haystack='Needle in a haystack, Needle in a haystack';
	printErr(haystack);
	
	eq2 = haystack ?= 'needle';
	printErr('eq2=' + eq2);
	
	eq3 = haystack ?= 'Needle';
	printErr('eq3=' + eq3);
	
	eq4 = haystack ~= 'Needle';
	printErr('eq4=' + eq4);
	
	eq5 = haystack ~= 'Needle.*';
	printErr('eq5=' + eq5);
	return 0;
}