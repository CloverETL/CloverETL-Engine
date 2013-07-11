string rep;
string rep1;
string rep2;

string test_empty1;
string test_empty2;
string test_null;
string test_null2;
string test_null3;
string test_null4;
function integer transform() {
	rep=replace(date2str(today(), "yyyy MMM dd"),'[lL]','t');
	rep1=replace("The dog says meow. All DOGs say meow.", "[dD][oO][gG]", "cat");
	rep2 =  replace('intruders must die','[0-9]','');
	test_empty1 = replace('','','a');
	test_empty2 = replace('','a','milk');
	
	test_null = replace(null,'[a-z]+','puddle');
	test_null2 = replace('','a+',null);
	test_null3 = replace('bbb','c+',null);
	test_null4 = replace(null,'[A-Z]*',null);
	return 0;
}