string rep;
string rep1;

string test_empty1;
string test_empty2;
string test_null;
function integer transform() {
	rep=replace(date2str(today(), "yyyy MMM dd"),'[lL]','t');
	rep1=replace("The dog says meow. All DOGs say meow.", "[dD][oO][gG]", "cat");
	
	test_empty1 = replace('','','a');
	test_empty2 = replace('','a','milk');
	
	test_null = replace(null,'[a-z]+','puddle');
	return 0;
}