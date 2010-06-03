string rep;
string rep1;

function integer transform() {
	rep=replace(date2str(today(), "yyyy MMM dd"),'[lL]','t');
	rep1=replace("The dog says meow. All DOGs say meow.", "[dD][oO][gG]", "cat");
	
	printErr(rep1);
	return 0;
}