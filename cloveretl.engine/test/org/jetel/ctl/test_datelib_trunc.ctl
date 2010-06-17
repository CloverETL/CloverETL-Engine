date inputDate;
date truncDate;

function integer transform() {
	inputDate=2004-01-02 17:13:20;
	
	truncDate=trunc(inputDate);
	
	printErr('truncation of '+inputDate+'='+truncDate);
	return 0;
}