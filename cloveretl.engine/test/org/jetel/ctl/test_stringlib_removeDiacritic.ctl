string test;
string test1;

function integer transform() {
	test=removeDiacritic('teścik');
	printErr(test); 
	
	test1=removeDiacritic('žabička');
	printErr(test1);
	return 0;
}