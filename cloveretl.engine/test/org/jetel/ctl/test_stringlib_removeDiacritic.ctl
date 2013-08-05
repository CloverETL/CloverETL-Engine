string test;
string test1;

string test_empty;
string test_null;

function integer transform() {
	test=removeDiacritic('teścik');
	
	test1=removeDiacritic('žabička');
	
	test_empty = removeDiacritic('');
	test_null = removeDiacritic(null);
	return 0;
}