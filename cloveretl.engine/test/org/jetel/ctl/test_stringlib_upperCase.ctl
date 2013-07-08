string input;
string upper;

string test_empty;
string test_null;

function integer transform() {
	input='The QUICk !!$  broWn fox juMPS over the lazy DOG ';
	upper=upperCase(input + 'BAgr	');
	
	test_empty = upperCase('');
//	test_null = upperCase(null);
	
	printErr('to upper case:'+upper );
	return 0;
}