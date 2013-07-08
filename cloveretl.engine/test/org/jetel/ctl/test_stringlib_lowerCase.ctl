string input;
string lower;

string lower_empty;
string lower_null;

function integer transform() {
	input='The QUICk !!$  broWn fox juMPS over the lazy DOG ';
	
	lower=lowerCase(input + 'BAgr  ');
	printErr('to lower case:'+lower );
	lower_empty = lowerCase('');
//	lower_null = lowerCase(null);
	
	return 0;
}