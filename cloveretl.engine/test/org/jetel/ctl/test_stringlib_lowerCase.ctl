string input;
string lower;


function integer transform() {
	input='The QUICk !!$  broWn fox juMPS over the lazy DOG ';
	
	lower=lowerCase(input + 'BAgr  ');
	printErr('to lower case:'+lower );
	return 0;
}