string input;
string subs;

function integer transform() {
	input='The QUICk !!$  broWn fox 	juMPS over the lazy DOG	';
	subs=substring(input, 5, 5);
	printErr('original string:' + input);
	printErr('substring:' + subs);
	return 0;
}