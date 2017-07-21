string input;
decimal lenght1;

integer stringLength;

integer length_empty;
integer length_null1;
function integer transform() {
	
	input=' The QUICk !!$  broWn fox juMPS over the lazy DOG ';
	lenght1=length(input);
	printErr('length of '+input+':'+lenght1 );
	
	
	// string length
	stringLength = length("12345678");
	printErr("string:" + stringLength);
		
	length_empty = length('');
	string str = null;
	length_null1 = length(str);
	return 0;
}