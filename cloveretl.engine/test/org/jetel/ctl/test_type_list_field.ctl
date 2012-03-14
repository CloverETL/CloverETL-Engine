string copyByValueTest1;
string copyByValueTest2;
string stringFromListField;
decimal decimalFromListField;

function string breakIt(string[] input) {
	return input[0] + "a";
}

function integer transform() {
	string[] input;
	for (integer i = 0; i < 5; i++) {
		input[i] = num2str(i);
	}
	$out.4.stringListField = input;
	$out.4.stringListField[2] = 'test';
	copyByValueTest1 = input[2]; // must equal '2'
	copyByValueTest2 = $out.4.stringListField[2]; // must equal 'test'
	
	string castToString = breakIt($in.3.stringListField);
	
	stringFromListField = $in.multivalueInput.stringListField[0];
	decimalFromListField = $in.multivalueInput.decimalListField[0];
	
	return 0;
}
