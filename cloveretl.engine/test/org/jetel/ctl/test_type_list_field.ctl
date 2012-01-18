string copyByValueTest1;
string copyByValueTest2;

function integer transform() {
	string[] input;
	for (integer i = 0; i < 5; i++) {
		input[i] = num2str(i);
	}
	$out.4.stringListField = input;
	$out.4.stringListField[2] = 'test';
	copyByValueTest1 = input[2]; // must equal '2'
	copyByValueTest2 = $out.4.stringListField[2]; // must equal 'test'
	
}
