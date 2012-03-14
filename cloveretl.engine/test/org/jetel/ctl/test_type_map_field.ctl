integer copyByValueTest1;
integer copyByValueTest2;
string stringFromMapField;
decimal decimalFromMapField;

function integer transform() {
	map[string, integer] input;
	for (integer i = 0; i < 5; i++) {
		string key = num2str(i);
		input[key] = i;
	}
	$out.4.integerMapField = input;
	$out.4.integerMapField["2"] = 100;
	copyByValueTest1 = input["2"]; // must equal 2
	copyByValueTest2 = $out.4.integerMapField["2"]; // must equal 100
	
	stringFromMapField = $in.multivalueInput.stringMapField["firstName"];
	decimalFromMapField = $in.multivalueInput.decimalMapField["asset"];
	
	return 0;
}
